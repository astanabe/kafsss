#!/usr/bin/perl

use strict;
use warnings;
use Getopt::Long;
use DBI;
use JSON;
use HTTP::Server::Simple::CGI;
use POSIX qw(strftime WNOHANG);
use Sys::Hostname;
use File::Basename;
use MIME::Base64;
use Time::HiRes qw(time);
use Fcntl qw(:flock);
use IO::Compress::Gzip qw(gzip $GzipError);
use IO::Uncompress::Gunzip qw(gunzip $GunzipError);
use IO::Compress::Zstd qw(zstd $ZstdError);
use IO::Uncompress::UnZstd qw(unzstd $UnZstdError);

# Version number
my $VERSION = "__VERSION__";

# Default values - Configure these for your environment
# These values will be used when not specified in API requests
my $default_host = $ENV{PGHOST} || 'localhost';           # PostgreSQL host
my $default_port = $ENV{PGPORT} || 5432;                  # PostgreSQL port
my $default_user = $ENV{PGUSER} || getpwuid($<);          # PostgreSQL username
my $default_password = $ENV{PGPASSWORD} || '';             # PostgreSQL password
my $default_database = '';       # Set default database name here (e.g., 'mykmersearch')
my $default_subset = '';      # Set default subset name here (e.g., 'bacteria')
my $default_maxnseq = 0;         # Set default maxnseq value here (0 = unlimited)
my $maxmaxnseq = 100000;         # Maximum allowed maxnseq value
my $default_minscore = 1;        # Set default minscore value here
my $default_minpsharedkmer = 0.5;  # Set default minimum shared k-mer rate here
my $default_mode = 'sequence';   # Set default mode (minimum, matchscore, sequence, maximum)
my $default_listen_port = 8080;  # HTTP server listen port
my $default_numthreads = 5;      # Number of parallel request processing threads

# Database configuration - Multiple databases support
my @available_databases = ();    # Array of available database names (e.g., ('mykmersearch', 'otherdb'))

# Subset configuration
# Format: "database_name:subset_name" (e.g., "mykmersearch:bacteria")
my @available_subsets = ();      # Array of available subsets

# Default GIN index parameters (all optional, used for index selection)
my $default_kmersize = '';           # Default kmer_size value (empty = unspecified)
my $default_occurbitlen = '';        # Default occur_bitlen value
my $default_maxpappear = '';         # Default max_appearance_rate (max 3 decimal places)
my $default_maxnappear = '';         # Default max_appearance_nrow value
my $default_precludehighfreqkmer = '';  # Default preclude_highfreq_kmer (1, 0, or empty)

# Available indices - populated at startup
my @available_indices = ();

# SQLite job management settings
my $default_sqlite_path = './kafsssearchserver.sqlite';  # SQLite database path
my $default_clean_limit = 86400;      # 24 hours (result retention period in seconds)
my $default_job_timeout = 1800;       # 30 minutes (job timeout in seconds)
my $default_max_jobs = 10;            # Maximum concurrent jobs
my $default_cleanup_interval = 300;   # 5 minutes (cleanup interval in seconds)

# Accepted modes - edit this array to restrict available modes
my @accepted_modes = ('minimum', 'normal', 'maximum');

# Command line options
my $host = $default_host;
my $port = $default_port;
my $username = $default_user;
my $listen_port = $default_listen_port;
my $numthreads = $default_numthreads;
my $sqlite_path = $default_sqlite_path;
my $clean_limit = $default_clean_limit;
my $job_timeout = $default_job_timeout;
my $max_jobs = $default_max_jobs;
my $cleanup_interval = $default_cleanup_interval;
my $help = 0;

# Parse command line options
GetOptions(
    'host=s' => \$host,
    'port=i' => \$port,
    'username=s' => \$username,
    'listenport=i' => \$listen_port,
    'numthreads=i' => \$numthreads,
    'sqlitepath=s' => \$sqlite_path,
    'cleanlimit=i' => \$clean_limit,
    'jobtimeout=i' => \$job_timeout,
    'maxnjob=i' => \$max_jobs,
    'cleaninterval=i' => \$cleanup_interval,
    'help|h' => \$help,
) or die "Error in command line arguments\n";

# Show help
if ($help) {
    print_help();
    exit 0;
}

print "kafsssearchserver version $VERSION\n";
print "PostgreSQL Host: $host\n";
print "PostgreSQL Port: $port\n";
print "PostgreSQL Username: $username\n";
print "Listen Port: $listen_port\n";
print "Number of threads: $numthreads\n";
print "SQLite Database: $sqlite_path\n";
print "Max concurrent jobs: $max_jobs\n";
print "Job timeout: $job_timeout seconds\n";
print "Result retention: $clean_limit seconds\n";

# Validate maxpappear precision at startup
if ($default_maxpappear ne '') {
    my $error = validate_maxpappear_precision($default_maxpappear, "Default");
    if ($error) {
        die "Error: $error\n";
    }
}

# Validate all configured databases and collect available indices
print "Validating database configurations...\n";
my $indices_ref = validate_all_databases($host, $port, $username, $default_password);
@available_indices = @$indices_ref;

# Validate default database configuration
validate_default_database($host, $port, $username, $default_password);

# Initialize SQLite database
print "Initializing SQLite database...\n";
initialize_sqlite_database($sqlite_path);

# Create and start HTTP server
my $server = KmerSearchWebServer->new($listen_port);
$server->setup(
    host => $host,
    port => $port,
    username => $username,
    numthreads => $numthreads,
    sqlite_path => $sqlite_path,
    clean_limit => $clean_limit,
    job_timeout => $job_timeout,
    max_jobs => $max_jobs,
    cleanup_interval => $cleanup_interval
);

print "Starting HTTP server on port $listen_port...\n";
print "API endpoints:\n";
print "  POST http://localhost:$listen_port/search  - Submit search job\n";
print "  POST http://localhost:$listen_port/result  - Get job result\n";
print "  POST http://localhost:$listen_port/status  - Get job status\n";
print "  POST http://localhost:$listen_port/cancel  - Cancel job\n";
print "Press Ctrl+C to stop the server.\n";

$server->run();

exit 0;

#
# Subroutines
#

sub initialize_sqlite_database {
    my ($sqlite_path) = @_;
    
    my $dbh = DBI->connect("dbi:SQLite:dbname=$sqlite_path", "", "", {
        AutoCommit => 1,
        PrintError => 0,
        RaiseError => 1,
        ShowErrorStatement => 1,
        AutoInactiveDestroy => 1,
        sqlite_unicode => 1
    }) or die "Cannot connect to SQLite database '$sqlite_path': $DBI::errstr\n";
    
    # Enable WAL mode for better concurrent access
    $dbh->do("PRAGMA journal_mode=WAL");
    $dbh->do("PRAGMA synchronous=NORMAL");
    $dbh->do("PRAGMA cache_size=10000");
    $dbh->do("PRAGMA temp_store=memory");
    
    # Create jobs table
    $dbh->do(<<'SQL');
CREATE TABLE IF NOT EXISTS kafsssearchserver_jobs (
    job_id TEXT PRIMARY KEY,
    time TEXT NOT NULL,
    querylabel TEXT NOT NULL,
    queryseq TEXT NOT NULL,
    db TEXT NOT NULL,
    subset TEXT,
    index_name TEXT NOT NULL,
    kmersize INTEGER NOT NULL,
    occurbitlen INTEGER NOT NULL,
    maxpappear REAL NOT NULL,
    maxnappear INTEGER NOT NULL,
    precludehighfreqkmer INTEGER NOT NULL,
    maxnseq INTEGER NOT NULL,
    minscore INTEGER NOT NULL,
    mode TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'running',
    pid INTEGER,
    timeout_time TEXT
)
SQL

    # Create results table
    $dbh->do(<<'SQL');
CREATE TABLE IF NOT EXISTS kafsssearchserver_results (
    job_id TEXT PRIMARY KEY,
    time TEXT NOT NULL,
    results TEXT NOT NULL
)
SQL

    # Create indexes for better performance
    $dbh->do("CREATE INDEX IF NOT EXISTS idx_jobs_status ON kafsssearchserver_jobs(status)");
    $dbh->do("CREATE INDEX IF NOT EXISTS idx_jobs_time ON kafsssearchserver_jobs(time)");
    $dbh->do("CREATE INDEX IF NOT EXISTS idx_results_time ON kafsssearchserver_results(time)");
    
    $dbh->disconnect();
    
    print "SQLite database initialized successfully.\n";
}

sub generate_random_bytes {
    my ($num_bytes) = @_;
    
    # Try /dev/urandom first
    if (-r '/dev/urandom') {
        open my $fh, '<:raw', '/dev/urandom' or die "Cannot open /dev/urandom: $!\n";
        my $bytes;
        read $fh, $bytes, $num_bytes or die "Cannot read from /dev/urandom: $!\n";
        close $fh;
        return $bytes;
    }
    
    # Try Crypt::OpenSSL::Random
    eval {
        require Crypt::OpenSSL::Random;
        my $random_bytes = Crypt::OpenSSL::Random::random_bytes($num_bytes);
        return $random_bytes;
    };
    
    if (!$@) {
        # This should not be reached if eval succeeded
        # The return should have happened in the eval block
    }
    
    # Try openssl command (output binary)
    my $openssl_output = `openssl rand -binary $num_bytes 2>/dev/null`;
    if ($? == 0 && length($openssl_output) == $num_bytes) {
        return $openssl_output;
    }
    
    # All methods failed
    die "Cannot generate random bytes: /dev/urandom not readable, Crypt::OpenSSL::Random not available, and openssl command failed\n";
}

sub generate_job_id {
    my $timestamp = strftime("%Y%m%dT%H%M%S", localtime);
    my $random_bytes = generate_random_bytes(24);  # 192 bits
    my $base64_subset = encode_base64($random_bytes, '');  # 32 characters, no newlines
    return "$timestamp-$base64_subset";
}

sub normalize_mode {
    my ($mode) = @_;
    return '' unless defined $mode;

    # Normalize mode aliases
    my %mode_aliases = (
        'min' => 'minimum',
        'minimize' => 'minimum',
        'minimum' => 'minimum',
        'matchscore' => 'matchscore',
        'score' => 'matchscore',
        'sequence' => 'sequence',
        'seq' => 'sequence',
        'max' => 'maximum',
        'maximize' => 'maximum',
        'maximum' => 'maximum'
    );

    return $mode_aliases{lc($mode)} || '';
}

# Get list of GIN indexes on kafsss_data.seq column
sub get_gin_indexes {
    my ($dbh) = @_;

    my $sth = $dbh->prepare(<<SQL);
SELECT indexname
FROM pg_indexes
WHERE tablename = 'kafsss_data'
  AND indexname LIKE 'idx_kafsss_data_seq_gin_km%'
ORDER BY indexname
SQL
    $sth->execute();

    my @indexes = ();
    while (my ($indexname) = $sth->fetchrow_array()) {
        push @indexes, $indexname;
    }
    $sth->finish();

    return \@indexes;
}

# Parse GIN index name to extract parameters
sub parse_gin_index_name {
    my ($indexname) = @_;

    if ($indexname =~ /idx_kafsss_data_seq_gin_km(\d+)_ob(\d+)_mar(\d{4})_man(\d+)_phk([TF])/) {
        return {
            kmer_size => int($1),
            occur_bitlen => int($2),
            max_appearance_rate => $3 / 1000,
            max_appearance_nrow => int($4),
            preclude_highfreq_kmer => ($5 eq 'T' ? 1 : 0)
        };
    }

    return undef;
}

# Select appropriate GIN index based on target parameters
sub select_gin_index {
    my ($dbh, $indexes, $target_params, $dbname) = @_;

    my $index_count = scalar(@$indexes);

    if ($index_count == 0) {
        return { error => "No GIN indexes found on kafsss_data.seq column in database '$dbname'." };
    }

    # If only one index exists, use it
    if ($index_count == 1) {
        my $parsed = parse_gin_index_name($indexes->[0]);
        if (!$parsed) {
            return { error => "Cannot parse GIN index name: $indexes->[0]" };
        }
        return {
            index_name => $indexes->[0],
            params => $parsed
        };
    }

    # Multiple indexes - filter by target parameters
    my @matching_indexes = ();

    for my $indexname (@$indexes) {
        my $parsed = parse_gin_index_name($indexname);
        next unless $parsed;

        my $matches = 1;

        if (defined $target_params->{kmer_size} && $target_params->{kmer_size} ne '') {
            $matches = 0 if $parsed->{kmer_size} != $target_params->{kmer_size};
        }
        if (defined $target_params->{occur_bitlen} && $target_params->{occur_bitlen} ne '') {
            $matches = 0 if $parsed->{occur_bitlen} != $target_params->{occur_bitlen};
        }
        if (defined $target_params->{max_appearance_rate} && $target_params->{max_appearance_rate} ne '') {
            $matches = 0 if abs($parsed->{max_appearance_rate} - $target_params->{max_appearance_rate}) >= 0.0001;
        }
        if (defined $target_params->{max_appearance_nrow} && $target_params->{max_appearance_nrow} ne '') {
            $matches = 0 if $parsed->{max_appearance_nrow} != $target_params->{max_appearance_nrow};
        }
        if (defined $target_params->{preclude_highfreq_kmer} && $target_params->{preclude_highfreq_kmer} ne '') {
            $matches = 0 if $parsed->{preclude_highfreq_kmer} != $target_params->{preclude_highfreq_kmer};
        }

        if ($matches) {
            push @matching_indexes, {
                index_name => $indexname,
                params => $parsed
            };
        }
    }

    my $match_count = scalar(@matching_indexes);

    if ($match_count == 0) {
        my @match_names = map { $_  } @$indexes;
        return {
            error => "No matching GIN index found for the specified parameters.",
            available_indexes => \@match_names
        };
    }

    if ($match_count == 1) {
        return $matching_indexes[0];
    }

    # Multiple matches
    my @match_names = map { $_->{index_name} } @matching_indexes;
    return {
        error => "Multiple GIN indexes match the specified parameters.",
        matching_indexes => \@match_names
    };
}

# Validate maxpappear precision (max 3 decimal places)
sub validate_maxpappear_precision {
    my ($value, $context) = @_;

    return undef if !defined $value || $value eq '';  # Unspecified is allowed

    my $value_str = sprintf("%.10f", $value);
    if ($value_str =~ /\.\d{4,}[1-9]/) {
        return "$context max_appearance_rate '$value' has more than 3 decimal places. Maximum 3 decimal places allowed (e.g., 0.050, 0.125).";
    }

    return undef;  # No error
}

# Validate all configured databases at startup
sub validate_all_databases {
    my ($pg_host, $pg_port, $pg_user, $pg_password) = @_;

    my @indices = ();

    # If no databases configured, skip validation
    if (@available_databases == 0) {
        print "Warning: No databases configured in \@available_databases.\n";
        return \@indices;
    }

    for my $dbname (@available_databases) {
        print "Validating database: $dbname\n";

        # Connect to database
        my $dsn = "DBI:Pg:dbname=$dbname;host=$pg_host;port=$pg_port";
        my $dbh = eval {
            DBI->connect($dsn, $pg_user, $pg_password, {
                AutoCommit => 1,
                PrintError => 0,
                RaiseError => 1,
                ShowErrorStatement => 1,
                AutoInactiveDestroy => 1,
                pg_enable_utf8 => 1
            });
        };

        if (!$dbh || $@) {
            die "Error: Cannot connect to database '$dbname': " . ($@ || $DBI::errstr) . "\n";
        }

        # Check kafsss_data and kafsss_meta tables exist
        my $sth = $dbh->prepare("SELECT COUNT(*) FROM information_schema.tables WHERE table_name IN ('kafsss_data', 'kafsss_meta')");
        $sth->execute();
        my ($table_count) = $sth->fetchrow_array();
        $sth->finish();

        if ($table_count < 2) {
            $dbh->disconnect();
            die "Error: Database '$dbname' is missing required tables (kafsss_data, kafsss_meta).\n";
        }

        # Get GIN indexes
        my $indexes = get_gin_indexes($dbh);
        if (scalar(@$indexes) == 0) {
            $dbh->disconnect();
            die "Error: No GIN indexes found in database '$dbname'. Please create indexes first using kafssindex.\n";
        }

        # Add to available_indices with "database:indexname" format
        for my $idx (@$indexes) {
            push @indices, "$dbname:$idx";
        }

        # Validate configured subsets for this database
        for my $subset_spec (@available_subsets) {
            my ($subset_db, $subset_name) = split(/:/, $subset_spec, 2);
            if ($subset_db eq $dbname) {
                # Check if subset exists in kafsss_data
                $sth = $dbh->prepare("SELECT 1 FROM kafsss_data WHERE ? = ANY(subset) LIMIT 1");
                $sth->execute($subset_name);
                my $exists = $sth->fetchrow_array();
                $sth->finish();

                if (!$exists) {
                    $dbh->disconnect();
                    die "Error: Subset '$subset_name' not found in database '$dbname'.\n";
                }
                print "  Validated subset: $subset_name\n";
            }
        }

        print "  Found " . scalar(@$indexes) . " GIN index(es)\n";
        $dbh->disconnect();
    }

    return \@indices;
}

# Validate default database configuration
sub validate_default_database {
    my ($pg_host, $pg_port, $pg_user, $pg_password) = @_;

    return if $default_database eq '';

    print "Validating default database configuration...\n";

    # Connect to default database
    my $dsn = "DBI:Pg:dbname=$default_database;host=$pg_host;port=$pg_port";
    my $dbh = eval {
        DBI->connect($dsn, $pg_user, $pg_password, {
            AutoCommit => 1,
            PrintError => 0,
            RaiseError => 1,
            ShowErrorStatement => 1,
            AutoInactiveDestroy => 1,
            pg_enable_utf8 => 1
        });
    };

    if (!$dbh || $@) {
        die "Error: Cannot connect to default database '$default_database': " . ($@ || $DBI::errstr) . "\n";
    }

    # Validate default_subset if specified
    if ($default_subset ne '') {
        my ($subset_db, $subset_name) = split(/:/, $default_subset, 2);

        # If no colon, assume it's just a subset name for the default database
        if (!defined $subset_name) {
            $subset_name = $default_subset;
            $subset_db = $default_database;
        }

        if ($subset_db ne $default_database) {
            $dbh->disconnect();
            die "Error: default_subset database '$subset_db' does not match default_database '$default_database'.\n";
        }

        my $sth = $dbh->prepare("SELECT 1 FROM kafsss_data WHERE ? = ANY(subset) LIMIT 1");
        $sth->execute($subset_name);
        my $exists = $sth->fetchrow_array();
        $sth->finish();

        if (!$exists) {
            $dbh->disconnect();
            die "Error: Default subset '$subset_name' not found in default database '$default_database'.\n";
        }
    }

    # Build target params from defaults
    my $target_params = {
        kmer_size => $default_kmersize,
        occur_bitlen => $default_occurbitlen,
        max_appearance_rate => $default_maxpappear,
        max_appearance_nrow => $default_maxnappear,
        preclude_highfreq_kmer => $default_precludehighfreqkmer
    };

    # Get GIN indexes and validate that defaults match exactly one
    my $indexes = get_gin_indexes($dbh);
    my $result = select_gin_index($dbh, $indexes, $target_params, $default_database);

    if ($result->{error}) {
        $dbh->disconnect();
        if ($result->{matching_indexes}) {
            my $list = join("\n  - ", @{$result->{matching_indexes}});
            die "Error: $result->{error}\nMatching indexes:\n  - $list\nPlease specify more specific default parameters.\n";
        } elsif ($result->{available_indexes}) {
            my $list = join("\n  - ", @{$result->{available_indexes}});
            die "Error: $result->{error}\nAvailable indexes:\n  - $list\n";
        } else {
            die "Error: $result->{error}\n";
        }
    }

    print "Default GIN index: $result->{index_name}\n";
    $dbh->disconnect();
}

sub format_timestamp {
    my ($time) = @_;
    $time ||= time();
    return strftime("%Y%m%dT%H%M%S", localtime($time));
}

sub print_help {
    print <<EOF;
kafsssearchserver version $VERSION

Usage: perl kafsssearchserver_standalone.pl [options]

REST API server for k-mer search using kafsss database.

Options:
  --host=HOST         PostgreSQL server host (default: \$PGHOST or localhost)
  --port=PORT         PostgreSQL server port (default: \$PGPORT or 5432)
  --username=USER     PostgreSQL username (default: \$PGUSER or current user)
  --listenport=PORT   HTTP server listen port (default: 8080)
  --numthreads=INT    Number of parallel request processing threads (default: 5)
  --sqlitepath=PATH   SQLite database file path (default: ./kafsssearchserver.sqlite)
  --cleanlimit=INT    Result retention period in seconds (default: 86400)
  --jobtimeout=INT    Job timeout in seconds (default: 1800)
  --maxnjob=INT       Maximum concurrent jobs (default: 10)
  --cleaninterval=INT Cleanup interval in seconds (default: 300)
  --help, -h          Show this help message

Environment variables:
  PGHOST           PostgreSQL server host
  PGPORT           PostgreSQL server port
  PGUSER           PostgreSQL username
  PGPASSWORD       PostgreSQL password

API Usage:
  POST /search
  
  Request JSON for search:
  {
    "querylabel": "sequence_name",      // required
    "queryseq": "ATCGATCGATCG...",     // required
    "db": "database_name",             // optional if default configured
    "subset": "subset_name",      // optional, uses default if configured
    "maxnseq": 1000,                   // optional, uses default if configured
    "minscore": 10,                    // optional, uses default if configured
    "minpsharedkmer": 0.5               // optional, default: 0.5
  }
  
  Request JSON for metadata:
  {
    "db": "database_name"              // required
  }
  
  Response JSON for search:
  {
    "querylabel": "sequence_name",
    "queryseq": "ATCGATCGATCG...",
    "db": "database_name",
    "subset": "subset_name",
    "maxnseq": 1000,
    "minscore": 10,
    "results": [
      {
        "seqid": ["AB123:1:100", "CD456:50:150"],
        "matchscore": 95
      },
      {
        "seqid": ["EF789:200:300"],
        "matchscore": 87
      }
    ]
  }
  
  Response JSON for metadata:
  {
    "ver": "1.0.0",
    "minlen": 64,
    "minsplitlen": 50000,
    "ovllen": 500,
    "nseq": 12345,
    "nchar": 1234567890,
    "subset": {
      "bacteria": {"nseq": 5000, "nchar": 500000000},
      "archaea": {"nseq": 2000, "nchar": 200000000}
    }
  }

Examples:
  perl kafsssearchserver_standalone.pl
  perl kafsssearchserver_standalone.pl --listenport=9090
  perl kafsssearchserver_standalone.pl --host=remote-db --port=5433
  perl kafsssearchserver_standalone.pl --numthreads=10

Note:
  For NGINX FastCGI environment, use kafsssearchserver_fastcgi.pl instead.

EOF
}

#
# HTTP Server Class
#

package KmerSearchWebServer;
use base qw(HTTP::Server::Simple::CGI);

sub new {
    my ($class, $port) = @_;
    my $self = $class->SUPER::new($port);
    $self->{active_children} = {};
    $self->{child_count} = 0;
    return $self;
}

sub setup {
    my ($self, %config) = @_;
    $self->{config} = \%config;
    $self->{max_children} = $config{numthreads} || 5;
    $self->{sqlite_path} = $config{sqlite_path};
    $self->{clean_limit} = $config{clean_limit};
    $self->{job_timeout} = $config{job_timeout};
    $self->{max_jobs} = $config{max_jobs};
    $self->{cleanup_interval} = $config{cleanup_interval};
    
    # Recover existing jobs on startup
    $self->recover_existing_jobs();
    
    # Start cleanup timer
    $self->start_cleanup_timer();
    
    # Setup signal handlers for child process management
    $SIG{CHLD} = sub {
        while ((my $pid = waitpid(-1, POSIX::WNOHANG)) > 0) {
            delete $self->{active_children}->{$pid};
            $self->{child_count}--;
        }
    };
    
    $SIG{INT} = $SIG{TERM} = sub {
        print STDERR "\nShutting down server...\n";
        $self->cleanup_children();
        exit 0;
    };
}

sub cleanup_children {
    my ($self) = @_;
    
    for my $pid (keys %{$self->{active_children}}) {
        kill 'TERM', $pid;
    }
    
    # Wait for children to terminate
    my $timeout = 10;
    while ($self->{child_count} > 0 && $timeout > 0) {
        sleep 1;
        $timeout--;
    }
    
    # Force kill any remaining children
    for my $pid (keys %{$self->{active_children}}) {
        kill 'KILL', $pid;
    }
}

sub handle_request {
    my ($self, $cgi) = @_;
    
    # Handle CORS preflight requests
    if ($cgi->request_method() eq 'OPTIONS') {
        $self->send_cors_headers();
        return;
    }
    
    my $path = $cgi->path_info() || '/';
    my $method = $cgi->request_method();
    
    # Route to appropriate handler based on path and method
    if ($path eq '/search' && $method eq 'POST') {
        $self->handle_search_request($cgi);
    } elsif ($path eq '/result' && $method eq 'POST') {
        $self->handle_result_request($cgi);
    } elsif ($path eq '/status' && $method eq 'POST') {
        $self->handle_status_request($cgi);
    } elsif ($path eq '/cancel' && $method eq 'POST') {
        $self->handle_cancel_request($cgi);
    } elsif ($path eq '/metadata' && $method eq 'GET') {
        $self->handle_metadata_request($cgi);
    } elsif ($path eq '/' && $method eq 'GET') {
        # Redirect GET / to /metadata
        $self->handle_metadata_request($cgi);
    } elsif ($method eq 'GET') {
        # Return 405 Method Not Allowed for all other GET requests
        $self->send_error_response(405, "METHOD_NOT_ALLOWED", "GET method is not allowed for this endpoint. Use POST method or GET /metadata.");
    } else {
        $self->send_error_response(404, "NOT_FOUND", "Endpoint not found. Use POST /search, /result, /status, or /cancel for async job management.");
    }
}

sub send_cors_headers {
    print "Content-Type: text/plain\r\n";
    print "Access-Control-Allow-Origin: *\r\n";
    print "Access-Control-Allow-Methods: POST, OPTIONS\r\n";
    print "Access-Control-Allow-Headers: Content-Type\r\n\r\n";
}

sub send_error_response {
    my ($self, $status_code, $error_code, $message) = @_;
    
    my $status_text = {
        400 => "Bad Request",
        404 => "Not Found", 
        405 => "Method Not Allowed",
        500 => "Internal Server Error",
        503 => "Service Unavailable"
    }->{$status_code} || "Error";
    
    print "Status: $status_code $status_text\r\n";
    print "Content-Type: application/json\r\n";
    print "Access-Control-Allow-Origin: *\r\n";
    print "Access-Control-Allow-Methods: POST, OPTIONS\r\n";
    print "Access-Control-Allow-Headers: Content-Type\r\n";
    
    # Add rate limit headers
    my $current_jobs = $self->get_current_job_count();
    print "X-Job-Queue-Size: $current_jobs\r\n";
    print "X-Job-Queue-Limit: " . $self->{max_jobs} . "\r\n";
    print "\r\n";
    
    print encode_json({
        error => JSON::true,
        message => $message,
        code => $error_code
    });
}

sub send_success_response {
    my ($self, $data, $cgi) = @_;

    # Check if client accepts compression (zstd preferred over gzip)
    my $accept_encoding = $cgi ? ($cgi->http('Accept-Encoding') || '') : '';
    my $use_zstd = ($accept_encoding =~ /zstd/i);
    my $use_gzip = ($accept_encoding =~ /gzip/i);

    # Add success field to response
    $data->{success} = JSON::true;
    my $json_body = encode_json($data);

    print "Status: 200 OK\r\n";
    print "Content-Type: application/json\r\n";
    print "Access-Control-Allow-Origin: *\r\n";
    print "Access-Control-Allow-Methods: POST, OPTIONS\r\n";
    print "Access-Control-Allow-Headers: Content-Type, Accept-Encoding\r\n";

    # Add rate limit headers
    my $current_jobs = $self->get_current_job_count();
    print "X-Job-Queue-Size: $current_jobs\r\n";
    print "X-Job-Queue-Limit: " . $self->{max_jobs} . "\r\n";

    if ($use_zstd && length($json_body) > 100) {
        # Compress response body with zstd
        my $compressed;
        if (zstd(\$json_body => \$compressed)) {
            print "Content-Encoding: zstd\r\n";
            print "\r\n";
            binmode STDOUT;
            print $compressed;
        } else {
            # Fallback to uncompressed on error
            print "\r\n";
            print $json_body;
        }
    } elsif ($use_gzip && length($json_body) > 100) {
        # Compress response body with gzip
        my $compressed;
        if (gzip(\$json_body => \$compressed)) {
            print "Content-Encoding: gzip\r\n";
            print "\r\n";
            binmode STDOUT;
            print $compressed;
        } else {
            # Fallback to uncompressed on error
            print "\r\n";
            print $json_body;
        }
    } else {
        print "\r\n";
        print $json_body;
    }
}

sub parse_json_request {
    my ($self, $cgi) = @_;

    my $content_type = $cgi->http('Content-Type') || '';
    my $content_encoding = $cgi->http('Content-Encoding') || '';

    if ($content_type !~ m{application/json}i) {
        die "Content-Type must be application/json";
    }

    my $raw_content;
    if ($ENV{REQUEST_METHOD} eq 'POST') {
        $raw_content = $cgi->param('POSTDATA');
    }

    if (!$raw_content) {
        # Try reading from STDIN
        local $/;
        $raw_content = <STDIN>;
    }

    if (!$raw_content) {
        die "No JSON data received";
    }

    # Decompress if compressed
    my $json_text;
    if ($content_encoding =~ /zstd/i) {
        IO::Uncompress::UnZstd::unzstd(\$raw_content => \$json_text)
            or die "Failed to decompress zstd request: $IO::Uncompress::UnZstd::UnZstdError";
    } elsif ($content_encoding =~ /gzip/i) {
        IO::Uncompress::Gunzip::gunzip(\$raw_content => \$json_text)
            or die "Failed to decompress gzip request: $IO::Uncompress::Gunzip::GunzipError";
    } else {
        $json_text = $raw_content;
    }

    my $data = decode_json($json_text);
    return $data;
}

sub handle_search_request {
    my ($self, $cgi) = @_;

    eval {
        # Parse JSON request
        my $request = $self->parse_json_request($cgi);

        # Check job queue limit
        my $current_jobs = $self->get_current_job_count();
        if ($current_jobs >= $self->{max_jobs}) {
            $self->send_error_response(503, "QUEUE_FULL",
                "Job queue is full. Maximum concurrent jobs: " . $self->{max_jobs});
            return;
        }

        # Validate required fields - queryseq is required, querylabel has default
        if (!$request->{queryseq}) {
            $self->send_error_response(400, "INVALID_REQUEST",
                "Missing required field: queryseq");
            return;
        }
        $request->{querylabel} ||= 'queryseq';

        # Handle database/db mutual exclusion
        if (defined $request->{database} && $request->{database} ne '' &&
            defined $request->{db} && $request->{db} ne '') {
            $self->send_error_response(400, "INVALID_REQUEST",
                "Cannot specify both 'database' and 'db'. Use one or the other.");
            return;
        }
        $request->{database} = $request->{database} || $request->{db} || $default_database;
        $request->{db} = $request->{database};  # Keep db for compatibility

        # Handle index vs individual params mutual exclusion
        my @index_params = qw(kmersize occurbitlen maxpappear maxnappear precludehighfreqkmer);
        if (defined $request->{index} && $request->{index} ne '') {
            for my $param (@index_params) {
                if (defined $request->{$param} && $request->{$param} ne '') {
                    $self->send_error_response(400, "INVALID_REQUEST",
                        "Cannot specify both 'index' and '$param'. Use 'index' alone or individual parameters.");
                    return;
                }
            }

            # Parse index name to extract parameters
            my $parsed = main::parse_gin_index_name($request->{index});
            if (!$parsed) {
                $self->send_error_response(400, "INVALID_REQUEST",
                    "Invalid index name format: $request->{index}");
                return;
            }

            $request->{kmersize} = $parsed->{kmer_size};
            $request->{occurbitlen} = $parsed->{occur_bitlen};
            $request->{maxpappear} = $parsed->{max_appearance_rate};
            $request->{maxnappear} = $parsed->{max_appearance_nrow};
            $request->{precludehighfreqkmer} = $parsed->{preclude_highfreq_kmer};
            $request->{index_name} = $request->{index};
        } else {
            # Apply defaults for individual params if not specified
            $request->{kmersize} = $request->{kmersize} // ($default_kmersize ne '' ? $default_kmersize : undef);
            $request->{occurbitlen} = $request->{occurbitlen} // ($default_occurbitlen ne '' ? $default_occurbitlen : undef);
            $request->{maxpappear} = $request->{maxpappear} // ($default_maxpappear ne '' ? $default_maxpappear : undef);
            $request->{maxnappear} = $request->{maxnappear} // ($default_maxnappear ne '' ? $default_maxnappear : undef);
            $request->{precludehighfreqkmer} = $request->{precludehighfreqkmer} // ($default_precludehighfreqkmer ne '' ? $default_precludehighfreqkmer : undef);
        }

        # Validate maxpappear precision
        if (defined $request->{maxpappear} && $request->{maxpappear} ne '') {
            my $error = main::validate_maxpappear_precision($request->{maxpappear}, "Request");
            if ($error) {
                $self->send_error_response(400, "INVALID_REQUEST", $error);
                return;
            }
        }

        # Set other defaults
        $request->{subset} ||= $default_subset;
        $request->{maxnseq} ||= $default_maxnseq;
        $request->{minscore} ||= $default_minscore;
        $request->{minpsharedkmer} ||= $default_minpsharedkmer;
        $request->{mode} ||= $default_mode;

        # Normalize and validate mode
        my $normalized_mode = normalize_mode($request->{mode});
        if (!$normalized_mode) {
            $self->send_error_response(400, "INVALID_REQUEST",
                "Invalid mode: $request->{mode}. Must be 'minimum'/'min', 'matchscore'/'score', 'sequence'/'seq', or 'maximum'/'max'");
            return;
        }
        $request->{mode} = $normalized_mode;

        # Validate values
        if ($request->{maxnseq} > $maxmaxnseq) {
            $self->send_error_response(400, "INVALID_REQUEST",
                "maxnseq value ($request->{maxnseq}) exceeds maximum allowed value ($maxmaxnseq)");
            return;
        }

        # Validate minpsharedkmer if specified
        if (defined $request->{minpsharedkmer} && $request->{minpsharedkmer} ne '') {
            if ($request->{minpsharedkmer} < 0.0 || $request->{minpsharedkmer} > 1.0) {
                $self->send_error_response(400, "INVALID_REQUEST",
                    "minpsharedkmer value ($request->{minpsharedkmer}) must be between 0.0 and 1.0");
                return;
            }
        }

        # Validate database exists in available_databases (if configured)
        if (@available_databases > 0) {
            my $db_found = 0;
            for my $db (@available_databases) {
                if ($db eq $request->{database}) {
                    $db_found = 1;
                    last;
                }
            }
            if (!$db_found) {
                $self->send_error_response(400, "INVALID_REQUEST",
                    "Database '$request->{database}' is not in the list of available databases.");
                return;
            }
        }

        # Validate subset exists (if configured)
        if (defined $request->{subset} && $request->{subset} ne '' && @available_subsets > 0) {
            my $subset_spec = "$request->{database}:$request->{subset}";
            my $subset_found = 0;
            for my $sub (@available_subsets) {
                if ($sub eq $subset_spec || $sub eq $request->{subset}) {
                    $subset_found = 1;
                    last;
                }
            }
            if (!$subset_found) {
                $self->send_error_response(400, "INVALID_REQUEST",
                    "Subset '$request->{subset}' is not available for database '$request->{database}'.");
                return;
            }
        }

        # Select GIN index if not already specified
        if (!$request->{index_name}) {
            my $index_result = $self->select_gin_index_for_request($request);
            if ($index_result->{error}) {
                if ($index_result->{matching_indexes}) {
                    $self->send_error_response(400, "MULTIPLE_INDEX_MATCH", {
                        message => $index_result->{error},
                        matching_indexes => $index_result->{matching_indexes}
                    });
                } else {
                    $self->send_error_response(400, "INVALID_REQUEST", $index_result->{error});
                }
                return;
            }

            $request->{index_name} = $index_result->{index_name};
            $request->{kmersize} = $index_result->{params}->{kmer_size};
            $request->{occurbitlen} = $index_result->{params}->{occur_bitlen};
            $request->{maxpappear} = $index_result->{params}->{max_appearance_rate};
            $request->{maxnappear} = $index_result->{params}->{max_appearance_nrow};
            $request->{precludehighfreqkmer} = $index_result->{params}->{preclude_highfreq_kmer};
        }

        # Generate job ID with retry logic
        my $job_id;
        my $max_retries = 10;
        for my $retry (1..$max_retries) {
            $job_id = main::generate_job_id();

            # Try to insert job into database
            if ($self->create_job($job_id, $request)) {
                last;
            }

            if ($retry == $max_retries) {
                $self->send_error_response(500, "INTERNAL_ERROR",
                    "Failed to generate unique job ID after $max_retries retries");
                return;
            }
        }

        # Start background job
        $self->start_background_job($job_id, $request);

        # Return job ID to client
        $self->send_success_response({ job_id => $job_id });

    };

    if ($@) {
        $self->send_error_response(400, "INVALID_REQUEST", "Request error: $@");
    }
}

sub handle_result_request {
    my ($self, $cgi) = @_;
    
    eval {
        my $request = $self->parse_json_request($cgi);
        
        if (!$request->{job_id}) {
            $self->send_error_response(400, "INVALID_REQUEST", "Missing job_id field");
            return;
        }
        
        my $job_id = $request->{job_id};
        
        # Check results table first
        my $result = $self->get_job_result($job_id);
        if ($result) {
            # Remove from results table after sending
            $self->delete_job_result($job_id);
            $self->send_success_response($result);
            return;
        }
        
        # Check if job is still running
        if ($self->is_job_running($job_id)) {
            $self->send_success_response({
                status => "running",
                message => "Job is still processing"
            });
            return;
        }
        
        # Job not found
        $self->send_error_response(404, "JOB_NOT_FOUND", "Job not found");
        
    };
    
    if ($@) {
        $self->send_error_response(400, "INVALID_REQUEST", "Request error: $@");
    }
}

sub handle_status_request {
    my ($self, $cgi) = @_;
    
    eval {
        my $request = $self->parse_json_request($cgi);
        
        if (!$request->{job_id}) {
            $self->send_error_response(400, "INVALID_REQUEST", "Missing job_id field");
            return;
        }
        
        my $job_id = $request->{job_id};
        
        # Check if result is ready
        if ($self->get_job_result($job_id)) {
            $self->send_success_response({
                status => "completed",
                created_time => substr($job_id, 0, 15)  # Extract timestamp from job_id
            });
            return;
        }
        
        # Check if job is still running
        if ($self->is_job_running($job_id)) {
            $self->send_success_response({
                status => "running",
                created_time => substr($job_id, 0, 15)  # Extract timestamp from job_id
            });
            return;
        }
        
        # Job not found
        $self->send_error_response(404, "JOB_NOT_FOUND", "Job not found");
        
    };
    
    if ($@) {
        $self->send_error_response(400, "INVALID_REQUEST", "Request error: $@");
    }
}

sub handle_cancel_request {
    my ($self, $cgi) = @_;
    
    eval {
        my $request = $self->parse_json_request($cgi);
        
        if (!$request->{job_id}) {
            $self->send_error_response(400, "INVALID_REQUEST", "Missing job_id field");
            return;
        }
        
        my $job_id = $request->{job_id};
        
        # Try to cancel the job
        if ($self->cancel_job($job_id)) {
            $self->send_success_response({
                status => "cancelled",
                message => "Job has been cancelled"
            });
        } else {
            $self->send_error_response(404, "JOB_NOT_FOUND", "Job not found or already completed");
        }
        
    };
    
    if ($@) {
        $self->send_error_response(400, "INVALID_REQUEST", "Request error: $@");
    }
}

sub handle_metadata_request {
    my ($self, $cgi) = @_;

    eval {
        my $response = {
            server_version => $VERSION,
            default_database => $default_database,
            default_maxnseq => $default_maxnseq,
            default_minscore => $default_minscore,
            available_databases => \@available_databases,
            available_subsets => \@available_subsets,
            available_indices => \@available_indices,
            accept_gzip_request => JSON::true,
            accept_zstd_request => JSON::true,
            supported_endpoints => ["/search", "/result", "/status", "/cancel", "/metadata"]
        };

        # Add optional fields only if they have values
        $response->{default_subset} = $default_subset if $default_subset ne '';
        $response->{default_kmersize} = int($default_kmersize) if $default_kmersize ne '';
        $response->{default_occurbitlen} = int($default_occurbitlen) if $default_occurbitlen ne '';
        $response->{default_maxpappear} = $default_maxpappear + 0 if $default_maxpappear ne '';
        $response->{default_maxnappear} = int($default_maxnappear) if $default_maxnappear ne '';
        $response->{default_precludehighfreqkmer} = ($default_precludehighfreqkmer ? JSON::true : JSON::false) if $default_precludehighfreqkmer ne '';

        $self->send_success_response($response, $cgi);
    };

    if ($@) {
        $self->send_error_response(500, "INTERNAL_ERROR", "Server error: $@");
    }
}


# SQLite database operations
sub get_current_job_count {
    my ($self) = @_;
    
    my $dbh = DBI->connect("dbi:SQLite:dbname=" . $self->{sqlite_path}, "", "", {
        AutoCommit => 1,
        PrintError => 0,
        RaiseError => 1,
        ShowErrorStatement => 1,
        AutoInactiveDestroy => 1,
        sqlite_unicode => 1
    });
    
    my $sth = $dbh->prepare("SELECT COUNT(*) FROM kafsssearchserver_jobs WHERE status = 'running'");
    $sth->execute();
    my ($count) = $sth->fetchrow_array();
    $sth->finish();
    $dbh->disconnect();
    
    return $count || 0;
}

sub create_job {
    my ($self, $job_id, $request) = @_;

    my $dbh = DBI->connect("dbi:SQLite:dbname=" . $self->{sqlite_path}, "", "", {
        AutoCommit => 1,
        PrintError => 0,
        RaiseError => 1,
        ShowErrorStatement => 1,
        AutoInactiveDestroy => 1,
        sqlite_unicode => 1
    });

    eval {
        my $timeout_time = main::format_timestamp(time() + $self->{job_timeout});

        $dbh->do(
            "INSERT INTO kafsssearchserver_jobs (job_id, time, querylabel, queryseq, db, subset, index_name, kmersize, occurbitlen, maxpappear, maxnappear, precludehighfreqkmer, maxnseq, minscore, mode, status, timeout_time) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'running', ?)",
            undef,
            $job_id,
            main::format_timestamp(),
            $request->{querylabel},
            $request->{queryseq},
            $request->{db},
            $request->{subset},
            $request->{index_name},
            $request->{kmersize},
            $request->{occurbitlen},
            $request->{maxpappear},
            $request->{maxnappear},
            $request->{precludehighfreqkmer} ? 1 : 0,
            $request->{maxnseq},
            $request->{minscore},
            $request->{mode},
            $timeout_time
        );
    };

    $dbh->disconnect();

    return !$@;  # Return true if no error occurred
}

sub start_background_job {
    my ($self, $job_id, $request) = @_;
    
    my $pid = fork();
    
    if (!defined $pid) {
        die "Cannot fork background job: $!";
    } elsif ($pid == 0) {
        # Child process - execute the search
        $self->execute_search_job($job_id, $request);
        exit 0;
    } else {
        # Parent process - update job with PID
        my $dbh = DBI->connect("dbi:SQLite:dbname=" . $self->{sqlite_path}, "", "", {
            AutoCommit => 1,
            PrintError => 0,
            RaiseError => 1,
            ShowErrorStatement => 1,
            AutoInactiveDestroy => 1,
            sqlite_unicode => 1
        });
        
        $dbh->do("UPDATE kafsssearchserver_jobs SET pid = ? WHERE job_id = ?", undef, $pid, $job_id);
        $dbh->disconnect();
        
        return $pid;
    }
}

sub execute_search_job {
    my ($self, $job_id, $request) = @_;
    
    eval {
        # Connect to PostgreSQL
        my $password = $ENV{PGPASSWORD} || '';
        my $dsn = "DBI:Pg:dbname=$request->{db};host=" . $self->{config}->{host} . ";port=" . $self->{config}->{port};
                
        my $pg_dbh = DBI->connect($dsn, $self->{config}->{username}, $password, {
            AutoCommit => 1,
            PrintError => 0,
            RaiseError => 1,
            ShowErrorStatement => 1,
            AutoInactiveDestroy => 1,
            pg_enable_utf8 => 1
        });
        
        # Get metadata and store kmer_size for response building
        my $metadata = $self->get_metadata_from_meta($pg_dbh);
        $self->{current_kmer_size} = $metadata->{kmer_size};
        
        # Perform the search (reusing existing search logic)
        my $results = $self->perform_database_search($pg_dbh, $request);
        
        # Build response based on mode
        my $response = $self->build_search_response($request, $results);
        
        # Store result in results table
        $self->store_job_result($job_id, $response);
        
        # Remove job from jobs table
        $self->delete_job($job_id);
        
        $pg_dbh->disconnect();
        
    };
    
    if ($@) {
        # Store error result
        my $error_response = {
            status => "failed",
            error => JSON::true,
            message => "Search failed: $@",
            code => "SEARCH_ERROR"
        };
        
        $self->store_job_result($job_id, $error_response);
        $self->delete_job($job_id);
    }
}

sub get_job_result {
    my ($self, $job_id) = @_;
    
    my $dbh = DBI->connect("dbi:SQLite:dbname=" . $self->{sqlite_path}, "", "", {
        AutoCommit => 1,
        PrintError => 0,
        RaiseError => 1,
        ShowErrorStatement => 1,
        AutoInactiveDestroy => 1,
        sqlite_unicode => 1
    });
    
    my $sth = $dbh->prepare("SELECT results FROM kafsssearchserver_results WHERE job_id = ?");
    $sth->execute($job_id);
    my ($results_json) = $sth->fetchrow_array();
    $sth->finish();
    $dbh->disconnect();
    
    return $results_json ? decode_json($results_json) : undef;
}

sub delete_job_result {
    my ($self, $job_id) = @_;
    
    my $dbh = DBI->connect("dbi:SQLite:dbname=" . $self->{sqlite_path}, "", "", {
        AutoCommit => 1,
        PrintError => 0,
        RaiseError => 1,
        ShowErrorStatement => 1,
        AutoInactiveDestroy => 1,
        sqlite_unicode => 1
    });
    
    $dbh->do("DELETE FROM kafsssearchserver_results WHERE job_id = ?", undef, $job_id);
    $dbh->disconnect();
}

sub is_job_running {
    my ($self, $job_id) = @_;
    
    my $dbh = DBI->connect("dbi:SQLite:dbname=" . $self->{sqlite_path}, "", "", {
        AutoCommit => 1,
        PrintError => 0,
        RaiseError => 1,
        ShowErrorStatement => 1,
        AutoInactiveDestroy => 1,
        sqlite_unicode => 1
    });
    
    my $sth = $dbh->prepare("SELECT 1 FROM kafsssearchserver_jobs WHERE job_id = ? AND status = 'running'");
    $sth->execute($job_id);
    my $exists = $sth->fetchrow_array();
    $sth->finish();
    $dbh->disconnect();
    
    return $exists ? 1 : 0;
}

sub cancel_job {
    my ($self, $job_id) = @_;
    
    my $dbh = DBI->connect("dbi:SQLite:dbname=" . $self->{sqlite_path}, "", "", {
        AutoCommit => 1,
        PrintError => 0,
        RaiseError => 1,
        ShowErrorStatement => 1,
        AutoInactiveDestroy => 1,
        sqlite_unicode => 1
    });
    
    # Get job PID
    my $sth = $dbh->prepare("SELECT pid FROM kafsssearchserver_jobs WHERE job_id = ? AND status = 'running'");
    $sth->execute($job_id);
    my ($pid) = $sth->fetchrow_array();
    $sth->finish();
    
    if ($pid) {
        # Kill the process
        kill 'TERM', $pid;
        sleep 1;
        kill 'KILL', $pid;  # Force kill if still running
        
        # Update job status
        $dbh->do("UPDATE kafsssearchserver_jobs SET status = 'cancelled' WHERE job_id = ?", undef, $job_id);
        $dbh->disconnect();
        return 1;
    }
    
    $dbh->disconnect();
    return 0;
}

sub delete_job {
    my ($self, $job_id) = @_;
    
    my $dbh = DBI->connect("dbi:SQLite:dbname=" . $self->{sqlite_path}, "", "", {
        AutoCommit => 1,
        PrintError => 0,
        RaiseError => 1,
        ShowErrorStatement => 1,
        AutoInactiveDestroy => 1,
        sqlite_unicode => 1
    });
    
    $dbh->do("DELETE FROM kafsssearchserver_jobs WHERE job_id = ?", undef, $job_id);
    $dbh->disconnect();
}

sub store_job_result {
    my ($self, $job_id, $result) = @_;
    
    my $dbh = DBI->connect("dbi:SQLite:dbname=" . $self->{sqlite_path}, "", "", {
        AutoCommit => 1,
        PrintError => 0,
        RaiseError => 1,
        ShowErrorStatement => 1,
        AutoInactiveDestroy => 1,
        sqlite_unicode => 1
    });
    
    $dbh->do(
        "INSERT INTO kafsssearchserver_results (job_id, time, results) VALUES (?, ?, ?)",
        undef,
        $job_id,
        main::format_timestamp(),
        encode_json($result)
    );
    
    $dbh->disconnect();
}

sub recover_existing_jobs {
    my ($self) = @_;

    my $dbh = DBI->connect("dbi:SQLite:dbname=" . $self->{sqlite_path}, "", "", {
        AutoCommit => 1,
        PrintError => 0,
        RaiseError => 1,
        ShowErrorStatement => 1,
        AutoInactiveDestroy => 1,
        sqlite_unicode => 1
    });

    # Get all running jobs (with new columns)
    my $sth = $dbh->prepare("SELECT job_id, querylabel, queryseq, db, subset, index_name, kmersize, occurbitlen, maxpappear, maxnappear, precludehighfreqkmer, maxnseq, minscore, mode FROM kafsssearchserver_jobs WHERE status = 'running'");
    $sth->execute();

    my @jobs_to_recover;
    while (my $row = $sth->fetchrow_hashref()) {
        push @jobs_to_recover, $row;
    }
    $sth->finish();
    $dbh->disconnect();

    # Restart each job
    for my $job (@jobs_to_recover) {
        print "Recovering job: $job->{job_id}\n";
        $self->start_background_job($job->{job_id}, $job);
    }

    if (@jobs_to_recover) {
        print "Recovered " . scalar(@jobs_to_recover) . " existing jobs.\n";
    }
}

sub start_cleanup_timer {
    my ($self) = @_;
    
    # Fork a cleanup process
    my $pid = fork();
    
    if (!defined $pid) {
        warn "Cannot fork cleanup process: $!";
        return;
    } elsif ($pid == 0) {
        # Child process - run cleanup loop
        while (1) {
            sleep($self->{cleanup_interval});
            $self->cleanup_old_results();
            $self->cleanup_timeout_jobs();
        }
    } else {
        # Parent process - store cleanup PID
        $self->{cleanup_pid} = $pid;
    }
}

sub cleanup_old_results {
    my ($self) = @_;
    
    my $dbh = DBI->connect("dbi:SQLite:dbname=" . $self->{sqlite_path}, "", "", {
        AutoCommit => 1,
        PrintError => 0,
        RaiseError => 1,
        ShowErrorStatement => 1,
        AutoInactiveDestroy => 1,
        sqlite_unicode => 1
    });
    
    my $cutoff_time = main::format_timestamp(time() - $self->{clean_limit});
    
    my $deleted = $dbh->do("DELETE FROM kafsssearchserver_results WHERE time < ?", undef, $cutoff_time);
    $dbh->disconnect();
    
    if ($deleted > 0) {
        print "Cleaned up $deleted old result(s).\n";
    }
}

sub cleanup_timeout_jobs {
    my ($self) = @_;
    
    my $dbh = DBI->connect("dbi:SQLite:dbname=" . $self->{sqlite_path}, "", "", {
        AutoCommit => 1,
        PrintError => 0,
        RaiseError => 1,
        ShowErrorStatement => 1,
        AutoInactiveDestroy => 1,
        sqlite_unicode => 1
    });
    
    my $current_time = main::format_timestamp();
    
    # Find timed out jobs
    my $sth = $dbh->prepare("SELECT job_id, pid FROM kafsssearchserver_jobs WHERE status = 'running' AND timeout_time < ?");
    $sth->execute($current_time);
    
    my @timeout_jobs;
    while (my ($job_id, $pid) = $sth->fetchrow_array()) {
        push @timeout_jobs, { job_id => $job_id, pid => $pid };
    }
    $sth->finish();
    
    # Kill timed out jobs
    for my $job (@timeout_jobs) {
        if ($job->{pid}) {
            kill 'TERM', $job->{pid};
            sleep 1;
            kill 'KILL', $job->{pid};
        }
        
        # Update status to timeout
        $dbh->do("UPDATE kafsssearchserver_jobs SET status = 'timeout' WHERE job_id = ?", undef, $job->{job_id});
        print "Job $job->{job_id} timed out and was terminated.\n";
    }
    
    $dbh->disconnect();
}

# Search execution methods (reusing existing logic)
sub perform_database_search {
    my ($self, $dbh, $request) = @_;

    # Verify database structure
    eval {
        my $sth = $dbh->prepare("SELECT 1 FROM pg_extension WHERE extname = 'pg_kmersearch'");
        $sth->execute();
        my $ext_exists = $sth->fetchrow_array();
        $sth->finish();

        die "pg_kmersearch extension is not installed in database '$request->{db}'\n"
            unless $ext_exists;

        # Check if kafsss_data table exists
        $sth = $dbh->prepare("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'kafsss_data'");
        $sth->execute();
        my ($table_count) = $sth->fetchrow_array();
        $sth->finish();

        die "Table 'kafsss_data' does not exist in database '$request->{db}'\n"
            unless $table_count > 0;

        # Check if specified index exists
        $sth = $dbh->prepare(<<SQL);
SELECT 1 FROM pg_indexes
WHERE tablename = 'kafsss_data'
  AND indexname = ?
LIMIT 1
SQL
        $sth->execute($request->{index_name});
        my $index_exists = $sth->fetchrow_array();
        $sth->finish();

        die "GIN index '$request->{index_name}' does not exist in database '$request->{db}'.\n"
            unless $index_exists;
    };

    if ($@) {
        die "Database validation failed: $@";
    }

    # Get ovllen from kafsss_meta table
    my $ovllen = $self->get_ovllen_from_meta($dbh);

    # Use request parameters for GUC variables (from selected index)
    my $kmer_size = $request->{kmersize};
    my $use_highfreq_cache = $request->{precludehighfreqkmer};

    # Set kmersearch GUC variables from request parameters
    $self->set_kmersearch_guc_variables_from_request($dbh, $request);

    # Set minimum score if specified
    if (defined $request->{minscore} && $request->{minscore} ne '') {
        eval {
            $dbh->do("SET kmersearch.min_score = $request->{minscore}");
        };
        if ($@) {
            die "Failed to set minimum score: $@";
        }
    }

    # Set minimum shared key rate if specified
    if (defined $request->{minpsharedkmer} && $request->{minpsharedkmer} ne '') {
        eval {
            $dbh->do("SET kmersearch.min_shared_kmer_rate = $request->{minpsharedkmer}");
        };
        if ($@) {
            die "Failed to set minimum shared key rate: $@";
        }
    }
    
    # Validate query sequence
    my $validation_result = $self->validate_query_sequence($request->{queryseq}, $ovllen, $kmer_size);
    if (!$validation_result->{valid}) {
        die "Invalid query sequence: $validation_result->{reason}";
    }
    
    # Build search query
    my $sql;
    my @params;
    
    # Build WHERE clause
    my $where_clause = "WHERE seq =% ?";
    @params = ($request->{queryseq});
    if (defined $request->{subset} && $request->{subset} ne '') {
        $where_clause .= " AND ? = ANY(subset)";
        push @params, $request->{subset};
    }
    
    # Build query based on mode and requirements
    if ($request->{mode} eq 'minimum') {
        if ($request->{maxnseq} == 0) {
            # No limit, no score needed
            $sql = "SELECT seqid FROM kafsss_data $where_clause";
        } else {
            # With limit, need score for ordering but don't output it
            $sql = "SELECT seqid FROM kafsss_data $where_clause ORDER BY kmersearch_matchscore(seq, ?) DESC LIMIT ?";
            push @params, $request->{queryseq}, $request->{maxnseq};
        }
    } elsif ($request->{mode} eq 'sequence') {
        if ($request->{maxnseq} == 0) {
            # No limit, no score needed
            $sql = "SELECT seqid, seq FROM kafsss_data $where_clause";
        } else {
            # With limit, need score for ordering but don't output it
            $sql = "SELECT seqid, seq FROM kafsss_data $where_clause ORDER BY kmersearch_matchscore(seq, ?) DESC LIMIT ?";
            push @params, $request->{queryseq}, $request->{maxnseq};
        }
    } elsif ($request->{mode} eq 'matchscore') {
        if ($request->{maxnseq} == 0) {
            $sql = "SELECT seqid, kmersearch_matchscore(seq, ?) AS score FROM kafsss_data $where_clause ORDER BY score DESC";
            unshift @params, $request->{queryseq};
        } else {
            $sql = "SELECT seqid, kmersearch_matchscore(seq, ?) AS score FROM kafsss_data $where_clause ORDER BY score DESC LIMIT ?";
            unshift @params, $request->{queryseq};
            push @params, $request->{maxnseq};
        }
    } else {
        # maximum mode
        if ($request->{maxnseq} == 0) {
            $sql = "SELECT seqid, kmersearch_matchscore(seq, ?) AS score, seq FROM kafsss_data $where_clause ORDER BY score DESC";
            unshift @params, $request->{queryseq};
        } else {
            $sql = "SELECT seqid, kmersearch_matchscore(seq, ?) AS score, seq FROM kafsss_data $where_clause ORDER BY score DESC LIMIT ?";
            unshift @params, $request->{queryseq};
            push @params, $request->{maxnseq};
        }
    }
    
    my @results = ();
    
    eval {
        my $sth = $dbh->prepare($sql);
        $sth->execute(@params);
        
        if ($request->{mode} eq 'maximum') {
            while (my ($seqid_array, $score, $seq) = $sth->fetchrow_array()) {
                # Parse PostgreSQL array and extract seqid
                my $seqid_str = $self->extract_seqid_string($seqid_array);
                
                push @results, {
                    seqid => [split(/,/, $seqid_str)],
                    matchscore => $score,
                    seq => $seq
                };
            }
        } elsif ($request->{mode} eq 'minimum') {
            while (my ($seqid_array) = $sth->fetchrow_array()) {
                # Parse PostgreSQL array and extract seqid
                my $seqid_str = $self->extract_seqid_string($seqid_array);
                
                push @results, {
                    seqid => [split(/,/, $seqid_str)]
                };
            }
        } elsif ($request->{mode} eq 'sequence') {
            while (my ($seqid_array, $seq) = $sth->fetchrow_array()) {
                # Parse PostgreSQL array and extract seqid
                my $seqid_str = $self->extract_seqid_string($seqid_array);
                
                push @results, {
                    seqid => [split(/,/, $seqid_str)],
                    seq => $seq
                };
            }
        } else {
            # matchscore mode
            while (my ($seqid_array, $score) = $sth->fetchrow_array()) {
                # Parse PostgreSQL array and extract seqid
                my $seqid_str = $self->extract_seqid_string($seqid_array);
                
                push @results, {
                    seqid => [split(/,/, $seqid_str)],
                    matchscore => $score
                };
            }
        }
        
        $sth->finish();
    };
    
    if ($@) {
        die "Search query failed: $@";
    }
    
    return \@results;
}

sub build_search_response {
    my ($self, $request, $results) = @_;

    my $response = {};

    if ($request->{mode} eq 'minimum') {
        # Minimum mode - only results with essential info
        $response = {
            status => "completed",
            index_name => $request->{index_name},
            results => $results
        };
    } else {
        # Normal and maximum modes - include full details
        $response = {
            status => "completed",
            querylabel => $request->{querylabel},
            queryseq => $request->{queryseq},
            db => $request->{db},
            subset => $request->{subset},
            index_name => $request->{index_name},
            kmer_size => $request->{kmersize},
            occur_bitlen => $request->{occurbitlen},
            max_appearance_rate => $request->{maxpappear},
            max_appearance_nrow => $request->{maxnappear},
            preclude_highfreq_kmer => ($request->{precludehighfreqkmer} ? JSON::true : JSON::false),
            maxnseq => $request->{maxnseq},
            minscore => $request->{minscore},
            mode => $request->{mode},
            results => $results
        };
    }

    return $response;
}

sub get_ovllen_from_meta {
    my ($self, $dbh) = @_;

    # Query kafsss_meta table to get ovllen only
    my $sth = $dbh->prepare("SELECT ovllen FROM kafsss_meta LIMIT 1");
    $sth->execute();
    my ($ovllen) = $sth->fetchrow_array();
    $sth->finish();

    if (!defined $ovllen) {
        die "No ovllen found in kafsss_meta table.\n";
    }

    return $ovllen;
}

sub set_kmersearch_guc_variables_from_request {
    my ($self, $dbh, $request) = @_;

    # Set all kmersearch GUC variables from request parameters
    eval {
        $dbh->do("SET kmersearch.kmer_size = $request->{kmersize}");
        $dbh->do("SET kmersearch.occur_bitlen = $request->{occurbitlen}");
        $dbh->do("SET kmersearch.max_appearance_rate = $request->{maxpappear}");
        $dbh->do("SET kmersearch.max_appearance_nrow = $request->{maxnappear}");

        if ($request->{precludehighfreqkmer}) {
            $dbh->do("SET kmersearch.preclude_highfreq_kmer = true");
            $dbh->do("SET kmersearch.force_use_parallel_highfreq_kmer_cache = true");
        } else {
            $dbh->do("SET kmersearch.preclude_highfreq_kmer = false");
            $dbh->do("SET kmersearch.force_use_parallel_highfreq_kmer_cache = false");
        }
    };

    if ($@) {
        die "Failed to set kmersearch GUC variables: $@";
    }
}

sub get_metadata_from_meta {
    my ($self, $dbh) = @_;

    # Query kafsss_meta table to get all metadata
    my $sth = $dbh->prepare(<<SQL);
SELECT ovllen, kmer_size, occur_bitlen, max_appearance_rate, max_appearance_nrow
FROM kafsss_meta LIMIT 1
SQL

    eval {
        $sth->execute();
        my ($ovllen, $kmer_size, $occur_bitlen, $max_appearance_rate, $max_appearance_nrow) = $sth->fetchrow_array();
        $sth->finish();

        if (!defined $ovllen || !defined $kmer_size) {
            die "No metadata found in kafsss_meta table. Please run kafssindex to create indexes first.\n";
        }

        return {
            ovllen => $ovllen,
            kmer_size => $kmer_size,
            occur_bitlen => $occur_bitlen,
            max_appearance_rate => $max_appearance_rate,
            max_appearance_nrow => $max_appearance_nrow
        };
    };

    if ($@) {
        die "Failed to retrieve metadata from kafsss_meta table: $@\n";
    }
}

sub check_highfreq_kmer_exists {
    my ($self, $dbh, $metadata) = @_;
    
    # Check if matching high-frequency k-mer data exists
    my $check_sql = <<SQL;
SELECT COUNT(*) 
FROM kmersearch_highfreq_kmer_meta 
WHERE table_oid = 'kafsss_data'::regclass 
  AND column_name = 'seq'
  AND kmer_size = ?
  AND occur_bitlen = ?
  AND max_appearance_rate = ?
  AND max_appearance_nrow = ?
SQL
    
    my $use_highfreq_cache = 0;
    
    eval {
        my $sth = $dbh->prepare($check_sql);
        $sth->execute(
            $metadata->{kmer_size},
            $metadata->{occur_bitlen} // 0,
            $metadata->{max_appearance_rate} // 0,
            $metadata->{max_appearance_nrow} // 0
        );
        my ($count) = $sth->fetchrow_array();
        $sth->finish();
        
        $use_highfreq_cache = ($count > 0) ? 1 : 0;
    };
    
    if ($@) {
        warn "Warning: Failed to check kmersearch_highfreq_kmer_meta table: $@\n";
    }
    
    return $use_highfreq_cache;
}

sub set_kmersearch_guc_variables {
    my ($self, $dbh, $metadata, $use_highfreq_cache) = @_;
    
    # Set k-mer size
    eval {
        $dbh->do("SET kmersearch.kmer_size = $metadata->{kmer_size}");
    };
    if ($@) {
        die "Failed to set k-mer size: $@";
    }
    
    # Set occur_bitlen if available
    if (defined $metadata->{occur_bitlen}) {
        eval {
            $dbh->do("SET kmersearch.occur_bitlen = $metadata->{occur_bitlen}");
        };
        if ($@) {
            warn "Warning: Failed to set occur_bitlen: $@\n";
        }
    }
    
    # Set max_appearance_rate if available
    if (defined $metadata->{max_appearance_rate}) {
        eval {
            $dbh->do("SET kmersearch.max_appearance_rate = $metadata->{max_appearance_rate}");
        };
        if ($@) {
            warn "Warning: Failed to set max_appearance_rate: $@\n";
        }
    }
    
    # Set max_appearance_nrow if available
    if (defined $metadata->{max_appearance_nrow}) {
        eval {
            $dbh->do("SET kmersearch.max_appearance_nrow = $metadata->{max_appearance_nrow}");
        };
        if ($@) {
            warn "Warning: Failed to set max_appearance_nrow: $@\n";
        }
    }
    
    # Set high-frequency k-mer exclusion parameters
    if ($use_highfreq_cache) {
        eval {
            $dbh->do("SET kmersearch.preclude_highfreq_kmer = true");
            $dbh->do("SET kmersearch.force_use_parallel_highfreq_kmer_cache = true");
        };
        if ($@) {
            warn "Warning: Failed to enable high-frequency k-mer exclusion: $@\n";
        }
    } else {
        eval {
            $dbh->do("SET kmersearch.preclude_highfreq_kmer = false");
            $dbh->do("SET kmersearch.force_use_parallel_highfreq_kmer_cache = false");
        };
        if ($@) {
            warn "Warning: Failed to disable high-frequency k-mer exclusion: $@\n";
        }
    }
}

sub validate_query_sequence {
    my ($self, $sequence, $ovllen, $kmer_size) = @_;
    
    # Check for invalid characters (allow all degenerate nucleotide codes)
    my @invalid_char = $sequence =~ /[^ACGTUMRWSYKVHDBN]/ig;
    if (@invalid_char) {
        my $invalid_chars_str = join('', @invalid_char);
        return {
            valid => 0,
            reason => "Query sequence contains invalid characters '$invalid_chars_str' (only A, C, G, T, U, M, R, W, S, Y, K, V, H, D, B, N are allowed)"
        };
    }
    
    # Check sequence length against ovllen
    my $seq_length = length($sequence);
    if ($seq_length > $ovllen) {
        return {
            valid => 0,
            reason => "Query sequence length ($seq_length bases) exceeds ovllen ($ovllen bases)"
        };
    }
    
    # Check minimum length for pg_kmersearch (requires at least kmer_size bases)
    if ($seq_length < $kmer_size) {
        return {
            valid => 0,
            reason => "Query sequence length ($seq_length bases) is too short (minimum $kmer_size bases required)"
        };
    }
    
    return {
        valid => 1,
        reason => "Valid query sequence"
    };
}

sub extract_seqid_string {
    my ($self, $seqid_array) = @_;
    
    return '' unless defined $seqid_array;
    
    # Parse PostgreSQL array format {"elem1","elem2",...}
    my @seqids = $self->parse_pg_array($seqid_array);
    
    # Remove quotes and spaces from each seqid
    my @clean_seqids = ();
    for my $seqid (@seqids) {
        $seqid =~ s/["'\s]//g;  # Remove double quotes, single quotes, and spaces
        push @clean_seqids, $seqid;
    }
    
    return join(',', @clean_seqids);
}

sub parse_pg_array {
    my ($self, $pg_array_str) = @_;
    
    return () unless defined $pg_array_str;
    
    # Remove outer braces
    $pg_array_str =~ s/^{//;
    $pg_array_str =~ s/}$//;
    
    # Return empty array if empty
    return () if $pg_array_str eq '';
    
    my @elements = ();
    my $current_element = '';
    my $in_quotes = 0;
    my $escaped = 0;
    
    my $i = 0;
    while ($i < length($pg_array_str)) {
        my $char = substr($pg_array_str, $i, 1);
        
        if ($escaped) {
            # Previous character was a backslash, add this character literally
            $current_element .= $char;
            $escaped = 0;
        } elsif ($char eq '\\') {
            # Backslash - escape next character
            $escaped = 1;
        } elsif ($char eq '"') {
            # Quote character
            if ($in_quotes) {
                # Check if this is a doubled quote (PostgreSQL escaping)
                if ($i + 1 < length($pg_array_str) && substr($pg_array_str, $i + 1, 1) eq '"') {
                    # Doubled quote - add literal quote and skip next character
                    $current_element .= '"';
                    $i++; # Skip the next quote
                } else {
                    # End of quoted string
                    $in_quotes = 0;
                }
            } else {
                # Start of quoted string
                $in_quotes = 1;
            }
        } elsif ($char eq ',' && !$in_quotes) {
            # Comma outside quotes - end of current element
            push @elements, $current_element;
            $current_element = '';
        } else {
            # Regular character
            $current_element .= $char;
        }
        $i++;
    }
    
    # Add the last element
    push @elements, $current_element if $current_element ne '' || @elements > 0;

    return @elements;
}

# Select GIN index for a search request
sub select_gin_index_for_request {
    my ($self, $request) = @_;

    # Connect to the database
    my $password = $ENV{PGPASSWORD} || '';
    my $dsn = "DBI:Pg:dbname=$request->{database};host=" . $self->{config}->{host} . ";port=" . $self->{config}->{port};

    my $dbh = eval {
        DBI->connect($dsn, $self->{config}->{username}, $password, {
            AutoCommit => 1,
            PrintError => 0,
            RaiseError => 1,
            ShowErrorStatement => 1,
            AutoInactiveDestroy => 1,
            pg_enable_utf8 => 1
        });
    };

    if (!$dbh || $@) {
        return { error => "Cannot connect to database '$request->{database}': " . ($@ || $DBI::errstr) };
    }

    # Get GIN indexes
    my $indexes = main::get_gin_indexes($dbh);

    # Build target params from request
    my $target_params = {
        kmer_size => $request->{kmersize},
        occur_bitlen => $request->{occurbitlen},
        max_appearance_rate => $request->{maxpappear},
        max_appearance_nrow => $request->{maxnappear},
        preclude_highfreq_kmer => $request->{precludehighfreqkmer}
    };

    my $result = main::select_gin_index($dbh, $indexes, $target_params, $request->{database});

    $dbh->disconnect();

    return $result;
}


1;
