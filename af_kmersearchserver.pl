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

# Version number
my $VERSION = "1.0.0";

# Default values - Configure these for your environment
# These values will be used when not specified in API requests
my $default_host = $ENV{PGHOST} || 'localhost';           # PostgreSQL host
my $default_port = $ENV{PGPORT} || 5432;                  # PostgreSQL port
my $default_user = $ENV{PGUSER} || getpwuid($<);          # PostgreSQL username
my $default_password = $ENV{PGPASSWORD} || '';             # PostgreSQL password
my $default_database = '';       # Set default database name here (e.g., 'mykmersearch')
my $default_partition = '';      # Set default partition name here (e.g., 'bacteria')
my $default_maxnseq = 1000;      # Set default maxnseq value here
my $maxmaxnseq = 100000;         # Maximum allowed maxnseq value
my $default_minscore = '';       # Set default minscore value here (empty = use pg_kmersearch default)
my $default_minpsharedkey = '';  # Set default minimum shared key rate here (empty = use pg_kmersearch default)
my $default_mode = 'normal';     # Set default mode (minimum, normal, maximum)
my $default_listen_port = 8080;  # HTTP server listen port
my $default_numthreads = 5;      # Number of parallel request processing threads

# SQLite job management settings
my $default_sqlite_path = './af_kmersearchserver.sqlite';  # SQLite database path
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
    'listen-port=i' => \$listen_port,
    'numthreads=i' => \$numthreads,
    'sqlite-path=s' => \$sqlite_path,
    'clean-limit=i' => \$clean_limit,
    'job-timeout=i' => \$job_timeout,
    'max-jobs=i' => \$max_jobs,
    'cleanup-interval=i' => \$cleanup_interval,
    'help|h' => \$help,
) or die "Error in command line arguments\n";

# Show help
if ($help) {
    print_help();
    exit 0;
}

print "af_kmersearchserver version $VERSION\n";
print "PostgreSQL Host: $host\n";
print "PostgreSQL Port: $port\n";
print "PostgreSQL Username: $username\n";
print "Listen Port: $listen_port\n";
print "Number of threads: $numthreads\n";
print "SQLite Database: $sqlite_path\n";
print "Max concurrent jobs: $max_jobs\n";
print "Job timeout: $job_timeout seconds\n";
print "Result retention: $clean_limit seconds\n";

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
        RaiseError => 1,
        AutoCommit => 1,
        sqlite_unicode => 1
    }) or die "Cannot connect to SQLite database '$sqlite_path': $DBI::errstr\n";
    
    # Enable WAL mode for better concurrent access
    $dbh->do("PRAGMA journal_mode=WAL");
    $dbh->do("PRAGMA synchronous=NORMAL");
    $dbh->do("PRAGMA cache_size=10000");
    $dbh->do("PRAGMA temp_store=memory");
    
    # Create jobs table
    $dbh->do(<<'SQL');
CREATE TABLE IF NOT EXISTS af_kmersearchserver_jobs (
    job_id TEXT PRIMARY KEY,
    time TEXT NOT NULL,
    querylabel TEXT NOT NULL,
    queryseq TEXT NOT NULL,
    db TEXT NOT NULL,
    partition TEXT,
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
CREATE TABLE IF NOT EXISTS af_kmersearchserver_results (
    job_id TEXT PRIMARY KEY,
    time TEXT NOT NULL,
    results TEXT NOT NULL
)
SQL

    # Create indexes for better performance
    $dbh->do("CREATE INDEX IF NOT EXISTS idx_jobs_status ON af_kmersearchserver_jobs(status)");
    $dbh->do("CREATE INDEX IF NOT EXISTS idx_jobs_time ON af_kmersearchserver_jobs(time)");
    $dbh->do("CREATE INDEX IF NOT EXISTS idx_results_time ON af_kmersearchserver_results(time)");
    
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
    my $base64_part = encode_base64($random_bytes, '');  # 32 characters, no newlines
    return "$timestamp-$base64_part";
}

sub format_timestamp {
    my ($time) = @_;
    $time ||= time();
    return strftime("%Y%m%dT%H%M%S", localtime($time));
}

sub print_help {
    print <<EOF;
af_kmersearchserver version $VERSION

Usage: perl af_kmersearchserver_standalone.pl [options]

REST API server for k-mer search using af_kmersearch database.

Options:
  --host=HOST         PostgreSQL server host (default: \$PGHOST or localhost)
  --port=PORT         PostgreSQL server port (default: \$PGPORT or 5432)
  --username=USER     PostgreSQL username (default: \$PGUSER or current user)
  --listen-port=PORT  HTTP server listen port (default: 8080)
  --numthreads=INT    Number of parallel request processing threads (default: 5)
  --sqlite-path=PATH  SQLite database file path (default: ./af_kmersearchserver.sqlite)
  --clean-limit=INT   Result retention period in seconds (default: 86400)
  --job-timeout=INT   Job timeout in seconds (default: 1800)
  --max-jobs=INT      Maximum concurrent jobs (default: 10)
  --cleanup-interval=INT Cleanup interval in seconds (default: 300)
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
    "partition": "partition_name",      // optional, uses default if configured
    "maxnseq": 1000,                   // optional, uses default if configured
    "minscore": 10,                    // optional, uses default if configured
    "minpsharedkey": 0.9               // optional, uses pg_kmersearch default if not specified
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
    "partition": "partition_name",
    "maxnseq": 1000,
    "minscore": 10,
    "results": [
      {
        "correctedscore": 95,
        "seqid": ["AB123:1:100", "CD456:50:150"]
      },
      {
        "correctedscore": 87,
        "seqid": ["EF789:200:300"]
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
    "part": {
      "bacteria": {"nseq": 5000, "nchar": 500000000},
      "archaea": {"nseq": 2000, "nchar": 200000000}
    }
  }

Examples:
  perl af_kmersearchserver_standalone.pl
  perl af_kmersearchserver_standalone.pl --listen-port=9090
  perl af_kmersearchserver_standalone.pl --host=remote-db --port=5433
  perl af_kmersearchserver_standalone.pl --numthreads=10

Note:
  For NGINX FastCGI environment, use af_kmersearchserver_fastcgi.pl instead.

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
    } elsif ($path eq '/search' && $method eq 'GET') {
        # Return 405 Method Not Allowed for GET on /search
        $self->send_error_response(405, "METHOD_NOT_ALLOWED", "GET method is not allowed for /search endpoint. Use POST method.");
    } elsif ($method eq 'GET') {
        # Return 405 Method Not Allowed for all other GET requests
        $self->send_error_response(405, "METHOD_NOT_ALLOWED", "GET method is not allowed. Use POST method.");
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
    my ($self, $data) = @_;
    
    print "Status: 200 OK\r\n";
    print "Content-Type: application/json\r\n";
    print "Access-Control-Allow-Origin: *\r\n";
    print "Access-Control-Allow-Methods: POST, OPTIONS\r\n";
    print "Access-Control-Allow-Headers: Content-Type\r\n";
    
    # Add rate limit headers
    my $current_jobs = $self->get_current_job_count();
    print "X-Job-Queue-Size: $current_jobs\r\n";
    print "X-Job-Queue-Limit: " . $self->{max_jobs} . "\r\n";
    print "\r\n";
    
    print encode_json($data);
}

sub parse_json_request {
    my ($self, $cgi) = @_;
    
    my $content_type = $cgi->http('Content-Type') || '';
    
    if ($content_type !~ m{application/json}i) {
        die "Content-Type must be application/json";
    }
    
    my $json_text;
    if ($ENV{REQUEST_METHOD} eq 'POST') {
        $json_text = $cgi->param('POSTDATA');
    }
    
    if (!$json_text) {
        # Try reading from STDIN
        local $/;
        $json_text = <STDIN>;
    }
    
    if (!$json_text) {
        die "No JSON data received";
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
        
        # Validate required fields
        for my $field (qw(querylabel queryseq)) {
            if (!$request->{$field}) {
                $self->send_error_response(400, "INVALID_REQUEST", 
                    "Missing required field: $field");
                return;
            }
        }
        
        # Set defaults
        $request->{db} ||= $default_database;
        $request->{partition} ||= $default_partition;
        $request->{maxnseq} ||= $default_maxnseq;
        $request->{minscore} ||= $default_minscore;
        $request->{minpsharedkey} ||= $default_minpsharedkey;
        $request->{mode} ||= $default_mode;
        
        # Validate values
        if ($request->{maxnseq} > $maxmaxnseq) {
            $self->send_error_response(400, "INVALID_REQUEST",
                "maxnseq value ($request->{maxnseq}) exceeds maximum allowed value ($maxmaxnseq)");
            return;
        }
        
        # Validate minpsharedkey if specified
        if (defined $request->{minpsharedkey} && $request->{minpsharedkey} ne '') {
            if ($request->{minpsharedkey} < 0.0 || $request->{minpsharedkey} > 1.0) {
                $self->send_error_response(400, "INVALID_REQUEST",
                    "minpsharedkey value ($request->{minpsharedkey}) must be between 0.0 and 1.0");
                return;
            }
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


# SQLite database operations
sub get_current_job_count {
    my ($self) = @_;
    
    my $dbh = DBI->connect("dbi:SQLite:dbname=" . $self->{sqlite_path}, "", "", {
        RaiseError => 1,
        AutoCommit => 1,
        sqlite_unicode => 1
    });
    
    my $sth = $dbh->prepare("SELECT COUNT(*) FROM af_kmersearchserver_jobs WHERE status = 'running'");
    $sth->execute();
    my ($count) = $sth->fetchrow_array();
    $sth->finish();
    $dbh->disconnect();
    
    return $count || 0;
}

sub create_job {
    my ($self, $job_id, $request) = @_;
    
    my $dbh = DBI->connect("dbi:SQLite:dbname=" . $self->{sqlite_path}, "", "", {
        RaiseError => 1,
        AutoCommit => 1,
        sqlite_unicode => 1
    });
    
    eval {
        my $timeout_time = main::format_timestamp(time() + $self->{job_timeout});
        
        $dbh->do(
            "INSERT INTO af_kmersearchserver_jobs (job_id, time, querylabel, queryseq, db, partition, maxnseq, minscore, mode, status, timeout_time) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'running', ?)",
            undef,
            $job_id,
            main::format_timestamp(),
            $request->{querylabel},
            $request->{queryseq},
            $request->{db},
            $request->{partition},
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
            RaiseError => 1,
            AutoCommit => 1,
            sqlite_unicode => 1
        });
        
        $dbh->do("UPDATE af_kmersearchserver_jobs SET pid = ? WHERE job_id = ?", undef, $pid, $job_id);
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
            RaiseError => 1,
            AutoCommit => 1,
            pg_enable_utf8 => 1
        });
        
        # Get kmer_size and store for response building
        $self->{current_kmer_size} = $self->get_kmer_size_from_meta($pg_dbh);
        
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
        RaiseError => 1,
        AutoCommit => 1,
        sqlite_unicode => 1
    });
    
    my $sth = $dbh->prepare("SELECT results FROM af_kmersearchserver_results WHERE job_id = ?");
    $sth->execute($job_id);
    my ($results_json) = $sth->fetchrow_array();
    $sth->finish();
    $dbh->disconnect();
    
    return $results_json ? decode_json($results_json) : undef;
}

sub delete_job_result {
    my ($self, $job_id) = @_;
    
    my $dbh = DBI->connect("dbi:SQLite:dbname=" . $self->{sqlite_path}, "", "", {
        RaiseError => 1,
        AutoCommit => 1,
        sqlite_unicode => 1
    });
    
    $dbh->do("DELETE FROM af_kmersearchserver_results WHERE job_id = ?", undef, $job_id);
    $dbh->disconnect();
}

sub is_job_running {
    my ($self, $job_id) = @_;
    
    my $dbh = DBI->connect("dbi:SQLite:dbname=" . $self->{sqlite_path}, "", "", {
        RaiseError => 1,
        AutoCommit => 1,
        sqlite_unicode => 1
    });
    
    my $sth = $dbh->prepare("SELECT 1 FROM af_kmersearchserver_jobs WHERE job_id = ? AND status = 'running'");
    $sth->execute($job_id);
    my $exists = $sth->fetchrow_array();
    $sth->finish();
    $dbh->disconnect();
    
    return $exists ? 1 : 0;
}

sub cancel_job {
    my ($self, $job_id) = @_;
    
    my $dbh = DBI->connect("dbi:SQLite:dbname=" . $self->{sqlite_path}, "", "", {
        RaiseError => 1,
        AutoCommit => 1,
        sqlite_unicode => 1
    });
    
    # Get job PID
    my $sth = $dbh->prepare("SELECT pid FROM af_kmersearchserver_jobs WHERE job_id = ? AND status = 'running'");
    $sth->execute($job_id);
    my ($pid) = $sth->fetchrow_array();
    $sth->finish();
    
    if ($pid) {
        # Kill the process
        kill 'TERM', $pid;
        sleep 1;
        kill 'KILL', $pid;  # Force kill if still running
        
        # Update job status
        $dbh->do("UPDATE af_kmersearchserver_jobs SET status = 'cancelled' WHERE job_id = ?", undef, $job_id);
        $dbh->disconnect();
        return 1;
    }
    
    $dbh->disconnect();
    return 0;
}

sub delete_job {
    my ($self, $job_id) = @_;
    
    my $dbh = DBI->connect("dbi:SQLite:dbname=" . $self->{sqlite_path}, "", "", {
        RaiseError => 1,
        AutoCommit => 1,
        sqlite_unicode => 1
    });
    
    $dbh->do("DELETE FROM af_kmersearchserver_jobs WHERE job_id = ?", undef, $job_id);
    $dbh->disconnect();
}

sub store_job_result {
    my ($self, $job_id, $result) = @_;
    
    my $dbh = DBI->connect("dbi:SQLite:dbname=" . $self->{sqlite_path}, "", "", {
        RaiseError => 1,
        AutoCommit => 1,
        sqlite_unicode => 1
    });
    
    $dbh->do(
        "INSERT INTO af_kmersearchserver_results (job_id, time, results) VALUES (?, ?, ?)",
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
        RaiseError => 1,
        AutoCommit => 1,
        sqlite_unicode => 1
    });
    
    # Get all running jobs
    my $sth = $dbh->prepare("SELECT job_id, querylabel, queryseq, db, partition, maxnseq, minscore, mode FROM af_kmersearchserver_jobs WHERE status = 'running'");
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
        RaiseError => 1,
        AutoCommit => 1,
        sqlite_unicode => 1
    });
    
    my $cutoff_time = main::format_timestamp(time() - $self->{clean_limit});
    
    my $deleted = $dbh->do("DELETE FROM af_kmersearchserver_results WHERE time < ?", undef, $cutoff_time);
    $dbh->disconnect();
    
    if ($deleted > 0) {
        print "Cleaned up $deleted old result(s).\n";
    }
}

sub cleanup_timeout_jobs {
    my ($self) = @_;
    
    my $dbh = DBI->connect("dbi:SQLite:dbname=" . $self->{sqlite_path}, "", "", {
        RaiseError => 1,
        AutoCommit => 1,
        sqlite_unicode => 1
    });
    
    my $current_time = main::format_timestamp();
    
    # Find timed out jobs
    my $sth = $dbh->prepare("SELECT job_id, pid FROM af_kmersearchserver_jobs WHERE status = 'running' AND timeout_time < ?");
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
        $dbh->do("UPDATE af_kmersearchserver_jobs SET status = 'timeout' WHERE job_id = ?", undef, $job->{job_id});
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
        
        # Check if af_kmersearch table exists
        $sth = $dbh->prepare("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'af_kmersearch'");
        $sth->execute();
        my ($table_count) = $sth->fetchrow_array();
        $sth->finish();
        
        die "Table 'af_kmersearch' does not exist in database '$request->{db}'\n" 
            unless $table_count > 0;
    };
    
    if ($@) {
        die "Database validation failed: $@";
    }
    
    # Get k-mer size from af_kmersearch_meta table
    my $kmer_size = $self->get_kmer_size_from_meta($dbh);
    
    # Set k-mer size for pg_kmersearch
    eval {
        $dbh->do("SET kmersearch.kmer_size = $kmer_size");
    };
    if ($@) {
        die "Failed to set k-mer size: $@";
    }
    
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
    if (defined $request->{minpsharedkey} && $request->{minpsharedkey} ne '') {
        eval {
            $dbh->do("SET kmersearch.min_shared_ngram_key_rate = $request->{minpsharedkey}");
        };
        if ($@) {
            die "Failed to set minimum shared key rate: $@";
        }
    }
    
    # Set rawscore cache max entries (maxnseq * 2)
    my $rawscore_cache_max_entries = $request->{maxnseq} * 2;
    eval {
        $dbh->do("SET kmersearch.rawscore_cache_max_entries = $rawscore_cache_max_entries");
    };
    if ($@) {
        die "Failed to set rawscore cache max entries: $@";
    }
    
    # Get ovllen from af_kmersearch_meta table for query validation
    my $ovllen = $self->get_ovllen_from_meta($dbh);
    
    # Validate query sequence
    my $validation_result = $self->validate_query_sequence($request->{queryseq}, $ovllen, $kmer_size);
    if (!$validation_result->{valid}) {
        die "Invalid query sequence: $validation_result->{reason}";
    }
    
    # Build search query with subquery for efficient sorting
    my $inner_sql;
    if ($request->{mode} eq 'maximum') {
        $inner_sql = "SELECT seq, seqid FROM af_kmersearch WHERE seq =% ?";
    } else {
        $inner_sql = "SELECT seq, seqid FROM af_kmersearch WHERE seq =% ?";
    }
    
    my @params = ($request->{queryseq});
    
    # Add partition condition if specified
    if (defined $request->{partition} && $request->{partition} ne '') {
        $inner_sql .= " AND ? = ANY(part)";
        push @params, $request->{partition};
    }
    
    # Add ORDER BY and LIMIT to inner query (use rawscore for performance)
    $inner_sql .= " ORDER BY kmersearch_rawscore(seq, ?) DESC LIMIT ?";
    push @params, $request->{queryseq}, $request->{maxnseq};
    
    # Build outer query with corrected score sorting
    my $sql;
    if ($request->{mode} eq 'maximum') {
        $sql = "SELECT kmersearch_correctedscore(seq, ?) AS score, seqid, seq FROM ($inner_sql) selected_rows ORDER BY score DESC";
    } else {
        $sql = "SELECT kmersearch_correctedscore(seq, ?) AS score, seqid FROM ($inner_sql) selected_rows ORDER BY score DESC";
    }
    
    # Add parameters for outer query
    unshift @params, $request->{queryseq};
    
    my @results = ();
    
    eval {
        my $sth = $dbh->prepare($sql);
        $sth->execute(@params);
        
        if ($request->{mode} eq 'maximum') {
            while (my ($score, $seqid_array, $seq) = $sth->fetchrow_array()) {
                # Parse PostgreSQL array and extract seqid
                my $seqid_str = $self->extract_seqid_string($seqid_array);
                
                push @results, {
                    correctedscore => $score,
                    seqid => [split(/,/, $seqid_str)],
                    seq => $seq
                };
            }
        } else {
            while (my ($score, $seqid_array) = $sth->fetchrow_array()) {
                # Parse PostgreSQL array and extract seqid
                my $seqid_str = $self->extract_seqid_string($seqid_array);
                
                push @results, {
                    correctedscore => $score,
                    seqid => [split(/,/, $seqid_str)]
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
        # Minimum mode - only results
        $response = {
            status => "completed",
            results => $results
        };
    } else {
        # Normal and maximum modes - include full details
        $response = {
            status => "completed",
            querylabel => $request->{querylabel},
            queryseq => $request->{queryseq},
            db => $request->{db},
            partition => $request->{partition},
            maxnseq => $request->{maxnseq},
            minscore => $request->{minscore},
            mode => $request->{mode},
            results => $results
        };
        
        if ($request->{mode} ne 'minimum') {
            # Add kmer_size for normal and maximum modes
            # Get kmer_size from database in execute_search_job method
            $response->{kmer_size} = $self->{current_kmer_size} || 15;
        }
    }
    
    return $response;
}

sub get_kmer_size_from_meta {
    my ($self, $dbh) = @_;
    
    # Query af_kmersearch_meta table to get kmer_size value
    my $sth = $dbh->prepare("SELECT kmer_size FROM af_kmersearch_meta LIMIT 1");
    eval {
        $sth->execute();
        my ($kmer_size) = $sth->fetchrow_array();
        $sth->finish();
        
        if (defined $kmer_size) {
            return $kmer_size;
        } else {
            die "No k-mer index found. Please run af_kmerindex to create indexes first.\n";
        }
    };
    
    if ($@) {
        die "Failed to retrieve kmer_size from af_kmersearch_meta table: $@\n";
    }
}

sub get_ovllen_from_meta {
    my ($self, $dbh) = @_;
    
    # Query af_kmersearch_meta table to get ovllen value
    my $sth = $dbh->prepare("SELECT ovllen FROM af_kmersearch_meta LIMIT 1");
    eval {
        $sth->execute();
        my ($ovllen) = $sth->fetchrow_array();
        $sth->finish();
        
        if (defined $ovllen) {
            return $ovllen;
        } else {
            die "No ovllen value found in af_kmersearch_meta table\n";
        }
    };
    
    if ($@) {
        die "Failed to retrieve ovllen from af_kmersearch_meta table: $@\n";
    }
}

sub validate_query_sequence {
    my ($self, $sequence, $ovllen, $kmer_size) = @_;
    
    # Check for invalid characters (allow all degenerate nucleotide codes)
    if ($sequence =~ /[^ACGTUMRWSYKVHDBN]/i) {
        return {
            valid => 0,
            reason => "Query sequence contains invalid characters (only A, C, G, T, U, M, R, W, S, Y, K, V, H, D, B, N are allowed)"
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


1;
