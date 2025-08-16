#!/usr/bin/perl

use strict;
use warnings;
use Getopt::Long;
use DBI;
use JSON;
use CGI::Fast;
use FCGI::ProcManager;
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
my $default_subset = '';      # Set default subset name here (e.g., 'bacteria')
my $default_maxnseq = 1000;      # Set default maxnseq value here
my $maxmaxnseq = 100000;         # Maximum allowed maxnseq value
my $default_minscore = '';       # Set default minscore value here (empty = use pg_kmersearch default)
my $default_minpsharedkey = '';  # Set default minimum shared key rate here (empty = use pg_kmersearch default)
my $default_mode = 'normal';     # Set default mode (minimum, normal, maximum)
my $default_numthreads = 5;      # Number of FastCGI processes

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

# Setup process manager
my $proc_manager = FCGI::ProcManager->new({
    n_processes => $numthreads,
    die_timeout => 60,
});

$proc_manager->pm_manage();

# Initialize SQLite database
print STDERR "Initializing SQLite database...\n";
initialize_sqlite_database($sqlite_path);

print STDERR "kafsssearchserver.fcgi version $VERSION\n";
print STDERR "PostgreSQL Host: $host\n";
print STDERR "PostgreSQL Port: $port\n";
print STDERR "PostgreSQL Username: $username\n";
print STDERR "FastCGI Processes: $numthreads\n";
print STDERR "SQLite Database: $sqlite_path\n";
print STDERR "Max concurrent jobs: $max_jobs\n";
print STDERR "Job timeout: $job_timeout seconds\n";
print STDERR "Result retention: $clean_limit seconds\n";
print STDERR "FastCGI server started.\n";
print STDERR "API endpoints:\n";
print STDERR "  POST /search  - Submit search job\n";
print STDERR "  POST /result  - Get job result\n";
print STDERR "  POST /status  - Get job status\n";
print STDERR "  POST /cancel  - Cancel job\n";

# FastCGI request loop
while (my $cgi = CGI::Fast->new()) {
    $proc_manager->pm_pre_dispatch();
    
    eval {
        handle_request($cgi);
    };
    
    if ($@) {
        print STDERR "Error handling request: $@\n";
        print "Status: 500 Internal Server Error\r\n";
        print "Content-Type: application/json\r\n\r\n";
        print encode_json({
            error => "Internal Server Error",
            message => "An unexpected error occurred"
        });
    }
    
    $proc_manager->pm_post_dispatch();
}

exit 0;

#
# Subroutines
#

sub print_help {
    print <<EOF;
kafsssearchserver.fcgi version $VERSION

Usage: perl kafsssearchserver.fcgi [options]

FastCGI server for asynchronous k-mer search using kafsss database.
Designed to work with NGINX FastCGI.

Options:
  --host=HOST         PostgreSQL server host (default: \$PGHOST or localhost)
  --port=PORT         PostgreSQL server port (default: \$PGPORT or 5432)
  --username=USER     PostgreSQL username (default: \$PGUSER or current user)
  --numthreads=NUM    Number of FastCGI processes (default: 5)
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

NGINX Configuration Example:
  location /api/ {
      include fastcgi_params;
      fastcgi_pass unix:/var/run/kafsss.sock;
      fastcgi_param SCRIPT_FILENAME \$document_root\$fastcgi_script_name;
  }

Spawn-FCGI Usage:
  spawn-fcgi -s /var/run/kafsss.sock -U nginx -G nginx \\
            -u www-data -g www-data -P /var/run/kafsss.pid \\
            -- perl kafsssearchserver.fcgi --numthreads=10

Asynchronous API Usage:
  POST /search   - Submit search job (returns job_id)
  POST /result   - Get job result
  POST /status   - Get job status  
  POST /cancel   - Cancel job
  
  1. Submit search job:
  POST /search
  {
    "querylabel": "sequence_name",      // required
    "queryseq": "ATCGATCGATCG...",     // required
    "db": "database_name",             // optional if default configured
    "subset": "subset_name",      // optional, uses default if configured
    "maxnseq": 1000,                   // optional, uses default if configured
    "minscore": 10,                    // optional, uses default if configured
    "mode": "normal"                   // optional: minimum, normal, maximum
  }
  
  Response: {"job_id": "20250703T120000-AbCdEfGhIjKlMnOpQrStUvWxYz012345"}
  
  2. Check job status:
  POST /status
  {"job_id": "20250703T120000-AbCdEfGhIjKlMnOpQrStUvWxYz012345"}
  
  Response: {"status": "running|completed", "created_time": "20250703T120000"}
  
  3. Get results (removes result after retrieval):
  POST /result
  {"job_id": "20250703T120000-AbCdEfGhIjKlMnOpQrStUvWxYz012345"}
  
  Response: 
  {
    "status": "completed",
    "querylabel": "sequence_name",
    "queryseq": "ATCGATCGATCG...",
    "db": "database_name",
    "results": [
      {
        "correctedscore": 95,
        "seqid": ["AB123:1:100", "CD456:50:150"]
      }
    ]
  }
  
  4. Cancel job:
  POST /cancel
  {"job_id": "20250703T120000-AbCdEfGhIjKlMnOpQrStUvWxYz012345"}
  
  Response: {"status": "cancelled", "message": "Job has been cancelled"}

Notes:
  - Job IDs have format: YYYYMMDDThhmmss-[32-char Base64]
  - Results are automatically cleaned up after 24 hours (configurable)
  - Jobs timeout after 30 minutes (configurable)
  - Maximum 10 concurrent jobs (configurable)

EOF
}

sub handle_request {
    my ($cgi) = @_;
    
    my $path = $cgi->path_info() || '/';
    my $method = $cgi->request_method();
    
    # Set CORS headers
    print "Access-Control-Allow-Origin: *\r\n";
    print "Access-Control-Allow-Methods: POST, OPTIONS\r\n";
    print "Access-Control-Allow-Headers: Content-Type\r\n";
    
    if ($method eq 'OPTIONS') {
        print "Status: 200 OK\r\n";
        print "Content-Type: text/plain\r\n\r\n";
        return;
    }
    
    # Route to appropriate handler based on path
    if ($path eq '/search' && $method eq 'POST') {
        handle_search_async($cgi);
    } elsif ($path eq '/result' && $method eq 'POST') {
        handle_result_request($cgi);
    } elsif ($path eq '/status' && $method eq 'POST') {
        handle_status_request($cgi);
    } elsif ($path eq '/cancel' && $method eq 'POST') {
        handle_cancel_request($cgi);
    } elsif ($path eq '/metadata' && $method eq 'GET') {
        handle_metadata_request($cgi);
    } elsif ($path eq '/search' && $method eq 'GET') {
        # Return 405 Method Not Allowed for GET on /search
        print "Status: 405 Method Not Allowed\r\n";
        print "Allow: POST\r\n";
        print "Content-Type: application/json\r\n\r\n";
        print encode_json({
            error => "Method Not Allowed",
            message => "GET method is not allowed for /search endpoint. Use POST method."
        });
    } elsif ($method eq 'GET') {
        # Return 405 Method Not Allowed for all other GET requests
        print "Status: 405 Method Not Allowed\r\n";
        print "Allow: POST\r\n";
        print "Content-Type: application/json\r\n\r\n";
        print encode_json({
            error => "Method Not Allowed",
            message => "GET method is not allowed. Use POST method."
        });
    } else {
        print "Status: 404 Not Found\r\n";
        print "Content-Type: application/json\r\n\r\n";
        print encode_json({
            error => "Not Found",
            message => "Endpoint not found. Use POST /search, /result, /status, or /cancel for async job management."
        });
    }
}


sub handle_search_async {
    my ($cgi) = @_;
    
    eval {
        # Parse JSON request
        my $request = parse_json_request($cgi);
        
        # Check job queue limit
        my $current_jobs = get_current_job_count();
        if ($current_jobs >= $max_jobs) {
            send_error_response(503, "QUEUE_FULL", 
                "Job queue is full. Maximum concurrent jobs: $max_jobs");
            return;
        }
        
        # Validate required fields
        for my $field (qw(querylabel queryseq)) {
            if (!$request->{$field}) {
                send_error_response(400, "INVALID_REQUEST", 
                    "Missing required field: $field");
                return;
            }
        }
        
        # Set defaults
        $request->{db} ||= $default_database;
        $request->{subset} ||= $default_subset;
        $request->{maxnseq} ||= $default_maxnseq;
        $request->{minscore} ||= $default_minscore;
        $request->{minpsharedkey} ||= $default_minpsharedkey;
        $request->{mode} ||= $default_mode;
        
        # Validate values
        if ($request->{maxnseq} > $maxmaxnseq) {
            send_error_response(400, "INVALID_REQUEST",
                "maxnseq value ($request->{maxnseq}) exceeds maximum allowed value ($maxmaxnseq)");
            return;
        }
        
        # Validate minpsharedkey if specified
        if (defined $request->{minpsharedkey} && $request->{minpsharedkey} ne '') {
            if ($request->{minpsharedkey} < 0.0 || $request->{minpsharedkey} > 1.0) {
                send_error_response(400, "INVALID_REQUEST",
                    "minpsharedkey value ($request->{minpsharedkey}) must be between 0.0 and 1.0");
                return;
            }
        }
        
        # Generate job ID with retry logic
        my $job_id;
        my $max_retries = 10;
        for my $retry (1..$max_retries) {
            $job_id = generate_job_id();
            
            # Try to insert job into database
            if (create_job($job_id, $request)) {
                last;
            }
            
            if ($retry == $max_retries) {
                send_error_response(500, "INTERNAL_ERROR",
                    "Failed to generate unique job ID after $max_retries retries");
                return;
            }
        }
        
        # Start background job
        start_background_job($job_id, $request);
        
        # Return job ID to client
        send_success_response({ job_id => $job_id });
        
    };
    
    if ($@) {
        send_error_response(400, "INVALID_REQUEST", "Request error: $@");
    }
}

sub handle_result_request {
    my ($cgi) = @_;
    
    eval {
        my $request = parse_json_request($cgi);
        
        if (!$request->{job_id}) {
            send_error_response(400, "INVALID_REQUEST", "Missing job_id field");
            return;
        }
        
        my $job_id = $request->{job_id};
        
        # Check results table first
        my $result = get_job_result($job_id);
        if ($result) {
            # Remove from results table after sending
            delete_job_result($job_id);
            send_success_response($result);
            return;
        }
        
        # Check if job is still running
        if (is_job_running($job_id)) {
            send_success_response({
                status => "running",
                message => "Job is still processing"
            });
            return;
        }
        
        # Job not found
        send_error_response(404, "JOB_NOT_FOUND", "Job not found");
        
    };
    
    if ($@) {
        send_error_response(400, "INVALID_REQUEST", "Request error: $@");
    }
}

sub handle_status_request {
    my ($cgi) = @_;
    
    eval {
        my $request = parse_json_request($cgi);
        
        if (!$request->{job_id}) {
            send_error_response(400, "INVALID_REQUEST", "Missing job_id field");
            return;
        }
        
        my $job_id = $request->{job_id};
        
        # Check if result is ready
        if (get_job_result($job_id)) {
            send_success_response({
                status => "completed",
                created_time => substr($job_id, 0, 15)  # Extract timestamp from job_id
            });
            return;
        }
        
        # Check if job is still running
        if (is_job_running($job_id)) {
            send_success_response({
                status => "running",
                created_time => substr($job_id, 0, 15)  # Extract timestamp from job_id
            });
            return;
        }
        
        # Job not found
        send_error_response(404, "JOB_NOT_FOUND", "Job not found");
        
    };
    
    if ($@) {
        send_error_response(400, "INVALID_REQUEST", "Request error: $@");
    }
}

sub handle_cancel_request {
    my ($cgi) = @_;
    
    eval {
        my $request = parse_json_request($cgi);
        
        if (!$request->{job_id}) {
            send_error_response(400, "INVALID_REQUEST", "Missing job_id field");
            return;
        }
        
        my $job_id = $request->{job_id};
        
        # Try to cancel the job
        if (cancel_job($job_id)) {
            send_success_response({
                status => "cancelled",
                message => "Job has been cancelled"
            });
        } else {
            send_error_response(404, "JOB_NOT_FOUND", "Job not found or already completed");
        }
        
    };
    
    if ($@) {
        send_error_response(400, "INVALID_REQUEST", "Request error: $@");
    }
}

sub handle_metadata_request {
    my ($cgi) = @_;
    
    eval {
        send_success_response({
            default_database => $default_database,
            default_subset => $default_subset,
            default_maxnseq => $default_maxnseq,
            default_minscore => $default_minscore,
            server_version => "1.0",
            supported_endpoints => ["/search", "/result", "/status", "/cancel", "/metadata"]
        });
    };
    
    if ($@) {
        send_error_response(500, "INTERNAL_ERROR", "Server error: $@");
    }
}

# Core async job management functions
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
CREATE TABLE IF NOT EXISTS kafsssearchserver_jobs (
    job_id TEXT PRIMARY KEY,
    time TEXT NOT NULL,
    querylabel TEXT NOT NULL,
    queryseq TEXT NOT NULL,
    db TEXT NOT NULL,
    subset TEXT,
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
    
    print STDERR "SQLite database initialized successfully.\n";
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

sub format_timestamp {
    my ($time) = @_;
    $time ||= time();
    return strftime("%Y%m%dT%H%M%S", localtime($time));
}

sub parse_json_request {
    my ($cgi) = @_;
    
    my $content_type = $ENV{CONTENT_TYPE} || '';
    
    if ($content_type !~ m{application/json}i) {
        die "Content-Type must be application/json";
    }
    
    my $json_text = '';
    
    # Try to read from param first (for form-encoded data)
    my $postdata = $cgi->param('POSTDATA');
    if ($postdata) {
        $json_text = $postdata;
    } else {
        # Read from STDIN for raw POST data
        my $content_length = $ENV{CONTENT_LENGTH} || 0;
        if ($content_length > 0) {
            read(STDIN, $json_text, $content_length);
        }
    }
    
    if (!$json_text) {
        die "No JSON data received";
    }
    
    my $data = decode_json($json_text);
    return $data;
}

sub send_error_response {
    my ($status_code, $error_code, $message) = @_;
    
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
    my $current_jobs = get_current_job_count();
    print "X-Job-Queue-Size: $current_jobs\r\n";
    print "X-Job-Queue-Limit: $max_jobs\r\n";
    print "\r\n";
    
    print encode_json({
        error => JSON::true,
        message => $message,
        code => $error_code
    });
}

sub send_success_response {
    my ($data) = @_;
    
    print "Status: 200 OK\r\n";
    print "Content-Type: application/json\r\n";
    print "Access-Control-Allow-Origin: *\r\n";
    print "Access-Control-Allow-Methods: POST, OPTIONS\r\n";
    print "Access-Control-Allow-Headers: Content-Type\r\n";
    
    # Add rate limit headers
    my $current_jobs = get_current_job_count();
    print "X-Job-Queue-Size: $current_jobs\r\n";
    print "X-Job-Queue-Limit: $max_jobs\r\n";
    print "\r\n";
    
    # Add success field to response
    $data->{success} = JSON::true;
    print encode_json($data);
}

# SQLite database operations
sub get_current_job_count {
    my $dbh = DBI->connect("dbi:SQLite:dbname=$sqlite_path", "", "", {
        RaiseError => 1,
        AutoCommit => 1,
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
    my ($job_id, $request) = @_;
    
    my $dbh = DBI->connect("dbi:SQLite:dbname=$sqlite_path", "", "", {
        RaiseError => 1,
        AutoCommit => 1,
        sqlite_unicode => 1
    });
    
    eval {
        my $timeout_time = format_timestamp(time() + $job_timeout);
        
        $dbh->do(
            "INSERT INTO kafsssearchserver_jobs (job_id, time, querylabel, queryseq, db, subset, maxnseq, minscore, mode, status, timeout_time) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'running', ?)",
            undef,
            $job_id,
            format_timestamp(),
            $request->{querylabel},
            $request->{queryseq},
            $request->{db},
            $request->{subset},
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
    my ($job_id, $request) = @_;
    
    my $pid = fork();
    
    if (!defined $pid) {
        die "Cannot fork background job: $!";
    } elsif ($pid == 0) {
        # Child process - execute the search
        execute_search_job($job_id, $request);
        exit 0;
    } else {
        # Parent process - update job with PID
        my $dbh = DBI->connect("dbi:SQLite:dbname=$sqlite_path", "", "", {
            RaiseError => 1,
            AutoCommit => 1,
            sqlite_unicode => 1
        });
        
        $dbh->do("UPDATE kafsssearchserver_jobs SET pid = ? WHERE job_id = ?", undef, $pid, $job_id);
        $dbh->disconnect();
        
        return $pid;
    }
}

sub execute_search_job {
    my ($job_id, $request) = @_;
    
    eval {
        # Connect to PostgreSQL server for validation
        my $password = $default_password;
        my $server_dsn = "DBI:Pg:dbname=postgres;host=$host;port=$port";
        
        my $server_dbh = DBI->connect($server_dsn, $username, $password, {
            RaiseError => 1,
            AutoCommit => 1,
            pg_enable_utf8 => 1
        });
        
        # Validate user and database existence
        validate_user_and_permissions($server_dbh, $username);
        check_database_exists($server_dbh, $request->{db});
        $server_dbh->disconnect();
        
        # Connect to target database
        my $dsn = "DBI:Pg:dbname=$request->{db};host=$host;port=$port";
                
        my $pg_dbh = DBI->connect($dsn, $username, $password, {
            RaiseError => 1,
            AutoCommit => 1,
            pg_enable_utf8 => 1
        });
        
        # Validate database permissions and schema
        validate_database_permissions($pg_dbh, $username);
        validate_database_schema($pg_dbh);
        
        # Get kmer_size and store for response building
        my $current_kmer_size = get_kmer_size_from_meta($pg_dbh);
        
        # Perform the search (reusing existing search logic)
        my $results = perform_database_search($pg_dbh, $request);
        
        # Build response based on mode
        my $response = build_search_response($request, $results, $current_kmer_size);
        
        # Store result in results table
        store_job_result($job_id, $response);
        
        # Remove job from jobs table
        delete_job($job_id);
        
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
        
        store_job_result($job_id, $error_response);
        delete_job($job_id);
    }
}

sub get_job_result {
    my ($job_id) = @_;
    
    my $dbh = DBI->connect("dbi:SQLite:dbname=$sqlite_path", "", "", {
        RaiseError => 1,
        AutoCommit => 1,
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
    my ($job_id) = @_;
    
    my $dbh = DBI->connect("dbi:SQLite:dbname=$sqlite_path", "", "", {
        RaiseError => 1,
        AutoCommit => 1,
        sqlite_unicode => 1
    });
    
    $dbh->do("DELETE FROM kafsssearchserver_results WHERE job_id = ?", undef, $job_id);
    $dbh->disconnect();
}

sub is_job_running {
    my ($job_id) = @_;
    
    my $dbh = DBI->connect("dbi:SQLite:dbname=$sqlite_path", "", "", {
        RaiseError => 1,
        AutoCommit => 1,
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
    my ($job_id) = @_;
    
    my $dbh = DBI->connect("dbi:SQLite:dbname=$sqlite_path", "", "", {
        RaiseError => 1,
        AutoCommit => 1,
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
    my ($job_id) = @_;
    
    my $dbh = DBI->connect("dbi:SQLite:dbname=$sqlite_path", "", "", {
        RaiseError => 1,
        AutoCommit => 1,
        sqlite_unicode => 1
    });
    
    $dbh->do("DELETE FROM kafsssearchserver_jobs WHERE job_id = ?", undef, $job_id);
    $dbh->disconnect();
}

sub store_job_result {
    my ($job_id, $result) = @_;
    
    my $dbh = DBI->connect("dbi:SQLite:dbname=$sqlite_path", "", "", {
        RaiseError => 1,
        AutoCommit => 1,
        sqlite_unicode => 1
    });
    
    $dbh->do(
        "INSERT INTO kafsssearchserver_results (job_id, time, results) VALUES (?, ?, ?)",
        undef,
        $job_id,
        format_timestamp(),
        encode_json($result)
    );
    
    $dbh->disconnect();
}

# Search execution methods (reusing existing logic)
sub perform_database_search {
    my ($dbh, $request) = @_;
    
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
    };
    
    if ($@) {
        die "Database validation failed: $@";
    }
    
    # Get k-mer size from kafsss_meta table
    my $kmer_size = get_kmer_size_from_meta($dbh);
    
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
            $dbh->do("SET kmersearch.min_shared_kmer_rate = $request->{minpsharedkey}");
        };
        if ($@) {
            die "Failed to set minimum shared key rate: $@";
        }
    }
    
    # Get ovllen from kafsss_meta table for query validation
    my $ovllen = get_ovllen_from_meta($dbh);
    
    # Validate query sequence
    my $validation_result = validate_query_sequence($request->{queryseq}, $ovllen, $kmer_size);
    if (!$validation_result->{valid}) {
        die "Invalid query sequence: $validation_result->{reason}";
    }
    
    # Build search query with subquery for efficient sorting
    my $inner_sql;
    if ($request->{mode} eq 'maximum') {
        $inner_sql = "SELECT seq, seqid FROM kafsss_data WHERE seq =% ?";
    } else {
        $inner_sql = "SELECT seq, seqid FROM kafsss_data WHERE seq =% ?";
    }
    
    my @params = ($request->{queryseq});
    
    # Add subset condition if specified
    if (defined $request->{subset} && $request->{subset} ne '') {
        $inner_sql .= " AND ? = ANY(subset)";
        push @params, $request->{subset};
    }
    
    # Add ORDER BY and LIMIT to inner query (use matchscore for performance)
    $inner_sql .= " ORDER BY kmersearch_matchscore(seq, ?) DESC LIMIT ?";
    push @params, $request->{queryseq}, $request->{maxnseq};
    
    # Build outer query with match score sorting
    my $sql;
    if ($request->{mode} eq 'maximum') {
        $sql = "SELECT kmersearch_matchscore(seq, ?) AS score, seqid, seq FROM ($inner_sql) selected_rows ORDER BY score DESC";
    } else {
        $sql = "SELECT kmersearch_matchscore(seq, ?) AS score, seqid FROM ($inner_sql) selected_rows ORDER BY score DESC";
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
                my $seqid_str = extract_seqid_string($seqid_array);
                
                push @results, {
                    correctedscore => $score,
                    seqid => [split(/,/, $seqid_str)],
                    seq => $seq
                };
            }
        } else {
            while (my ($score, $seqid_array) = $sth->fetchrow_array()) {
                # Parse PostgreSQL array and extract seqid
                my $seqid_str = extract_seqid_string($seqid_array);
                
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
    my ($request, $results, $kmer_size) = @_;
    
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
            subset => $request->{subset},
            maxnseq => $request->{maxnseq},
            minscore => $request->{minscore},
            mode => $request->{mode},
            results => $results
        };
        
        if ($request->{mode} ne 'minimum') {
            # Add kmer_size for normal and maximum modes
            $response->{kmer_size} = $kmer_size || 15;
        }
    }
    
    return $response;
}

sub get_kmer_size_from_meta {
    my ($dbh) = @_;
    
    # Query kafsss_meta table to get kmer_size value
    my $sth = $dbh->prepare("SELECT kmer_size FROM kafsss_meta LIMIT 1");
    eval {
        $sth->execute();
        my ($kmer_size) = $sth->fetchrow_array();
        $sth->finish();
        
        if (defined $kmer_size) {
            return $kmer_size;
        } else {
            die "No k-mer index found. Please run kafssindex to create indexes first.\n";
        }
    };
    
    if ($@) {
        die "Failed to retrieve kmer_size from kafsss_meta table: $@\n";
    }
}

sub get_ovllen_from_meta {
    my ($dbh) = @_;
    
    # Query kafsss_meta table to get ovllen value
    my $sth = $dbh->prepare("SELECT ovllen FROM kafsss_meta LIMIT 1");
    eval {
        $sth->execute();
        my ($ovllen) = $sth->fetchrow_array();
        $sth->finish();
        
        if (defined $ovllen) {
            return $ovllen;
        } else {
            die "No ovllen value found in kafsss_meta table\n";
        }
    };
    
    if ($@) {
        die "Failed to retrieve ovllen from kafsss_meta table: $@\n";
    }
}

sub validate_query_sequence {
    my ($sequence, $ovllen, $kmer_size) = @_;
    
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
    my ($seqid_array) = @_;
    
    return '' unless defined $seqid_array;
    
    # Parse PostgreSQL array format {"elem1","elem2",...}
    my @seqids = parse_pg_array($seqid_array);
    
    # Remove quotes and spaces from each seqid
    my @clean_seqids = ();
    for my $seqid (@seqids) {
        $seqid =~ s/["'\s]//g;  # Remove double quotes, single quotes, and spaces
        push @clean_seqids, $seqid;
    }
    
    return join(',', @clean_seqids);
}

sub parse_pg_array {
    my ($pg_array_str) = @_;
    
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

sub get_database_metadata {
    my ($database_name) = @_;
    
    # Connect to PostgreSQL database
    my $password = $default_password;
    my $dsn = "DBI:Pg:dbname=$database_name;host=$host;port=$port";
        
    my $dbh = DBI->connect($dsn, $username, $password, {
        RaiseError => 1,
        AutoCommit => 1,
        pg_enable_utf8 => 1
    }) or die "Cannot connect to database '$database_name': $DBI::errstr";
    
    # Get metadata from kafsss_meta table
    my $sth = $dbh->prepare("SELECT ver, minlen, minsplitlen, ovllen, nseq, nchar, subset, kmer_size FROM kafsss_meta LIMIT 1");
    $sth->execute();
    my ($ver, $minlen, $minsplitlen, $ovllen, $nseq, $nchar, $subset_json, $kmer_size) = $sth->fetchrow_array();
    $sth->finish();
    
    $dbh->disconnect();
    
    # Parse subset JSON
    my $subset_data = {};
    if ($subset_json) {
        eval {
            $subset_data = decode_json($subset_json);
        };
        if ($@) {
            warn "Warning: Failed to parse subset JSON: $@";
            $subset_data = {};
        }
    }
    
    return {
        ver => $ver,
        minlen => int($minlen || 0),
        minsplitlen => int($minsplitlen || 0),
        ovllen => int($ovllen || 0),
        nseq => int($nseq || 0),
        nchar => int($nchar || 0),
        subset => $subset_data,
        kmer_size => int($kmer_size || 0)
    };
}

sub normalize_mode {
    my ($mode) = @_;
    return '' unless defined $mode;
    
    # Normalize mode aliases
    my %mode_aliases = (
        'min' => 'minimum',
        'minimize' => 'minimum',
        'minimum' => 'minimum',
        'normal' => 'normal',
        'max' => 'maximum',
        'maximize' => 'maximum',
        'maximum' => 'maximum'
    );
    
    my $normalized = $mode_aliases{lc($mode)};
    return '' unless $normalized;
    
    # Check if mode is accepted
    return grep { $_ eq $normalized } @accepted_modes ? $normalized : '';
}

sub validate_user_and_permissions {
    my ($dbh, $username) = @_;
    
    # Check if user exists
    my $sth = $dbh->prepare("SELECT 1 FROM pg_user WHERE usename = ?");
    $sth->execute($username);
    my $user_exists = $sth->fetchrow_array();
    $sth->finish();
    
    unless ($user_exists) {
        die "Error: PostgreSQL user '$username' does not exist.\n";
    }
}

sub check_database_exists {
    my ($dbh, $dbname) = @_;
    
    my $sth = $dbh->prepare("SELECT 1 FROM pg_database WHERE datname = ?");
    $sth->execute($dbname);
    my $result = $sth->fetchrow_array();
    $sth->finish();
    
    unless ($result) {
        die "Error: Database '$dbname' does not exist.\n";
    }
}

sub validate_database_permissions {
    my ($dbh, $username) = @_;
    
    # Check if pg_kmersearch extension exists
    my $sth = $dbh->prepare("SELECT 1 FROM pg_extension WHERE extname = 'pg_kmersearch'");
    $sth->execute();
    my $ext_exists = $sth->fetchrow_array();
    $sth->finish();
    
    unless ($ext_exists) {
        die "Error: Extension 'pg_kmersearch' is not installed in this database.\n";
    }
    
    # Check table permissions - server needs SELECT on both tables
    $sth = $dbh->prepare("SELECT has_table_privilege(?, 'kafsss_meta', 'SELECT')");
    $sth->execute($username);
    my $has_meta_perm = $sth->fetchrow_array();
    $sth->finish();
    
    unless ($has_meta_perm) {
        die "Error: User '$username' does not have SELECT permission on kafsss_meta table.\n";
    }
    
    $sth = $dbh->prepare("SELECT has_table_privilege(?, 'kafsss_data', 'SELECT')");
    $sth->execute($username);
    my $has_table_perm = $sth->fetchrow_array();
    $sth->finish();
    
    unless ($has_table_perm) {
        die "Error: User '$username' does not have SELECT permission on kafsss_data table.\n";
    }
}

sub validate_database_schema {
    my ($dbh) = @_;
    
    # Check if required tables exist
    my @required_tables = ('kafsss_meta', 'kafsss_data');
    
    for my $table (@required_tables) {
        my $sth = $dbh->prepare("SELECT 1 FROM information_schema.tables WHERE table_name = ?");
        $sth->execute($table);
        my $table_exists = $sth->fetchrow_array();
        $sth->finish();
        
        unless ($table_exists) {
            die "Error: Required table '$table' does not exist in database.\n";
        }
    }
}