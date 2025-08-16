#!/usr/bin/perl

use strict;
use warnings;
use Getopt::Long;
use DBI;
use POSIX qw(strftime);
use File::Basename;

# Version number
my $VERSION = "1.0.0";

# Default values
my $default_host = $ENV{PGHOST} || 'localhost';
my $default_port = $ENV{PGPORT} || 5432;
my $default_user = $ENV{PGUSER} || getpwuid($<);

# Command line options
my $host = $default_host;
my $port = $default_port;
my $username = $default_user;
my $verbose = 0;
my $help = 0;

# Parse command line options
GetOptions(
    'host=s' => \$host,
    'port=i' => \$port,
    'username=s' => \$username,
    'verbose|v' => \$verbose,
    'help|h' => \$help,
) or die "Error in command line arguments\n";

# Show help
if ($help) {
    print_help();
    exit 0;
}

# Check required arguments
if (@ARGV != 1) {
    die "Usage: kafsspreload [options] database_name\n" .
        "Use --help for detailed usage information.\n";
}

my $database_name = $ARGV[0];

# Fixed table and column names
my $table_name = 'kafsss_data';
my $column_name = 'seq';

# Global variables
my $dbh;
my $initial_table_hash;
my $initial_system_hash;
my $cache_loaded = 0;
my $exit_flag = 0;

# Signal handlers for graceful shutdown
$SIG{INT} = \&handle_signal;
$SIG{TERM} = \&handle_signal;
$SIG{HUP} = \&handle_signal;

# Main execution
main();

sub main {
    # Connect to database
    connect_to_database();
    
    # Load cache
    load_cache();
    
    # Get initial state
    $initial_table_hash = get_table_hash();
    $initial_system_hash = get_system_table_hash();
    
    print_log("Cache loaded successfully. Starting monitoring loop...");
    
    # Main monitoring loop
    while (!$exit_flag) {
        # Sleep for 1 hour
        for (my $i = 0; $i < 3600 && !$exit_flag; $i++) {
            sleep(1);
        }
        
        last if $exit_flag;
        
        # Check for changes
        if (!check_connection() || has_changes()) {
            print_log("Changes detected or connection lost. Cleaning up and exiting...");
            cleanup_and_exit();
        }
        
        print_log("No changes detected. Continuing monitoring...");
    }
    
    cleanup_and_exit();
}

sub connect_to_database {
    my $dsn = "dbi:Pg:dbname=$database_name;host=$host;port=$port";
    
    print_log("Connecting to database $database_name at $host:$port...");
    
    # Get password from environment or prompt
    my $password = $ENV{PGPASSWORD} || '';
    
    # Try to connect
    $dbh = DBI->connect($dsn, $username, $password, {
        AutoCommit => 1,
        RaiseError => 1,
        PrintError => 0,
    }) or die "Cannot connect to database: $DBI::errstr\n";
    
    print_log("Connected to database successfully");
    
    # Check if pg_kmersearch extension exists
    my $ext_check = $dbh->selectrow_arrayref(
        "SELECT 1 FROM pg_extension WHERE extname = 'pg_kmersearch'"
    );
    
    unless ($ext_check) {
        die "Error: pg_kmersearch extension is not installed in database '$database_name'\n";
    }
    
    # Check if kafsss_data table exists
    my $table_check = $dbh->selectrow_arrayref(
        "SELECT 1 FROM information_schema.tables WHERE table_name = ? AND table_schema = 'public'",
        undef, $table_name
    );
    
    unless ($table_check) {
        die "Error: Table '$table_name' does not exist in database '$database_name'\n";
    }
}

sub load_cache {
    print_log("Loading high-frequency k-mer cache for $table_name.$column_name...");
    
    eval {
        $dbh->do("SELECT kmersearch_parallel_highfreq_kmer_cache_load(?, ?)", 
                 undef, $table_name, $column_name);
        $cache_loaded = 1;
        print_log("Cache loaded successfully");
    };
    
    if ($@) {
        die "Failed to load cache: $@\n";
    }
}

sub free_cache {
    return unless $cache_loaded;
    
    print_log("Freeing high-frequency k-mer cache...");
    
    eval {
        $dbh->do("SELECT kmersearch_parallel_highfreq_kmer_cache_free(?, ?)", 
                 undef, $table_name, $column_name);
        $cache_loaded = 0;
        print_log("Cache freed successfully");
    };
    
    if ($@) {
        warn "Warning: Failed to free cache: $@\n";
    }
}

sub check_connection {
    eval {
        $dbh->ping();
    };
    
    if ($@) {
        print_log("Database connection check failed: $@");
        return 0;
    }
    
    return 1;
}

sub get_table_hash {
    # Get hash of table structure and row count
    my $result = $dbh->selectrow_hashref(
        "SELECT 
            COUNT(*) as row_count,
            pg_relation_size(?) as table_size,
            obj_description(c.oid, 'pg_class') as comment
         FROM kafsss_data, pg_class c
         WHERE c.relname = ?
         GROUP BY c.oid",
        undef, $table_name, $table_name
    );
    
    return generate_hash($result);
}

sub get_system_table_hash {
    # Get hash of pg_kmersearch system tables
    my $result = $dbh->selectall_arrayref(
        "SELECT table_name, 
                (SELECT COUNT(*) FROM information_schema.tables t2 
                 WHERE t2.table_name = t1.table_name) as row_count
         FROM information_schema.tables t1
         WHERE table_schema = 'public' 
           AND table_name LIKE 'kmersearch_%'
         ORDER BY table_name",
        { Slice => {} }
    );
    
    # Also check for high-frequency k-mer related tables
    my $hf_result = $dbh->selectall_arrayref(
        "SELECT COUNT(*) as count FROM kmersearch_highfreq_kmers_dna4
         WHERE table_name = ? AND column_name = ?",
        undef, $table_name, $column_name
    ) if table_exists('kmersearch_highfreq_kmers_dna4');
    
    return generate_hash([$result, $hf_result]);
}

sub table_exists {
    my ($table) = @_;
    
    my $result = $dbh->selectrow_arrayref(
        "SELECT 1 FROM information_schema.tables 
         WHERE table_schema = 'public' AND table_name = ?",
        undef, $table
    );
    
    return defined($result);
}

sub has_changes {
    my $current_table_hash = get_table_hash();
    my $current_system_hash = get_system_table_hash();
    
    if ($current_table_hash ne $initial_table_hash) {
        print_log("Table structure or data has changed");
        return 1;
    }
    
    if ($current_system_hash ne $initial_system_hash) {
        print_log("System tables have changed");
        return 1;
    }
    
    return 0;
}

sub generate_hash {
    my ($data) = @_;
    
    # Simple hash generation using stringified data
    use Digest::MD5 qw(md5_hex);
    
    my $str = '';
    if (ref($data) eq 'HASH') {
        foreach my $key (sort keys %$data) {
            $str .= "$key:" . ($data->{$key} // 'NULL') . ";";
        }
    } elsif (ref($data) eq 'ARRAY') {
        $str = join('|', map { ref($_) ? generate_hash($_) : ($_ // 'NULL') } @$data);
    } else {
        $str = $data // 'NULL';
    }
    
    return md5_hex($str);
}

sub handle_signal {
    my ($signal) = @_;
    print_log("Received signal $signal. Initiating graceful shutdown...");
    $exit_flag = 1;
}

sub cleanup_and_exit {
    # Free cache if loaded
    free_cache() if $cache_loaded;
    
    # Disconnect from database
    if ($dbh) {
        print_log("Disconnecting from database...");
        $dbh->disconnect();
    }
    
    print_log("kafsspreload terminated");
    exit 0;
}

sub print_log {
    my ($message) = @_;
    
    return unless $verbose;
    
    my $timestamp = strftime("%Y-%m-%d %H:%M:%S", localtime);
    print STDERR "[$timestamp] $message\n";
}

sub print_help {
    my $prog = basename($0);
    
    print <<"EOF";
$prog - Preload high-frequency k-mer cache for kafsss

Version: $VERSION

Usage:
  $prog [options] database_name

Description:
  This tool loads high-frequency k-mer information into memory cache using
  pg_kmersearch's kmersearch_parallel_highfreq_kmer_cache_load function.
  It maintains the database connection and monitors for changes hourly.
  When changes are detected, it frees the cache and exits gracefully.
  
  While this daemon is running, kafssindex builds and kafsssearch/kafsssearchserver
  operations will be accelerated due to the preloaded cache.

Options:
  --host <host>        PostgreSQL server host (default: $default_host)
  --port <port>        PostgreSQL server port (default: $default_port)
  --username <user>    PostgreSQL username (default: $default_user)
  --verbose, -v        Enable verbose output
  --help, -h           Show this help message

Environment Variables:
  PGHOST              Default PostgreSQL host
  PGPORT              Default PostgreSQL port
  PGUSER              Default PostgreSQL username
  PGPASSWORD          PostgreSQL password (avoids password prompt)

Signals:
  SIGINT, SIGTERM, SIGHUP  Triggers graceful shutdown with cache cleanup

Notes:
  - Table name is fixed to 'kafsss_data'
  - Column name is fixed to 'seq'
  - Monitoring interval is 1 hour
  - Requires pg_kmersearch extension and kafssfreq to be run first

Examples:
  # Basic usage
  $prog mydb
  
  # With custom connection settings
  $prog --host=dbserver --port=5433 --username=dbuser mydb
  
  # With verbose logging
  $prog --verbose mydb

EOF
}