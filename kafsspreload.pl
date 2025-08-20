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
my $default_kmer_size = 8;
my $default_max_appearance_rate = 0.5;
my $default_max_appearance_nrow = 0;
my $default_occur_bitlen = 8;

# Command line options
my $host = $default_host;
my $port = $default_port;
my $username = $default_user;
my $kmer_size = undef;
my $max_appearance_rate = undef;
my $max_appearance_nrow = undef;
my $occur_bitlen = undef;
my $verbose = 0;
my $help = 0;

# Parse command line options
GetOptions(
    'host=s' => \$host,
    'port=i' => \$port,
    'username=s' => \$username,
    'kmersize=i' => \$kmer_size,
    'maxpappear=f' => \$max_appearance_rate,
    'maxnappear=i' => \$max_appearance_nrow,
    'occurbitlen=i' => \$occur_bitlen,
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
    
    # Load and validate parameters from kmersearch_highfreq_kmer
    load_and_validate_parameters();
    
    # Set GUC variables
    set_guc_variables();
    
    # Load cache
    load_cache();
    
    # Get initial state
    $initial_table_hash = get_table_hash();
    $initial_system_hash = get_system_table_hash();
    
    print_log("Cache loaded successfully.");
    print "\n";
    print "High-frequency k-mer cache is now active and accessible to other processes.\n";
    print "The cache will remain available as long as this program is running.\n";
    print "\n";
    
    # User interaction loop
    while (!$exit_flag) {
        print "Press 'exit' or 'q' to quit, or 'status' to check cache status: ";
        
        # Read user input
        my $input = <STDIN>;
        
        # Handle EOF (Ctrl+D)
        if (!defined($input)) {
            print "\nReceived EOF. Initiating graceful shutdown...\n";
            last;
        }
        
        chomp($input);
        $input = lc($input);  # Convert to lowercase for case-insensitive comparison
        
        # Process commands
        if ($input eq 'exit' || $input eq 'q' || $input eq 'quit') {
            print "Initiating graceful shutdown...\n";
            last;
        } elsif ($input eq 'status' || $input eq 's') {
            # Check status
            eval {
                print_status();
            };
            if ($@) {
                print "Error while checking status: $@\n";
            }
        } elsif ($input eq 'reconnect' || $input eq 'r') {
            # Try to reconnect to database
            print "Attempting to reconnect to database...\n";
            eval {
                $dbh->disconnect() if $dbh;
                connect_to_database();
                print "Reconnection successful.\n";
            };
            if ($@) {
                print "Reconnection failed: $@\n";
                print "The cache remains in memory but database connection is lost.\n";
            }
        } elsif ($input eq 'help' || $input eq 'h' || $input eq '?') {
            print "\nAvailable commands:\n";
            print "  exit, q, quit   - Exit the program and free the cache\n";
            print "  status, s       - Show cache and connection status\n";
            print "  reconnect, r    - Attempt to reconnect to database\n";
            print "  help, h, ?      - Show this help message\n";
            print "\n";
        } elsif ($input eq '') {
            # Empty input, just continue
            continue;
        } else {
            print "Unknown command: '$input'. Type 'help' for available commands.\n";
        }
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
    print_log("Parameters: kmer_size=$kmer_size, occur_bitlen=$occur_bitlen, max_appearance_rate=$max_appearance_rate, max_appearance_nrow=$max_appearance_nrow");
    
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

sub print_status {
    print "\n=== Cache Status ===\n";
    
    # Check database connection
    my $connection_ok = 0;
    eval {
        $connection_ok = check_connection();
    };
    if ($@) {
        print "Database connection: ERROR - $@\n";
        $connection_ok = 0;
    } elsif ($connection_ok) {
        print "Database connection: ACTIVE\n";
    } else {
        print "Database connection: LOST\n";
    }
    
    # Check cache status
    if ($cache_loaded) {
        print "Cache status: LOADED\n";
        print "Table: $table_name\n";
        print "Column: $column_name\n";
        print "Parameters:\n";
        print "  kmer_size: $kmer_size\n";
        print "  occur_bitlen: $occur_bitlen\n";
        print "  max_appearance_rate: $max_appearance_rate\n";
        print "  max_appearance_nrow: $max_appearance_nrow\n";
        
        # Only check for changes if connection is active
        if ($connection_ok) {
            eval {
                if (has_changes()) {
                    print "\nWARNING: Table or system tables have changed since cache was loaded!\n";
                    print "The cache may be outdated. Consider restarting the program.\n";
                } else {
                    print "\nNo changes detected since cache was loaded.\n";
                }
            };
            if ($@) {
                print "\nWARNING: Could not check for changes: $@\n";
            }
        } else {
            print "\nWARNING: Cannot check for changes - database connection lost.\n";
            print "The cache remains in memory but may be outdated.\n";
        }
    } else {
        print "Cache status: NOT LOADED\n";
    }
    
    print "==================\n\n";
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

sub load_and_validate_parameters {
    print_log("Checking for parameters in kmersearch_highfreq_kmer table...");
    
    # Track which parameters were specified on command line
    my $kmer_size_specified = defined($kmer_size);
    my $max_appearance_rate_specified = defined($max_appearance_rate);
    my $max_appearance_nrow_specified = defined($max_appearance_nrow);
    my $occur_bitlen_specified = defined($occur_bitlen);
    
    # Try to get parameters from kmersearch_highfreq_kmer_meta table
    my $sth = $dbh->prepare(<<SQL);
SELECT DISTINCT kmer_size, occur_bitlen, max_appearance_rate, max_appearance_nrow
FROM kmersearch_highfreq_kmer_meta
WHERE table_oid = ?::regclass
  AND column_name = ?
SQL
    
    eval {
        $sth->execute($table_name, $column_name);
        my @rows = ();
        while (my $row = $sth->fetchrow_hashref()) {
            push @rows, $row;
        }
        $sth->finish();
        
        if (@rows == 0) {
            # No data in kmersearch_highfreq_kmer_meta table
            die "Error: No high-frequency k-mer data found in kmersearch_highfreq_kmer_meta table for $table_name.$column_name.\n" .
                "Please run kafssfreq first to generate high-frequency k-mer data.\n";
        } elsif (@rows == 1) {
            # Found exactly one set of parameters
            my $row = $rows[0];
            my $db_kmer_size = $row->{kmer_size};
            my $db_occur_bitlen = $row->{occur_bitlen};
            my $db_max_appearance_rate = $row->{max_appearance_rate};
            my $db_max_appearance_nrow = $row->{max_appearance_nrow};
            
            print_log("Found parameters in kmersearch_highfreq_kmer_meta table:");
            print_log("  kmer_size: $db_kmer_size");
            print_log("  occur_bitlen: $db_occur_bitlen");
            print_log("  max_appearance_rate: $db_max_appearance_rate");
            print_log("  max_appearance_nrow: $db_max_appearance_nrow");
            
            # Validate or use database values
            if ($kmer_size_specified) {
                if ($kmer_size != $db_kmer_size) {
                    die "Error: Specified kmer_size ($kmer_size) does not match value in kmersearch_highfreq_kmer_meta table ($db_kmer_size).\n" .
                        "Please use --kmersize=$db_kmer_size or run kafssfreq again with --kmersize=$kmer_size.\n";
                }
            } else {
                $kmer_size = $db_kmer_size;
                print_log("Using kmer_size from database: $kmer_size");
            }
            
            if ($occur_bitlen_specified) {
                if ($occur_bitlen != $db_occur_bitlen) {
                    die "Error: Specified occur_bitlen ($occur_bitlen) does not match value in kmersearch_highfreq_kmer_meta table ($db_occur_bitlen).\n" .
                        "Please use --occurbitlen=$db_occur_bitlen or run kafssfreq again with --occurbitlen=$occur_bitlen.\n";
                }
            } else {
                $occur_bitlen = $db_occur_bitlen;
                print_log("Using occur_bitlen from database: $occur_bitlen");
            }
            
            if ($max_appearance_rate_specified) {
                if (abs($max_appearance_rate - $db_max_appearance_rate) > 0.0001) {
                    die "Error: Specified max_appearance_rate ($max_appearance_rate) does not match value in kmersearch_highfreq_kmer_meta table ($db_max_appearance_rate).\n" .
                        "Please use --maxpappear=$db_max_appearance_rate or run kafssfreq again with --maxpappear=$max_appearance_rate.\n";
                }
            } else {
                $max_appearance_rate = $db_max_appearance_rate;
                print_log("Using max_appearance_rate from database: $max_appearance_rate");
            }
            
            if ($max_appearance_nrow_specified) {
                if ($max_appearance_nrow != $db_max_appearance_nrow) {
                    die "Error: Specified max_appearance_nrow ($max_appearance_nrow) does not match value in kmersearch_highfreq_kmer_meta table ($db_max_appearance_nrow).\n" .
                        "Please use --maxnappear=$db_max_appearance_nrow or run kafssfreq again with --maxnappear=$max_appearance_nrow.\n";
                }
            } else {
                $max_appearance_nrow = $db_max_appearance_nrow;
                print_log("Using max_appearance_nrow from database: $max_appearance_nrow");
            }
        } else {
            # Multiple different parameter sets found
            die "Error: Multiple different parameter sets found in kmersearch_highfreq_kmer_meta table.\n" .
                "This indicates inconsistent frequency analysis. Please run kafssfreq again to fix this.\n";
        }
    };
    
    if ($@) {
        die "Failed to load parameters from kmersearch_highfreq_kmer_meta table: $@";
    }
    
    # Final validation of parameters
    die "kmersize must be between 4 and 64\n" unless $kmer_size >= 4 && $kmer_size <= 64;
    die "maxpappear must be between 0.0 and 1.0\n" unless $max_appearance_rate >= 0.0 && $max_appearance_rate <= 1.0;
    die "maxnappear must be non-negative\n" unless $max_appearance_nrow >= 0;
    die "occurbitlen must be between 0 and 16\n" unless $occur_bitlen >= 0 && $occur_bitlen <= 16;
}

sub set_guc_variables {
    print_log("Setting GUC variables...");
    
    # Set kmer_size
    eval {
        $dbh->do("SET kmersearch.kmer_size = $kmer_size");
        print_log("Set kmersearch.kmer_size = $kmer_size");
    };
    if ($@) {
        die "Failed to set kmersearch.kmer_size: $@\n";
    }
    
    # Set occur_bitlen
    eval {
        $dbh->do("SET kmersearch.occur_bitlen = $occur_bitlen");
        print_log("Set kmersearch.occur_bitlen = $occur_bitlen");
    };
    if ($@) {
        die "Failed to set kmersearch.occur_bitlen: $@\n";
    }
    
    # Set max_appearance_rate
    eval {
        $dbh->do("SET kmersearch.max_appearance_rate = $max_appearance_rate");
        print_log("Set kmersearch.max_appearance_rate = $max_appearance_rate");
    };
    if ($@) {
        die "Failed to set kmersearch.max_appearance_rate: $@\n";
    }
    
    # Set max_appearance_nrow
    eval {
        $dbh->do("SET kmersearch.max_appearance_nrow = $max_appearance_nrow");
        print_log("Set kmersearch.max_appearance_nrow = $max_appearance_nrow");
    };
    if ($@) {
        die "Failed to set kmersearch.max_appearance_nrow: $@\n";
    }
    
    # Set preclude_highfreq_kmer and force_use_parallel_highfreq_kmer_cache
    eval {
        $dbh->do("SET kmersearch.preclude_highfreq_kmer = true");
        print_log("Set kmersearch.preclude_highfreq_kmer = true");
    };
    if ($@) {
        die "Failed to set kmersearch.preclude_highfreq_kmer: $@\n";
    }
    
    eval {
        $dbh->do("SET kmersearch.force_use_parallel_highfreq_kmer_cache = true");
        print_log("Set kmersearch.force_use_parallel_highfreq_kmer_cache = true");
    };
    if ($@) {
        die "Failed to set kmersearch.force_use_parallel_highfreq_kmer_cache: $@\n";
    }
    
    print_log("GUC variables set successfully.");
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
  It maintains the database connection and keeps the cache available for
  other PostgreSQL processes.
  
  While this program is running, kafssindex builds and kafsssearch/kafsssearchserver
  operations will be accelerated due to the preloaded cache.
  
  The program runs interactively and accepts the following commands:
    exit, q, quit   - Exit the program and free the cache
    status, s       - Show cache and connection status
    reconnect, r    - Attempt to reconnect to database
    help, h, ?      - Show help message

Options:
  --host <host>        PostgreSQL server host (default: $default_host)
  --port <port>        PostgreSQL server port (default: $default_port)
  --username <user>    PostgreSQL username (default: $default_user)
  --kmersize <int>     K-mer length (default: from kmersearch_highfreq_kmer table or $default_kmer_size)
  --maxpappear <real>  Max k-mer appearance rate (default: from table or $default_max_appearance_rate)
  --maxnappear <int>   Max rows containing k-mer (default: from table or $default_max_appearance_nrow)
  --occurbitlen <int>  Bits for occurrence count (default: from table or $default_occur_bitlen)
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
  - The cache remains accessible to other PostgreSQL processes while this program runs
  - Interactive mode allows checking status and graceful shutdown
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