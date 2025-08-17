#!/usr/bin/perl

use strict;
use warnings;
use Getopt::Long;
use DBI;
use POSIX qw(strftime);
use Sys::Hostname;
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
my $mode = '';
my $kmer_size = $default_kmer_size;
my $max_appearance_rate = $default_max_appearance_rate;
my $max_appearance_nrow = $default_max_appearance_nrow;
my $occur_bitlen = $default_occur_bitlen;
my $numthreads = 0;
my $workingmemory = '8GB';
my $maintenanceworkingmemory = '8GB';
my $temporarybuffer = '512MB';
my $verbose = 0;
my $overwrite = 0;
my $help = 0;

# Parse command line options
GetOptions(
    'host=s' => \$host,
    'port=i' => \$port,
    'username=s' => \$username,
    'mode=s' => \$mode,
    'kmersize=i' => \$kmer_size,
    'maxpappear=f' => \$max_appearance_rate,
    'maxnappear=i' => \$max_appearance_nrow,
    'occurbitlen=i' => \$occur_bitlen,
    'numthreads=i' => \$numthreads,
    'workingmemory=s' => \$workingmemory,
    'maintenanceworkingmemory=s' => \$maintenanceworkingmemory,
    'temporarybuffer=s' => \$temporarybuffer,
    'verbose|v' => \$verbose,
    'overwrite' => \$overwrite,
    'help|h' => \$help,
) or die "Error in command line arguments\n";

# Show help
if ($help) {
    print_help();
    exit 0;
}

# Check required arguments
if (@ARGV != 1) {
    die "Usage: kafssfreq [options] database_name\n" .
        "Use --help for detailed usage information.\n";
}

my ($database_name) = @ARGV;

# Validate required options
die "Mode must be specified with --mode option (create or drop)\n" unless $mode;
die "Invalid mode '$mode'. Must be 'create' or 'drop'\n" unless $mode eq 'create' || $mode eq 'drop';

# Validate parameters
die "kmer_size must be between 4 and 64\n" unless $kmer_size >= 4 && $kmer_size <= 64;
die "max_appearance_rate must be between 0.0 and 1.0\n" unless $max_appearance_rate >= 0.0 && $max_appearance_rate <= 1.0;
die "max_appearance_nrow must be non-negative\n" unless $max_appearance_nrow >= 0;
die "occur_bitlen must be between 0 and 16\n" unless $occur_bitlen >= 0 && $occur_bitlen <= 16;
die "numthreads must be non-negative\n" unless $numthreads >= 0;

print "kafssfreq version $VERSION\n";
print "Database: $database_name\n";
print "Host: $host\n";
print "Port: $port\n";
print "Username: $username\n";
print "Mode: $mode\n";
print "K-mer size: $kmer_size\n";
print "Max appearance rate: $max_appearance_rate\n";
print "Max appearance nrow: $max_appearance_nrow\n";
print "Occur bitlen: $occur_bitlen\n";
print "Num threads: " . ($numthreads ? $numthreads : 'default') . "\n";
print "Working memory: $workingmemory\n";
print "Maintenance working memory: $maintenanceworkingmemory\n";
print "Temporary buffer: $temporarybuffer\n";
print "Overwrite: " . ($overwrite ? 'yes' : 'no') . "\n";

# Connect to PostgreSQL server first for validation
my $password = $ENV{PGPASSWORD} || '';
my $server_dsn = "DBI:Pg:host=$host;port=$port";

my $server_dbh = DBI->connect($server_dsn, $username, $password, {
    RaiseError => 1,
    AutoCommit => 1,
    pg_enable_utf8 => 1
}) or die "Cannot connect to PostgreSQL server: $DBI::errstr\n";

# Validate user existence and permissions
validate_user_and_permissions($server_dbh, $username);

# Check if database exists
unless (check_database_exists($server_dbh, $database_name)) {
    $server_dbh->disconnect();
    die "Error: Database '$database_name' does not exist.\n" .
        "Please create it first using kafssstore.\n";
}

$server_dbh->disconnect();

# Connect to target database
my $dsn = "DBI:Pg:dbname=$database_name;host=$host;port=$port";
my $dbh = DBI->connect($dsn, $username, $password, {
    RaiseError => 1,
    AutoCommit => 1,
    pg_enable_utf8 => 1
}) or die "Cannot connect to database '$database_name': $DBI::errstr\n";

print "Connected to database successfully.\n" if $verbose;

# Validate database permissions and schema
validate_database_permissions($dbh, $username, $mode);
validate_database_schema($dbh);

# Acquire advisory lock for exclusive access to prevent conflicts with other tools
print "Acquiring exclusive lock...\n" if $verbose;
eval {
    $dbh->do("SELECT pg_advisory_xact_lock(999)");
    print "Exclusive lock acquired.\n" if $verbose;
};
if ($@) {
    die "Failed to acquire advisory lock: $@\n";
}

# Verify database structure
verify_database_structure($dbh);

# Detect PostgreSQL version
my $pg_version = get_postgresql_version($dbh);
print "PostgreSQL version: $pg_version\n" if $verbose;

# Set GUC variables
set_guc_variables($dbh);

# Clean up any temporary tables first
cleanup_temporary_tables($dbh);

# Set parallel processing parameters based on numthreads
if ($numthreads > 0) {
    set_parallel_parameters($dbh, $numthreads);
}

# Execute the requested operation
if ($mode eq 'create') {
    # Check if analysis already exists
    if (check_analysis_exists($dbh)) {
        if ($overwrite) {
            print "Existing analysis found. Removing due to --overwrite option...\n";
            undo_highfreq_analysis($dbh);
        } else {
            print "High-frequency k-mer analysis already exists. Use --overwrite to recreate.\n";
            $dbh->disconnect();
            exit 0;
        }
    }
    perform_highfreq_analysis($dbh);
} elsif ($mode eq 'drop') {
    undo_highfreq_analysis($dbh);
}

$dbh->disconnect();

print "Operation completed successfully.\n";

exit 0;

#
# Subroutines
#

sub print_help {
    print <<EOF;
kafssfreq version $VERSION

Usage: kafssfreq [options] database_name

Perform or undo high-frequency k-mer analysis on kafsss_data table.

Required arguments:
  database_name     PostgreSQL database name

Required options:
  --mode=MODE       Operation mode: 'create' or 'drop'

Other options:
  --host=HOST       PostgreSQL server host (default: \$PGHOST or localhost)
  --port=PORT       PostgreSQL server port (default: \$PGPORT or 5432)
  --username=USER   PostgreSQL username (default: \$PGUSER or current user)
  --kmersize=INT    K-mer length for analysis (default: 8, range: 4-64)
  --maxpappear=REAL Max k-mer appearance rate (default: 0.5, range: 0.0-1.0)
  --maxnappear=INT  Max rows containing k-mer (default: 0=unlimited)
  --occurbitlen=INT Bits for occurrence count (default: 8, range: 0-16)
  --numthreads=INT  Number of parallel workers (default: 0=auto)
  --workingmemory=SIZE  Work memory for each operation (default: 8GB)
  --maintenanceworkingmemory=SIZE  Maintenance work memory (default: 8GB)
  --temporarybuffer=SIZE  Temporary buffer size (default: 512MB)
  --verbose, -v     Show detailed processing messages (default: false)
  --overwrite       Overwrite existing analysis (only for --mode=create)
  --help, -h        Show this help message

Environment variables:
  PGHOST           PostgreSQL server host
  PGPORT           PostgreSQL server port
  PGUSER           PostgreSQL username
  PGPASSWORD       PostgreSQL password

Examples:
  kafssfreq --mode=create mydb
  kafssfreq --mode=drop mydb
  kafssfreq --mode=create --kmersize=16 --numthreads=32 mydb
  kafssfreq --mode=create --maxpappear=0.3 --maxnappear=500 mydb
  kafssfreq --mode=create --overwrite --numthreads=32 mydb

EOF
}

sub verify_database_structure {
    my ($dbh) = @_;
    
    print "Verifying database structure...\n";
    
    # Check if pg_kmersearch extension exists
    my $sth = $dbh->prepare("SELECT 1 FROM pg_extension WHERE extname = 'pg_kmersearch'");
    $sth->execute();
    my $ext_exists = $sth->fetchrow_array();
    $sth->finish();
    
    die "pg_kmersearch extension is not installed in database '$database_name'\n" 
        unless $ext_exists;
    
    # Check if kafsss_data table exists
    $sth = $dbh->prepare(<<SQL);
SELECT COUNT(*)
FROM information_schema.tables 
WHERE table_name = 'kafsss_data'
SQL
    $sth->execute();
    my ($table_count) = $sth->fetchrow_array();
    $sth->finish();
    
    die "Table 'kafsss_data' does not exist in database '$database_name'\n" 
        unless $table_count > 0;
    
    # Check if required columns exist with correct types
    $sth = $dbh->prepare(<<SQL);
SELECT column_name, CASE WHEN data_type = 'USER-DEFINED' THEN udt_name ELSE data_type END AS data_type
FROM information_schema.columns 
WHERE table_name = 'kafsss_data'
AND column_name IN ('seq', 'subset', 'seqid')
ORDER BY column_name
SQL
    $sth->execute();
    
    my %columns = ();
    while (my ($col, $type) = $sth->fetchrow_array()) {
        $columns{$col} = $type;
    }
    $sth->finish();
    
    die "Required columns not found in table 'kafsss_data'\n"
        unless exists $columns{seq} && exists $columns{subset} && exists $columns{seqid};
    
    die "Column 'subset' must be ARRAY type\n" unless $columns{subset} eq 'ARRAY';
    die "Column 'seqid' must be ARRAY type\n" unless $columns{seqid} eq 'ARRAY';
    die "Column 'seq' must be DNA2 or DNA4 type\n" 
        unless lc($columns{seq}) eq 'dna2' || lc($columns{seq}) eq 'dna4';
    
    print "Database structure verified.\n";
}

sub get_postgresql_version {
    my ($dbh) = @_;
    
    my $version;
    my $sth = $dbh->prepare("SELECT version()");
    eval {
        $sth->execute();
        my ($version_string) = $sth->fetchrow_array();
        $sth->finish();
        
        # Extract major version number (e.g., "PostgreSQL 16.1" -> 16)
        if ($version_string =~ /PostgreSQL (\d+)\./) {
            $version = $1;
        } elsif ($version_string =~ /PostgreSQL (\d+)/) {
            $version = $1;
        } else {
            die "Could not parse PostgreSQL version: $version_string\n";
        }
    };
    
    if ($@) {
        die "Failed to get PostgreSQL version: $@\n";
    }
    
    return $version;
}

sub set_guc_variables {
    my ($dbh) = @_;
    
    print "Setting GUC variables...\n";
    
    # Set kmer_size
    eval {
        $dbh->do("SET kmersearch.kmer_size = $kmer_size");
        print "Set kmersearch.kmer_size = $kmer_size\n";
    };
    if ($@) {
        die "Failed to set kmersearch.kmer_size: $@\n";
    }
    
    # Set occur_bitlen
    eval {
        $dbh->do("SET kmersearch.occur_bitlen = $occur_bitlen");
        print "Set kmersearch.occur_bitlen = $occur_bitlen\n";
    };
    if ($@) {
        die "Failed to set kmersearch.occur_bitlen: $@\n";
    }
    
    # Set max_appearance_rate
    eval {
        $dbh->do("SET kmersearch.max_appearance_rate = $max_appearance_rate");
        print "Set kmersearch.max_appearance_rate = $max_appearance_rate\n";
    };
    if ($@) {
        die "Failed to set kmersearch.max_appearance_rate: $@\n";
    }
    
    # Set max_appearance_nrow
    eval {
        $dbh->do("SET kmersearch.max_appearance_nrow = $max_appearance_nrow");
        print "Set kmersearch.max_appearance_nrow = $max_appearance_nrow\n";
    };
    if ($@) {
        die "Failed to set kmersearch.max_appearance_nrow: $@\n";
    }
    
    print "GUC variables set successfully.\n";
}

sub set_parallel_parameters {
    my ($dbh, $threads) = @_;
    
    print "Setting parallel processing parameters...\n";
    
    # Set max_parallel_workers_per_gather
    eval {
        $dbh->do("SET max_parallel_workers_per_gather = $threads");
        print "Set max_parallel_workers_per_gather = $threads\n";
    };
    if ($@) {
        die "Failed to set max_parallel_workers_per_gather: $@\n";
    }
    
    # Set max_parallel_workers
    eval {
        $dbh->do("SET max_parallel_workers = $threads");
        print "Set max_parallel_workers = $threads\n";
    };
    if ($@) {
        die "Failed to set max_parallel_workers: $@\n";
    }
    
    
    # Set max_parallel_maintenance_workers
    eval {
        $dbh->do("SET max_parallel_maintenance_workers = $threads");
        print "Set max_parallel_maintenance_workers = $threads\n";
    };
    if ($@) {
        die "Failed to set max_parallel_maintenance_workers: $@\n";
    }
    
    print "Parallel processing parameters set successfully.\n";
}

sub perform_highfreq_analysis {
    my ($dbh) = @_;
    
    print "Performing high-frequency k-mer analysis...\n";
    
    eval {
        my $sth = $dbh->prepare("SELECT kmersearch_perform_highfreq_analysis('kafsss_data', 'seq')");
        $sth->execute();
        my ($result) = $sth->fetchrow_array();
        $sth->finish();
        
        print "High-frequency k-mer analysis completed: $result\n";
    };
    
    if ($@) {
        die "Failed to perform high-frequency k-mer analysis: $@\n";
    }
}

sub undo_highfreq_analysis {
    my ($dbh) = @_;
    
    print "Undoing high-frequency k-mer analysis...\n";
    
    eval {
        my $sth = $dbh->prepare("SELECT kmersearch_undo_highfreq_analysis('kafsss_data', 'seq')");
        $sth->execute();
        my ($result) = $sth->fetchrow_array();
        $sth->finish();
        
        print "High-frequency k-mer analysis undone: $result\n";
    };
    
    if ($@) {
        die "Failed to undo high-frequency k-mer analysis: $@\n";
    }
}

sub validate_user_and_permissions {
    my ($dbh, $username) = @_;
    
    print "Validating user '$username' and permissions...\n";
    
    # Check if user exists
    my $sth = $dbh->prepare("SELECT 1 FROM pg_user WHERE usename = ?");
    $sth->execute($username);
    my $user_exists = $sth->fetchrow_array();
    $sth->finish();
    
    unless ($user_exists) {
        die "Error: PostgreSQL user '$username' does not exist.\n" .
            "Please create the user first:\n" .
            "  sudo -u postgres psql\n" .
            "  CREATE USER $username;\n" .
            "  \\q\n";
    }
    
    print "User validation completed.\n";
}

sub check_database_exists {
    my ($dbh, $dbname) = @_;
    
    my $sth = $dbh->prepare("SELECT 1 FROM pg_database WHERE datname = ?");
    $sth->execute($dbname);
    my $result = $sth->fetchrow_array();
    $sth->finish();
    
    return defined $result;
}

sub validate_database_permissions {
    my ($dbh, $username, $mode) = @_;
    
    print "Validating database permissions for '$username'...\n";
    
    # Check if pg_kmersearch extension exists
    my $sth = $dbh->prepare("SELECT 1 FROM pg_extension WHERE extname = 'pg_kmersearch'");
    $sth->execute();
    my $ext_exists = $sth->fetchrow_array();
    $sth->finish();
    
    unless ($ext_exists) {
        die "Error: Extension 'pg_kmersearch' is not installed in this database.\n" .
            "Please install it first:\n" .
            "  sudo -u postgres psql -d " . $dbh->{pg_db} . "\n" .
            "  CREATE EXTENSION IF NOT EXISTS pg_kmersearch;\n" .
            "  \\q\n";
    }
    
    # Check table permissions based on mode
    if ($mode eq 'create') {
        # Check if user can create/modify high-frequency analysis tables
        $sth = $dbh->prepare("SELECT has_table_privilege(?, 'kafsss_data', 'SELECT, INSERT, UPDATE, DELETE')");
        $sth->execute($username);
        my $has_table_perm = $sth->fetchrow_array();
        $sth->finish();
        
        unless ($has_table_perm) {
            die "Error: User '$username' does not have sufficient permissions on kafsss_data table.\n" .
                "Please grant permissions:\n" .
                "  sudo -u postgres psql -d " . $dbh->{pg_db} . "\n" .
                "  GRANT SELECT, INSERT, UPDATE, DELETE ON kafsss_data TO $username;\n" .
                "  \\q\n";
        }
    } elsif ($mode eq 'drop') {
        # Check if user can drop high-frequency analysis tables
        $sth = $dbh->prepare("SELECT has_table_privilege(?, 'kafsss_data', 'SELECT')");
        $sth->execute($username);
        my $has_table_perm = $sth->fetchrow_array();
        $sth->finish();
        
        unless ($has_table_perm) {
            die "Error: User '$username' does not have SELECT permission on kafsss_data table.\n" .
                "Please grant permissions:\n" .
                "  sudo -u postgres psql -d " . $dbh->{pg_db} . "\n" .
                "  GRANT SELECT ON kafsss_data TO $username;\n" .
                "  \\q\n";
        }
    }
    
    print "Database permissions validated.\n";
}

sub cleanup_temporary_tables {
    my ($dbh) = @_;
    
    print "Cleaning up temporary tables...\n" if $verbose;
    
    # Find and drop temporary tables with specific prefixes
    my @temp_prefixes = ('temp_kmer_worker_', 'temp_kmer_final_');
    
    for my $prefix (@temp_prefixes) {
        my $sth = $dbh->prepare(<<SQL);
SELECT schemaname, tablename 
FROM pg_tables 
WHERE tablename LIKE ? AND schemaname LIKE 'pg_temp_%'
SQL
        $sth->execute("${prefix}%");
        
        while (my ($schema, $table) = $sth->fetchrow_array()) {
            print "Dropping temporary table: $schema.$table\n" if $verbose;
            eval {
                $dbh->do("DROP TABLE IF EXISTS \"$schema\".\"$table\"");
            };
            if ($@) {
                print "Warning: Failed to drop temporary table $schema.$table: $@\n";
            }
        }
        $sth->finish();
    }
    
    print "Temporary table cleanup completed.\n" if $verbose;
}

sub check_analysis_exists {
    my ($dbh) = @_;
    
    # Check if kmersearch_highfreq_kmer table exists and has data for kafsss_data
    my $sth = $dbh->prepare(<<SQL);
SELECT COUNT(*) 
FROM information_schema.tables 
WHERE table_name = 'kmersearch_highfreq_kmer'
SQL
    $sth->execute();
    my ($table_exists) = $sth->fetchrow_array();
    $sth->finish();
    
    if ($table_exists > 0) {
        # Check if there are records for kafsss_data table
        $sth = $dbh->prepare("SELECT COUNT(*) FROM kmersearch_highfreq_kmer WHERE table_oid = 'kafsss_data'::regclass AND column_name = 'seq'");
        $sth->execute();
        my ($record_count) = $sth->fetchrow_array();
        $sth->finish();
        
        return $record_count > 0;
    }
    
    # Check if kmersearch_highfreq_kmer_meta table exists and has data for kafsss_data
    $sth = $dbh->prepare(<<SQL);
SELECT COUNT(*) 
FROM information_schema.tables 
WHERE table_name = 'kmersearch_highfreq_kmer_meta'
SQL
    $sth->execute();
    ($table_exists) = $sth->fetchrow_array();
    $sth->finish();
    
    if ($table_exists > 0) {
        # Check if there are records for kafsss_data table
        $sth = $dbh->prepare("SELECT COUNT(*) FROM kmersearch_highfreq_kmer_meta WHERE table_oid = 'kafsss_data'::regclass AND column_name = 'seq'");
        $sth->execute();
        my ($record_count) = $sth->fetchrow_array();
        $sth->finish();
        
        return $record_count > 0;
    }
    
    return 0;
}

sub validate_database_schema {
    my ($dbh) = @_;
    
    print "Validating database schema...\n";
    
    # Check if required tables exist
    my @required_tables = ('kafsss_meta', 'kafsss_data');
    
    for my $table (@required_tables) {
        my $sth = $dbh->prepare("SELECT 1 FROM information_schema.tables WHERE table_name = ?");
        $sth->execute($table);
        my $table_exists = $sth->fetchrow_array();
        $sth->finish();
        
        unless ($table_exists) {
            die "Error: Required table '$table' does not exist in database.\n" .
                "This database may not have been created with kafssstore.\n" .
                "Please create the database properly using kafssstore first.\n";
        }
    }
    
    print "Database schema validation completed.\n";
}