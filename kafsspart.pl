#!/usr/bin/perl

use strict;
use warnings;
use Getopt::Long;
use DBI;

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
my $tablespace = '';
my $npart = 0;
my $verbose = 0;
my $help = 0;

# Parse command line options
GetOptions(
    'host=s' => \$host,
    'port=i' => \$port,
    'username=s' => \$username,
    'tablespace=s' => \$tablespace,
    'npart=i' => \$npart,
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
    die "Usage: kafsspart [options] database_name\n" .
        "Use --help for detailed usage information.\n";
}

my $database_name = $ARGV[0];

# Validate npart
if ($npart < 2) {
    die "Error: --npart must be 2 or greater\n";
}

# Connect to PostgreSQL database
my $dsn = "dbi:Pg:dbname=$database_name;host=$host;port=$port";
my $dbh = DBI->connect($dsn, $username, '', {
    RaiseError => 1,
    AutoCommit => 1,
    PrintError => 0,
}) or die "Failed to connect to database: $DBI::errstr\n";

# Check if kafsss_data table exists
my $check_table_sql = "SELECT EXISTS (
    SELECT 1 FROM information_schema.tables 
    WHERE table_schema = 'public' 
    AND table_name = 'kafsss_data'
)";
my $table_exists = $dbh->selectrow_array($check_table_sql);

if (!$table_exists) {
    die "Error: Table 'kafsss_data' does not exist in database '$database_name'\n";
}

# Check if table is already partitioned
my $check_partition_sql = "SELECT relkind FROM pg_class WHERE relname = 'kafsss_data'";
my $relkind = $dbh->selectrow_array($check_partition_sql);

if ($relkind eq 'p') {
    die "Error: Table 'kafsss_data' is already partitioned\n";
}

# Check if GIN indexes exist on seq column of kafsss_data table
my $check_gin_index_sql = "
    SELECT i.indexname
    FROM pg_indexes i
    JOIN pg_class c ON c.relname = i.indexname
    JOIN pg_index idx ON idx.indexrelid = c.oid
    JOIN pg_attribute a ON a.attrelid = idx.indrelid AND a.attnum = ANY(idx.indkey)
    WHERE i.tablename = 'kafsss_data'
    AND a.attname = 'seq'
    AND i.indexdef LIKE '%USING gin%'
    LIMIT 1
";
my $gin_index_name = $dbh->selectrow_array($check_gin_index_sql);

if ($gin_index_name) {
    die "Error: GIN index '$gin_index_name' exists on kafsss_data.seq column.\n" .
        "Partitioning cannot proceed with existing GIN indexes.\n" .
        "Please remove GIN indexes first using:\n" .
        "  kafssindex --mode=drop $database_name\n" .
        "After partitioning, recreate indexes with:\n" .
        "  kafssindex --mode=create $database_name\n";
}

# Prepare tablespace parameter (NULL if not specified)
my $tablespace_param = $tablespace ? "'$tablespace'" : 'NULL';

# Execute partitioning
eval {
    print "Partitioning kafsss_data table into $npart partitions...\n" if $verbose;
    
    # Call kmersearch_partition_table function
    my $partition_sql = "SELECT kmersearch_partition_table('kafsss_data', $npart, $tablespace_param)";
    
    $dbh->do($partition_sql);
    
    print "Successfully partitioned kafsss_data table into $npart partitions\n";
    
    # Show partition information if verbose
    if ($verbose) {
        my $partition_info_sql = "
            SELECT 
                schemaname,
                tablename,
                pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
            FROM pg_tables 
            WHERE tablename LIKE 'kafsss_data_%'
            ORDER BY tablename
        ";
        
        my $sth = $dbh->prepare($partition_info_sql);
        $sth->execute();
        
        print "\nPartition Information:\n";
        print "-" x 50 . "\n";
        while (my $row = $sth->fetchrow_hashref) {
            printf "  %-30s %10s\n", $row->{tablename}, $row->{size};
        }
    }
};

if ($@) {
    die "Failed to partition table: $@\n";
}

# Disconnect from database
$dbh->disconnect;

exit 0;

# Function to print help message
sub print_help {
    print <<EOF;
kafsspart version $VERSION

Usage: kafsspart [options] database_name

This tool partitions the kafsss_data table using pg_kmersearch's 
kmersearch_partition_table function for improved performance.

Required arguments:
  database_name         Name of the PostgreSQL database containing kafsss_data table

Required options:
  --npart=INT          Number of partitions (must be 2 or greater)

Optional arguments:
  --host=HOST          Database server host (default: $default_host)
  --port=PORT          Database server port (default: $default_port)
  --username=USER      Database user name (default: $default_user)
  --tablespace=NAME    Tablespace name for partitions (optional)
  --verbose, -v        Enable verbose output
  --help, -h           Show this help message

Environment variables:
  PGHOST               Default database host
  PGPORT               Default database port
  PGUSER               Default database user
  PGPASSWORD           Database password (used automatically by DBI)

Examples:
  # Partition kafsss_data into 16 partitions
  kafsspart --npart=16 mydb

  # Partition with specific tablespace
  kafsspart --npart=32 --tablespace=fast_ssd mydb

  # Partition on remote server
  kafsspart --host=dbserver --port=5433 --npart=8 mydb

Notes:
  - The kafsss_data table must exist before running this command
  - The table cannot be already partitioned
  - Partitioning improves query performance for large datasets
  - The pg_kmersearch extension must be installed

EOF
}