#!/usr/bin/perl

use strict;
use warnings;
use Getopt::Long;
use DBI;
use JSON;
use POSIX qw(strftime);
use Sys::Hostname;
use File::Basename;

# Version number
my $VERSION = "__VERSION__";

# Default values
my $default_host = $ENV{PGHOST} || 'localhost';
my $default_port = $ENV{PGPORT} || 5432;
my $default_user = $ENV{PGUSER} || getpwuid($<);

# Command line options
my $host = $default_host;
my $port = $default_port;
my $username = $default_user;
my $help = 0;

# Parse command line options
GetOptions(
    'host=s' => \$host,
    'port=i' => \$port,
    'username=s' => \$username,
    'help|h' => \$help,
) or die "Error in command line arguments\n";

# Show help
if ($help) {
    print_help();
    exit 0;
}

# Check required arguments
if (@ARGV != 1) {
    die "Usage: kafssdbinfo [options] database_name\n" .
        "Use --help for detailed usage information.\n";
}

my ($database_name) = @ARGV;

print STDERR "kafssdbinfo version $VERSION\n";
print STDERR "Database: $database_name\n";
print STDERR "Host: $host\n";
print STDERR "Port: $port\n";
print STDERR "Username: $username\n";
print STDERR "\n";

# Connect to PostgreSQL server for validation
my $password = $ENV{PGPASSWORD} || '';
my $server_dsn = "DBI:Pg:dbname=postgres;host=$host;port=$port";

my $server_dbh = DBI->connect($server_dsn, $username, $password, {
    AutoCommit => 1,
    PrintError => 0,
    RaiseError => 1,
    ShowErrorStatement => 1,
    AutoInactiveDestroy => 1,
    pg_enable_utf8 => 1
}) or die "Cannot connect to PostgreSQL server: $DBI::errstr\n";

# Validate user and database existence
validate_user_and_permissions($server_dbh, $username);
check_database_exists($server_dbh, $database_name);
$server_dbh->disconnect();

# Connect to target database
my $dsn = "DBI:Pg:dbname=$database_name;host=$host;port=$port";

my $dbh = DBI->connect($dsn, $username, $password, {
    AutoCommit => 1,
    PrintError => 0,
    RaiseError => 1,
    ShowErrorStatement => 1,
    AutoInactiveDestroy => 1,
    pg_enable_utf8 => 1
}) or die "Cannot connect to database '$database_name': $DBI::errstr\n";

# Validate database permissions and schema
validate_database_permissions($dbh, $username);
validate_database_schema($dbh);

# Check if kafsss_meta table exists
my $sth = $dbh->prepare(<<SQL);
SELECT COUNT(*)
FROM information_schema.tables 
WHERE table_name = 'kafsss_meta'
SQL
$sth->execute();
my ($table_count) = $sth->fetchrow_array();
$sth->finish();

unless ($table_count > 0) {
    print STDERR "Error: Table 'kafsss_meta' does not exist in database '$database_name'\n";
    $dbh->disconnect();
    exit 1;
}

# Get metadata from kafsss_meta table
$sth = $dbh->prepare("SELECT ver, minlen, minsplitlen, ovllen, nseq, nchar, subset FROM kafsss_meta LIMIT 1");
$sth->execute();
my ($ver, $minlen, $minsplitlen, $ovllen, $nseq, $nchar, $subset_json) = $sth->fetchrow_array();
$sth->finish();

# Get sequence data type information
my $seq_datatype = 'unknown';
eval {
    my $datatype_sth = $dbh->prepare(<<SQL);
SELECT CASE WHEN data_type = 'USER-DEFINED' THEN udt_name ELSE data_type END AS data_type
FROM information_schema.columns
WHERE table_name = 'kafsss_data' AND column_name = 'seq'
SQL
    $datatype_sth->execute();
    ($seq_datatype) = $datatype_sth->fetchrow_array();
    $datatype_sth->finish();
};

# Get GIN index information from pg_indexes
my @gin_indexes = ();
eval {
    my $idx_sth = $dbh->prepare(<<SQL);
SELECT indexname, indexdef
FROM pg_indexes
WHERE tablename = 'kafsss_data'
  AND indexdef LIKE '%USING gin%'
  AND indexdef LIKE '%seq %'
ORDER BY indexname
SQL
    $idx_sth->execute();
    while (my ($indexname, $indexdef) = $idx_sth->fetchrow_array()) {
        push @gin_indexes, { name => $indexname, definition => $indexdef };
    }
    $idx_sth->finish();
};

# Try to get detailed index info from kmersearch_index_info if available
my @detailed_index_info = ();
eval {
    my $info_sth = $dbh->prepare(<<SQL);
SELECT
    i.indexrelid::regclass AS index_name,
    kii.kmer_size,
    kii.occur_bitlen,
    kii.max_appearance_rate,
    kii.max_appearance_nrow,
    kii.preclude_highfreq_kmer,
    kii.total_nrow,
    kii.highfreq_kmer_count,
    kii.created_at
FROM kmersearch_index_info kii
JOIN pg_index i ON i.indexrelid = kii.index_oid
WHERE kii.table_oid = 'kafsss_data'::regclass
  AND kii.column_name = 'seq'
ORDER BY kii.created_at DESC
SQL
    $info_sth->execute();
    while (my $row = $info_sth->fetchrow_hashref()) {
        push @detailed_index_info, $row;
    }
    $info_sth->finish();
};
# Ignore error if kmersearch_index_info table doesn't exist

$dbh->disconnect();

# Check if data exists
unless (defined $ver) {
    print STDERR "Error: No data found in kafsss_meta table\n";
    exit 1;
}

# Display basic metadata
print STDERR "=== Database Metadata ===\n";
print STDERR "Version: $ver\n";
print STDERR "Sequence data type: " . uc($seq_datatype || 'unknown') . "\n";
print STDERR "Min length: $minlen\n";
print STDERR "Min split length: $minsplitlen\n";
print STDERR "Overlap length: $ovllen\n";
print STDERR "Total sequences: $nseq\n";
print STDERR "Total characters: $nchar\n";

# Display GIN index information
print STDERR "\n=== GIN Index Information ===\n";
if (@detailed_index_info > 0) {
    for my $info (@detailed_index_info) {
        print STDERR "Index: $info->{index_name}\n";
        print STDERR "  K-mer size: $info->{kmer_size}\n";
        print STDERR "  Occurrence bit length: $info->{occur_bitlen}\n";
        print STDERR "  Max appearance rate: $info->{max_appearance_rate}\n";
        print STDERR "  Max appearance nrow: $info->{max_appearance_nrow}\n";
        print STDERR "  High-freq k-mer excluded: " . ($info->{preclude_highfreq_kmer} ? 'yes' : 'no') . "\n";
        print STDERR "  Total rows: $info->{total_nrow}\n" if defined $info->{total_nrow};
        print STDERR "  High-freq k-mer count: $info->{highfreq_kmer_count}\n" if defined $info->{highfreq_kmer_count};
        print STDERR "  Created at: $info->{created_at}\n" if defined $info->{created_at};
        print STDERR "\n";
    }
} elsif (@gin_indexes > 0) {
    # Fall back to basic index information from pg_indexes
    for my $idx (@gin_indexes) {
        print STDERR "Index: $idx->{name}\n";
        # Try to parse index name for parameters if it follows new naming convention
        if ($idx->{name} =~ /idx_kafsss_data_seq_gin_km(\d+)_ob(\d+)_mar(\d{4})_man(\d+)_phk([TF])/) {
            my ($km, $ob, $mar, $man, $phk) = ($1, $2, $3, $4, $5);
            print STDERR "  K-mer size: $km\n";
            print STDERR "  Occurrence bit length: $ob\n";
            print STDERR "  Max appearance rate: " . ($mar / 1000) . "\n";
            print STDERR "  Max appearance nrow: $man\n";
            print STDERR "  High-freq k-mer excluded: " . ($phk eq 'T' ? 'yes' : 'no') . "\n";
        }
        print STDERR "\n";
    }
} else {
    print STDERR "No GIN indexes found on seq column.\n";
}

# Parse and display subset information
print STDERR "\n=== Subset Information ===\n";
if ($subset_json) {
    eval {
        my $subset_data = decode_json($subset_json);
        
        if (keys %$subset_data > 0) {
            print STDERR "Subsets:\n";
            
            # Sort subset names for consistent output
            my @subset_names = sort keys %$subset_data;
            
            for my $subset_name (@subset_names) {
                my $subset_info = $subset_data->{$subset_name};
                my $subset_nseq = $subset_info->{nseq} || 0;
                my $subset_nchar = $subset_info->{nchar} || 0;
                
                print STDERR "  $subset_name:\n";
                print STDERR "    Sequences: $subset_nseq\n";
                print STDERR "    Characters: $subset_nchar\n";
            }
        } else {
            print STDERR "Subsets: none\n";
        }
    };
    
    if ($@) {
        print STDERR "Warning: Failed to parse subset data: $@\n";
        print STDERR "Raw subset data: $subset_json\n";
    }
} else {
    print STDERR "Subsets: none\n";
}

exit 0;

#
# Subroutines
#

sub print_help {
    print <<EOF;
kafssdbinfo version $VERSION

Usage: kafssdbinfo [options] database_name

Display metadata information from kafsss database.

Required arguments:
  database_name     PostgreSQL database name

Options:
  --host=HOST       PostgreSQL server host (default: \$PGHOST or localhost)
  --port=PORT       PostgreSQL server port (default: \$PGPORT or 5432)
  --username=USER   PostgreSQL username (default: \$PGUSER or current user)
  --help, -h        Show this help message

Environment variables:
  PGHOST           PostgreSQL server host
  PGPORT           PostgreSQL server port
  PGUSER           PostgreSQL username
  PGPASSWORD       PostgreSQL password

Output:
  All output is written to STDERR and includes:
  - Database connection information
  - Version, min length, overlap length
  - Total sequences and characters
  - Subset information with sequence and character counts

Examples:
  kafssdbinfo mydb
  kafssdbinfo --host=remote-server mydb
  kafssdbinfo --host=localhost --port=5433 --username=postgres mydb

EOF
}

sub validate_user_and_permissions {
    my ($dbh, $username) = @_;
    
    print STDERR "Validating user '$username' and permissions...\n";
    
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
    
    print STDERR "User validation completed.\n";
}

sub check_database_exists {
    my ($dbh, $dbname) = @_;
    
    my $sth = $dbh->prepare("SELECT 1 FROM pg_database WHERE datname = ?");
    $sth->execute($dbname);
    my $result = $sth->fetchrow_array();
    $sth->finish();
    
    unless ($result) {
        die "Error: Database '$dbname' does not exist.\n" .
            "Please create the database first using kafssstore.\n";
    }
    
    print STDERR "Database '$dbname' exists.\n";
}

sub validate_database_permissions {
    my ($dbh, $username) = @_;
    
    print STDERR "Validating database permissions for '$username'...\n";
    
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
    
    # Check table permissions - kafssdbinfo needs SELECT on both tables
    $sth = $dbh->prepare("SELECT has_table_privilege(?, 'kafsss_meta', 'SELECT')");
    $sth->execute($username);
    my $has_meta_perm = $sth->fetchrow_array();
    $sth->finish();
    
    unless ($has_meta_perm) {
        die "Error: User '$username' does not have SELECT permission on kafsss_meta table.\n" .
            "Please grant permissions:\n" .
            "  sudo -u postgres psql -d " . $dbh->{pg_db} . "\n" .
            "  GRANT SELECT ON kafsss_meta TO $username;\n" .
            "  \\q\n";
    }
    
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
    
    print STDERR "Database permissions validation completed.\n";
}

sub validate_database_schema {
    my ($dbh) = @_;
    
    print STDERR "Validating database schema...\n";
    
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
    
    print STDERR "Database schema validation completed.\n";
}