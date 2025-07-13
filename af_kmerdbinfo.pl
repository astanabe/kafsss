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
my $VERSION = "1.0.0";

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
    die "Usage: af_kmerdbinfo [options] database_name\n" .
        "Use --help for detailed usage information.\n";
}

my ($database_name) = @ARGV;

print STDERR "af_kmerdbinfo version $VERSION\n";
print STDERR "Database: $database_name\n";
print STDERR "Host: $host\n";
print STDERR "Port: $port\n";
print STDERR "Username: $username\n";
print STDERR "\n";

# Connect to PostgreSQL database
my $password = $ENV{PGPASSWORD} || '';
my $dsn = "DBI:Pg:dbname=$database_name;host=$host;port=$port";

my $dbh = DBI->connect($dsn, $username, $password, {
    RaiseError => 1,
    AutoCommit => 1,
    pg_enable_utf8 => 1
}) or die "Cannot connect to database '$database_name': $DBI::errstr\n";

# Check if af_kmersearch_meta table exists
my $sth = $dbh->prepare(<<SQL);
SELECT COUNT(*)
FROM information_schema.tables 
WHERE table_name = 'af_kmersearch_meta'
SQL
$sth->execute();
my ($table_count) = $sth->fetchrow_array();
$sth->finish();

unless ($table_count > 0) {
    print STDERR "Error: Table 'af_kmersearch_meta' does not exist in database '$database_name'\n";
    $dbh->disconnect();
    exit 1;
}

# Get metadata from af_kmersearch_meta table (including new kmer-related columns)
$sth = $dbh->prepare("SELECT ver, minlen, minsplitlen, ovllen, nseq, nchar, part, kmer_size, occur_bitlen, max_appearance_rate, max_appearance_nrow FROM af_kmersearch_meta LIMIT 1");
$sth->execute();
my ($ver, $minlen, $minsplitlen, $ovllen, $nseq, $nchar, $part_json, $kmer_size, $occur_bitlen, $max_appearance_rate, $max_appearance_nrow) = $sth->fetchrow_array();
$sth->finish();

# Get sequence data type information
my $seq_datatype = 'unknown';
eval {
    my $datatype_sth = $dbh->prepare(<<SQL);
SELECT data_type FROM information_schema.columns 
WHERE table_name = 'af_kmersearch' AND column_name = 'seq'
SQL
    $datatype_sth->execute();
    ($seq_datatype) = $datatype_sth->fetchrow_array();
    $datatype_sth->finish();
};

$dbh->disconnect();

# Check if data exists
unless (defined $ver) {
    print STDERR "Error: No data found in af_kmersearch_meta table\n";
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

# Display K-mer index configuration if available
if (defined $kmer_size) {
    print STDERR "\n=== K-mer Index Configuration ===\n";
    print STDERR "K-mer size: $kmer_size\n";
    print STDERR "Occurrence bit length: $occur_bitlen\n";
    print STDERR "Max appearance rate: $max_appearance_rate\n";
    print STDERR "Max appearance nrow: $max_appearance_nrow\n";
} else {
    print STDERR "\n=== K-mer Index Configuration ===\n";
    print STDERR "K-mer index: Not configured\n";
}

# Parse and display partition information
print STDERR "\n=== Partition Information ===\n";
if ($part_json) {
    eval {
        my $part_data = decode_json($part_json);
        
        if (keys %$part_data > 0) {
            print STDERR "Partitions:\n";
            
            # Sort partition names for consistent output
            my @partition_names = sort keys %$part_data;
            
            for my $partition_name (@partition_names) {
                my $partition_info = $part_data->{$partition_name};
                my $partition_nseq = $partition_info->{nseq} || 0;
                my $partition_nchar = $partition_info->{nchar} || 0;
                
                print STDERR "  $partition_name:\n";
                print STDERR "    Sequences: $partition_nseq\n";
                print STDERR "    Characters: $partition_nchar\n";
            }
        } else {
            print STDERR "Partitions: none\n";
        }
    };
    
    if ($@) {
        print STDERR "Warning: Failed to parse partition data: $@\n";
        print STDERR "Raw partition data: $part_json\n";
    }
} else {
    print STDERR "Partitions: none\n";
}

exit 0;

#
# Subroutines
#

sub print_help {
    print <<EOF;
af_kmerdbinfo version $VERSION

Usage: af_kmerdbinfo [options] database_name

Display metadata information from af_kmersearch database.

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
  - Partition information with sequence and character counts

Examples:
  af_kmerdbinfo mydb
  af_kmerdbinfo --host=remote-server mydb
  af_kmerdbinfo --host=localhost --port=5433 --username=postgres mydb

EOF
}