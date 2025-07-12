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

# Command line options
my $host = $default_host;
my $port = $default_port;
my $username = $default_user;
my $tablespace = '';
my $mode = '';
my $kmer_size = $default_kmer_size;
my $workingmemory = '';
my $help = 0;

# Parse command line options
GetOptions(
    'host=s' => \$host,
    'port=i' => \$port,
    'username=s' => \$username,
    'tablespace=s' => \$tablespace,
    'mode=s' => \$mode,
    'kmer_size=i' => \$kmer_size,
    'workingmemory=s' => \$workingmemory,
    'help|h' => \$help,
) or die "Error in command line arguments\n";

# Show help
if ($help) {
    print_help();
    exit 0;
}

# Check required arguments
if (@ARGV != 1) {
    die "Usage: perl af_kmerindex.pl [options] database_name\n" .
        "Use --help for detailed usage information.\n";
}

my ($database_name) = @ARGV;

# Validate required options
die "Mode must be specified with --mode option (create or drop)\n" unless $mode;
die "Invalid mode '$mode'. Must be 'create' or 'drop'\n" unless $mode eq 'create' || $mode eq 'drop';

# Validate kmer_size
die "kmer_size must be between 4 and 64\n" unless $kmer_size >= 4 && $kmer_size <= 64;

print "af_kmerindex.pl version $VERSION\n";
print "Database: $database_name\n";
print "Host: $host\n";
print "Port: $port\n";
print "Username: $username\n";
print "Tablespace: " . ($tablespace ? $tablespace : 'default') . "\n";
print "Mode: $mode\n";
print "K-mer size: $kmer_size\n";
print "Working memory: " . ($workingmemory ? $workingmemory : 'default') . "\n";

# Connect to PostgreSQL database
my $password = $ENV{PGPASSWORD} || '';
my $dsn = "DBI:Pg:dbname=$database_name;host=$host;port=$port";

my $dbh = DBI->connect($dsn, $username, $password, {
    RaiseError => 1,
    AutoCommit => 1,
    pg_enable_utf8 => 1
}) or die "Cannot connect to database '$database_name': $DBI::errstr\n";

print "Connected to database successfully.\n";

# Validate tablespace if specified and mode is 'create'
if ($tablespace && $mode eq 'create') {
    validate_tablespace_exists($dbh, $tablespace);
}

# Acquire advisory lock for exclusive access to prevent conflicts with other tools
print "Acquiring exclusive lock...\n";
eval {
    $dbh->do("SELECT pg_advisory_xact_lock(999)");
    print "Exclusive lock acquired.\n";
};
if ($@) {
    die "Failed to acquire advisory lock: $@\n";
}

# Verify database structure
verify_database_structure($dbh);

# Execute the requested operation
if ($mode eq 'create') {
    create_indexes($dbh);
} elsif ($mode eq 'drop') {
    drop_indexes($dbh);
}

$dbh->disconnect();

print "Operation completed successfully.\n";

exit 0;

#
# Subroutines
#

sub print_help {
    print <<EOF;
af_kmerindex.pl version $VERSION

Usage: perl af_kmerindex.pl [options] database_name

Create or drop GIN indexes on af_kmersearch table.

Required arguments:
  database_name     PostgreSQL database name

Required options:
  --mode=MODE       Operation mode: 'create' or 'drop'

Other options:
  --host=HOST       PostgreSQL server host (default: \$PGHOST or localhost)
  --port=PORT       PostgreSQL server port (default: \$PGPORT or 5432)
  --username=USER   PostgreSQL username (default: \$PGUSER or current user)
  --tablespace=NAME Tablespace name for CREATE INDEX (default: default tablespace)
  --kmer_size=INT   K-mer length for index creation (default: 8, range: 4-64)
  --workingmemory=SIZE Working memory for index creation (e.g., 64GB, 512MB, default: PostgreSQL setting)
  --help, -h        Show this help message

Environment variables:
  PGHOST           PostgreSQL server host
  PGPORT           PostgreSQL server port
  PGUSER           PostgreSQL username
  PGPASSWORD       PostgreSQL password

Examples:
  perl af_kmerindex.pl --mode=create mydb
  perl af_kmerindex.pl --mode=drop mydb
  perl af_kmerindex.pl --mode=create --tablespace=fast_ssd mydb
  perl af_kmerindex.pl --mode=create --kmer_size=16 mydb
  perl af_kmerindex.pl --mode=create --workingmemory=64GB mydb
  perl af_kmerindex.pl --mode=create --kmer_size=32 --workingmemory=128GB --tablespace=fast_ssd mydb

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
    
    # Check if af_kmersearch table exists
    $sth = $dbh->prepare(<<SQL);
SELECT COUNT(*)
FROM information_schema.tables 
WHERE table_name = 'af_kmersearch'
SQL
    $sth->execute();
    my ($table_count) = $sth->fetchrow_array();
    $sth->finish();
    
    die "Table 'af_kmersearch' does not exist in database '$database_name'\n" 
        unless $table_count > 0;
    
    # Check if required columns exist with correct types
    $sth = $dbh->prepare(<<SQL);
SELECT column_name, data_type
FROM information_schema.columns 
WHERE table_name = 'af_kmersearch'
AND column_name IN ('seq', 'part', 'seqid')
ORDER BY column_name
SQL
    $sth->execute();
    
    my %columns = ();
    while (my ($col, $type) = $sth->fetchrow_array()) {
        $columns{$col} = $type;
    }
    $sth->finish();
    
    die "Required columns not found in table 'af_kmersearch'\n"
        unless exists $columns{seq} && exists $columns{part} && exists $columns{seqid};
    
    die "Column 'part' must be ARRAY type\n" unless $columns{part} eq 'ARRAY';
    die "Column 'seqid' must be ARRAY type\n" unless $columns{seqid} eq 'ARRAY';
    die "Column 'seq' must be DNA2 or DNA4 type\n" 
        unless lc($columns{seq}) eq 'dna2' || lc($columns{seq}) eq 'dna4';
    
    print "Database structure verified.\n";
}

sub create_indexes {
    my ($dbh) = @_;
    
    print "Creating GIN indexes...\n";
    
    # Validate and set working memory if specified
    if ($workingmemory) {
        validate_and_set_working_memory($dbh, $workingmemory);
    }
    
    # Set k-mer size for pg_kmersearch
    print "Setting k-mer size to $kmer_size...\n";
    eval {
        $dbh->do("SET kmersearch.kmer_size = $kmer_size");
        print "K-mer size set to $kmer_size successfully.\n";
    };
    if ($@) {
        die "Failed to set k-mer size: $@\n";
    }
    
    # Check if indexes already exist
    my $existing_indexes = get_existing_indexes($dbh);
    
    # Create seq column GIN index
    my $seq_index_name = 'idx_af_kmersearch_seq_gin';
    if (exists $existing_indexes->{$seq_index_name}) {
        print "Index '$seq_index_name' already exists, skipping...\n";
    } else {
        print "Creating index '$seq_index_name'...\n";
        my $seq_sql = "CREATE INDEX $seq_index_name ON af_kmersearch USING gin(seq)";
        if ($tablespace) {
            $seq_sql .= " TABLESPACE \"$tablespace\"";
        }
        
        eval {
            $dbh->do($seq_sql);
            print "Index '$seq_index_name' created successfully.\n";
        };
        if ($@) {
            die "Failed to create index '$seq_index_name': $@\n";
        }
    }
    
    # Create part column GIN index
    my $part_index_name = 'idx_af_kmersearch_part_gin';
    if (exists $existing_indexes->{$part_index_name}) {
        print "Index '$part_index_name' already exists, skipping...\n";
    } else {
        print "Creating index '$part_index_name'...\n";
        my $part_sql = "CREATE INDEX $part_index_name ON af_kmersearch USING gin(part)";
        if ($tablespace) {
            $part_sql .= " TABLESPACE \"$tablespace\"";
        }
        
        eval {
            $dbh->do($part_sql);
            print "Index '$part_index_name' created successfully.\n";
        };
        if ($@) {
            die "Failed to create index '$part_index_name': $@\n";
        }
    }
    
    # Create seqid column GIN index
    my $seqid_index_name = 'idx_af_kmersearch_seqid_gin';
    if (exists $existing_indexes->{$seqid_index_name}) {
        print "Index '$seqid_index_name' already exists, skipping...\n";
    } else {
        print "Creating index '$seqid_index_name'...\n";
        my $seqid_sql = "CREATE INDEX $seqid_index_name ON af_kmersearch USING gin(seqid)";
        if ($tablespace) {
            $seqid_sql .= " TABLESPACE \"$tablespace\"";
        }
        
        eval {
            $dbh->do($seqid_sql);
            print "Index '$seqid_index_name' created successfully.\n";
        };
        if ($@) {
            die "Failed to create index '$seqid_index_name': $@\n";
        }
    }
    
    # Update kmer_size in af_kmersearch_meta table
    print "Updating kmer_size in af_kmersearch_meta table...\n";
    eval {
        my $update_sth = $dbh->prepare("UPDATE af_kmersearch_meta SET kmer_size = ?");
        $update_sth->execute($kmer_size);
        $update_sth->finish();
        print "K-mer size set to $kmer_size in af_kmersearch_meta table.\n";
    };
    if ($@) {
        die "Failed to update kmer_size in af_kmersearch_meta table: $@\n";
    }
    
    print "All indexes created successfully.\n";
}

sub drop_indexes {
    my ($dbh) = @_;
    
    print "Dropping GIN indexes...\n";
    
    # Get existing indexes
    my $existing_indexes = get_existing_indexes($dbh);
    
    # List of indexes to drop (for seq, part, and seqid columns)
    my @indexes_to_drop = ();
    
    # Find all indexes on seq, part, and seqid columns
    for my $index_name (keys %$existing_indexes) {
        my $index_info = $existing_indexes->{$index_name};
        if ($index_info->{columns} =~ /\b(seq|part|seqid)\b/) {
            push @indexes_to_drop, $index_name;
        }
    }
    
    if (@indexes_to_drop == 0) {
        print "No indexes found on 'seq', 'part', or 'seqid' columns.\n";
        return;
    }
    
    # Drop each index
    for my $index_name (@indexes_to_drop) {
        print "Dropping index '$index_name'...\n";
        eval {
            $dbh->do("DROP INDEX IF EXISTS \"$index_name\"");
            print "Index '$index_name' dropped successfully.\n";
        };
        if ($@) {
            print STDERR "Warning: Failed to drop index '$index_name': $@\n";
        }
    }
    
    # Set kmer_size to NULL in af_kmersearch_meta table
    print "Setting kmer_size to NULL in af_kmersearch_meta table...\n";
    eval {
        my $update_sth = $dbh->prepare("UPDATE af_kmersearch_meta SET kmer_size = NULL");
        $update_sth->execute();
        $update_sth->finish();
        print "K-mer size set to NULL in af_kmersearch_meta table.\n";
    };
    if ($@) {
        print STDERR "Warning: Failed to update kmer_size in af_kmersearch_meta table: $@\n";
    }
    
    print "Index dropping completed.\n";
}

sub get_existing_indexes {
    my ($dbh) = @_;
    
    my $sth = $dbh->prepare(<<SQL);
SELECT 
    i.indexname,
    i.indexdef,
    string_agg(a.attname, ', ') as columns
FROM pg_indexes i
JOIN pg_class c ON c.relname = i.indexname
JOIN pg_index idx ON idx.indexrelid = c.oid
JOIN pg_attribute a ON a.attrelid = idx.indrelid AND a.attnum = ANY(idx.indkey)
WHERE i.tablename = 'af_kmersearch'
GROUP BY i.indexname, i.indexdef
ORDER BY i.indexname
SQL
    
    $sth->execute();
    
    my %indexes = ();
    while (my ($name, $def, $cols) = $sth->fetchrow_array()) {
        $indexes{$name} = {
            definition => $def,
            columns => $cols
        };
    }
    $sth->finish();
    
    return \%indexes;
}

sub validate_and_set_working_memory {
    my ($dbh, $memory_value) = @_;
    
    print "Validating and setting working memory to '$memory_value'...\n";
    
    # First, validate that PostgreSQL can recognize the memory value
    eval {
        # Test the memory value by temporarily setting it
        my $sth = $dbh->prepare("SELECT setting FROM pg_settings WHERE name = 'maintenance_work_mem'");
        $sth->execute();
        my ($original_value) = $sth->fetchrow_array();
        $sth->finish();
        
        # Try to set the new value
        $dbh->do("SET maintenance_work_mem = '$memory_value'");
        
        # If successful, get the actual value PostgreSQL understood
        $sth = $dbh->prepare("SHOW maintenance_work_mem");
        $sth->execute();
        my ($actual_value) = $sth->fetchrow_array();
        $sth->finish();
        
        print "Working memory set to '$actual_value' (from '$memory_value').\n";
    };
    
    if ($@) {
        die "Invalid working memory value '$memory_value': $@\n";
    }
}

sub validate_tablespace_exists {
    my ($dbh, $tablespace_name) = @_;
    
    print "Validating tablespace '$tablespace_name'...\n";
    
    my $sth = $dbh->prepare("SELECT 1 FROM pg_tablespace WHERE spcname = ?");
    eval {
        $sth->execute($tablespace_name);
        my $exists = $sth->fetchrow_array();
        $sth->finish();
        
        unless ($exists) {
            die "Tablespace '$tablespace_name' does not exist\n";
        }
        
        print "Tablespace '$tablespace_name' exists.\n";
    };
    
    if ($@) {
        die "Failed to validate tablespace '$tablespace_name': $@\n";
    }
}