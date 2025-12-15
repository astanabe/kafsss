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
my $default_datatype = 'DNA4';
my $default_minsplitlen = 50000;
my $default_minlen = 64;
my $default_ovllen = 500;
my $default_numthreads = 1;
my $default_compress = 'lz4';
my $default_batchsize = 100000;

# Command line options
my $host = $default_host;
my $port = $default_port;
my $username = $default_user;
my $datatype = $default_datatype;
my $minsplitlen = $default_minsplitlen;
my $minlen = $default_minlen;
my $ovllen = $default_ovllen;
my $numthreads = $default_numthreads;
my $compress = $default_compress;
my $batchsize = $default_batchsize;
my @subsets = ();
my $tablespace = '';
my $workingmemory = '8GB';
my $maintenanceworkingmemory = '8GB';
my $temporarybuffer = '512MB';
my $overwrite = 0;
my $verbose = 0;
my $help = 0;

# Parse command line options
GetOptions(
    'host=s' => \$host,
    'port=i' => \$port,
    'username=s' => \$username,
    'datatype=s' => \$datatype,
    'minsplitlen=i' => \$minsplitlen,
    'minlen=i' => \$minlen,
    'ovllen=i' => \$ovllen,
    'numthreads=i' => \$numthreads,
    'compress=s' => \$compress,
    'batchsize=i' => \$batchsize,
    'subset=s' => \@subsets,
    'tablespace=s' => \$tablespace,
    'workingmemory=s' => \$workingmemory,
    'maintenanceworkingmemory=s' => \$maintenanceworkingmemory,
    'temporarybuffer=s' => \$temporarybuffer,
    'overwrite:s' => sub {
        my ($name, $value) = @_;
        $overwrite = (!defined $value || $value eq '' || $value eq 'enable') ? 1 : 0;
    },
    'verbose|v' => \$verbose,
    'help|h' => \$help,
) or die "Error in command line arguments\n";

# Show help
if ($help) {
    print_help();
    exit 0;
}

# Check required arguments
if (@ARGV < 2) {
    die "Usage: kafssstore [options] input_file(s) output_database\n" .
        "Use --help for detailed usage information.\n";
}

my $output_db = pop @ARGV;
my @input_patterns = @ARGV;

# Expand input file patterns (glob)
my @input_files = expand_input_files(@input_patterns);

# Validate that we have at least one input file
if (@input_files == 0) {
    die "No input files found for pattern(s): " . join(', ', @input_patterns) . "\n";
}

# Validate datatype
die "Invalid datatype '$datatype'. Must be DNA2 or DNA4\n" 
    unless $datatype eq 'DNA2' || $datatype eq 'DNA4';

# Validate compress option
die "Invalid compress option '$compress'. Must be lz4, pglz, or disable\n"
    unless $compress eq 'lz4' || $compress eq 'pglz' || $compress eq 'disable';

# Validate minsplitlen, minlen and ovllen
die "minsplitlen must be positive integer\n" unless $minsplitlen > 0;
die "minlen must be non-negative integer\n" unless $minlen >= 0;
die "ovllen must be non-negative integer\n" unless $ovllen >= 0;
die "ovllen must be less than half of minsplitlen to prevent overlap conflicts\n" unless $ovllen < $minsplitlen / 2;
die "numthreads must be positive integer\n" unless $numthreads > 0;
die "batchsize must be positive integer\n" unless $batchsize > 0;

# Parse subsets from comma-separated values
my @all_subsets = ();
for my $subset_spec (@subsets) {
    push @all_subsets, split(/,/, $subset_spec);
}

# Create subset array for PostgreSQL
my $subset_array = \@all_subsets;

print "kafssstore version $VERSION\n";
print "Input files (" . scalar(@input_files) . "):\n";
for my $i (0..$#input_files) {
    print "  " . ($i + 1) . ". $input_files[$i]\n";
}
print "Output database: $output_db\n";
print "Host: $host\n";
print "Port: $port\n";
print "Username: $username\n";
print "Data type: $datatype\n";
print "Min split length: $minsplitlen\n";
print "Min length: $minlen\n";
print "Overlap length: $ovllen\n";
print "Number of threads: $numthreads\n";
print "Compression: $compress\n";
print "Batch size: $batchsize\n";
print "Subsets: " . (@all_subsets ? join(', ', @all_subsets) : 'none') . "\n";
print "Tablespace: " . ($tablespace ? $tablespace : 'default') . "\n";
print "Overwrite: " . ($overwrite ? 'yes' : 'no') . "\n";

# Connect to PostgreSQL server
my $password = $ENV{PGPASSWORD} || '';
my $dsn = "DBI:Pg:host=$host;port=$port";

my $dbh = DBI->connect($dsn, $username, $password, {
    AutoCommit => 1,
    PrintError => 0,
    RaiseError => 1,
    ShowErrorStatement => 1,
    AutoInactiveDestroy => 1,
    pg_enable_utf8 => 1
}) or die "Cannot connect to PostgreSQL server: $DBI::errstr\n";

# Validate user existence and permissions
validate_user_and_permissions($dbh, $username);

# Validate tablespace if specified
if ($tablespace) {
    validate_tablespace_exists($dbh, $tablespace);
}

# Check if database exists
my $db_exists = check_database_exists($dbh, $output_db);

if ($db_exists && !$overwrite) {
    # Validate existing database with retry logic for initialization in progress
    my $max_retries = 10;
    my $retry_interval = 5;
    my $validation_result;

    for my $attempt (1..$max_retries) {
        $validation_result = validate_existing_database($dbh, $output_db);

        if ($validation_result == -2) {
            # Tables not ready: another process may be initializing
            print "Database schema not ready (attempt $attempt/$max_retries), waiting for initialization lock...\n";

            # Connect to target database to wait for initialization lock
            my $temp_dsn = "DBI:Pg:dbname=$output_db;host=$host;port=$port";
            my $temp_dbh = DBI->connect($temp_dsn, $username, $password, {
                AutoCommit => 1,
                PrintError => 0,
                RaiseError => 0,
                AutoInactiveDestroy => 1,
            });

            if ($temp_dbh) {
                # Wait for initialization lock (blocking) - this ensures we wait for setup_database to complete
                eval {
                    $temp_dbh->do("SELECT pg_advisory_lock(1000)");
                    $temp_dbh->do("SELECT pg_advisory_unlock(1000)");
                };
                $temp_dbh->disconnect();
            } else {
                # Cannot connect yet, wait and retry
                sleep($retry_interval);
            }
            next;
        }
        last;  # Exit loop for any result other than -2
    }

    if ($validation_result == 1) {
        print "Using existing database '$output_db'\n";
    } elsif ($validation_result == -1) {
        die "Existing database '$output_db' has indexes. Cannot add data while indexes exist.\n" .
            "To add data, first drop indexes using: kafssindex --mode=drop $output_db\n";
    } elsif ($validation_result == -2) {
        die "Database schema still not ready after $max_retries attempts (${retry_interval}s intervals).\n" .
            "Another process may have failed during initialization.\n";
    } else {
        die "Existing database '$output_db' is not compatible. Parameter mismatch detected.\n" .
            "Use --overwrite to recreate database, or adjust parameters to match existing database.\n" .
            "Check minlen ($minlen), minsplitlen ($minsplitlen), ovllen ($ovllen), and datatype ($datatype).\n";
    }
} else {
    # Drop database if overwrite is specified
    if ($db_exists && $overwrite) {
        print "Dropping existing database '$output_db'\n";
        $dbh->do("DROP DATABASE \"$output_db\"");
    }

    # Create new database
    print "Creating database '$output_db'\n";
    my $create_db_sql = "CREATE DATABASE \"$output_db\"";
    if ($tablespace) {
        $create_db_sql .= " TABLESPACE \"$tablespace\"";
    }

    eval {
        $dbh->do($create_db_sql);
    };
    if ($@) {
        # Check if database was created by another process
        if (check_database_exists($dbh, $output_db)) {
            print "Database '$output_db' was created by another process, continuing...\n";
            # Validate the database created by another process with retry logic
            my $max_retries = 10;
            my $retry_interval = 5;
            my $validation_result;

            for my $attempt (1..$max_retries) {
                $validation_result = validate_existing_database($dbh, $output_db);

                if ($validation_result == -2) {
                    # Tables not ready: another process may be initializing
                    print "Database schema not ready (attempt $attempt/$max_retries), waiting for initialization lock...\n";

                    # Connect to target database to wait for initialization lock
                    my $temp_dsn = "DBI:Pg:dbname=$output_db;host=$host;port=$port";
                    my $temp_dbh = DBI->connect($temp_dsn, $username, $password, {
                        AutoCommit => 1,
                        PrintError => 0,
                        RaiseError => 0,
                        AutoInactiveDestroy => 1,
                    });

                    if ($temp_dbh) {
                        # Wait for initialization lock (blocking) - this ensures we wait for setup_database to complete
                        eval {
                            $temp_dbh->do("SELECT pg_advisory_lock(1000)");
                            $temp_dbh->do("SELECT pg_advisory_unlock(1000)");
                        };
                        $temp_dbh->disconnect();
                    } else {
                        # Cannot connect yet, wait and retry
                        sleep($retry_interval);
                    }
                    next;
                }
                last;  # Exit loop for any result other than -2
            }

            if ($validation_result == 1) {
                print "Using database '$output_db' created by another process\n";
            } elsif ($validation_result == -1) {
                die "Database '$output_db' has indexes. Cannot add data while indexes exist.\n" .
                    "To add data, first drop indexes using: kafssindex --mode=drop $output_db\n";
            } elsif ($validation_result == -2) {
                die "Database schema still not ready after $max_retries attempts (${retry_interval}s intervals).\n" .
                    "Another process may have failed during initialization.\n";
            } else {
                die "Database '$output_db' is not compatible. Parameter mismatch detected.\n" .
                    "Check minlen ($minlen), minsplitlen ($minsplitlen), ovllen ($ovllen), and datatype ($datatype).\n";
            }
            # Mark as existing for later logic
            $db_exists = 1;
        } else {
            die "Failed to create database '$output_db': $@\n";
        }
    }
}

# Disconnect from server and connect to target database
$dbh->disconnect();

$dsn = "DBI:Pg:dbname=$output_db;host=$host;port=$port";

$dbh = DBI->connect($dsn, $username, $password, {
    AutoCommit => 1,
    PrintError => 0,
    RaiseError => 1,
    ShowErrorStatement => 1,
    AutoInactiveDestroy => 1,
    pg_enable_utf8 => 1
}) or die "Cannot connect to database '$output_db': $DBI::errstr\n";

# Setup database if new or overwritten (with exclusive control)
if (!$db_exists || $overwrite) {
    # Try to acquire initialization lock (session-level advisory lock)
    print "Acquiring initialization lock...\n";
    my ($got_lock) = $dbh->selectrow_array("SELECT pg_try_advisory_lock(1000)");

    if ($got_lock) {
        # Lock acquired: this process will initialize
        # But first check if another process already completed initialization
        my $tables_exist = check_tables_already_exist($dbh);
        if (!$tables_exist) {
            print "Initializing database schema...\n";
            setup_database($dbh);
        } else {
            print "Tables already created by another process, skipping setup.\n";
        }
        # Release initialization lock
        $dbh->do("SELECT pg_advisory_unlock(1000)");
        print "Initialization lock released.\n";
    } else {
        # Lock not acquired: another process is initializing
        print "Another process is initializing database, waiting for completion...\n";
        # Wait for initialization to complete (blocking lock)
        $dbh->do("SELECT pg_advisory_lock(1000)");
        # Immediately release the lock (we just needed to wait)
        $dbh->do("SELECT pg_advisory_unlock(1000)");
        print "Initialization completed by another process.\n";
    }
}

# Process FASTA files
print "Processing FASTA files...\n" if $verbose;
my $total_sequences = 0;

# Disconnect main process before starting worker processes
print "Disconnecting main process to allow worker processes to run...\n" if $verbose;
$dbh->disconnect();

eval {
    for my $i (0..$#input_files) {
        my $input_file = $input_files[$i];
        print "Processing file " . ($i + 1) . "/" . scalar(@input_files) . ": $input_file\n" if $verbose;
        my $file_sequences = process_fasta_file($input_file, undef);
        $total_sequences += $file_sequences;
        print "  Processed $file_sequences sequences from this file.\n" if $verbose;
    }
    
    print "All worker processes completed successfully.\n" if $verbose;
};

if ($@) {
    print STDERR "Error during FASTA file processing: $@\n";
    die "FASTA file processing failed: $@\n";
}

print "Total sequences processed: $total_sequences\n";

# Reconnect main process for statistics update
print "Reconnecting main process for statistics update...\n" if $verbose;
$dbh = DBI->connect($dsn, $username, $password, {
    AutoCommit => 1,
    PrintError => 0,
    RaiseError => 1,
    ShowErrorStatement => 1,
    AutoInactiveDestroy => 1,
    pg_enable_utf8 => 1
}) or die "Cannot reconnect to database '$output_db': $DBI::errstr\n";

# Set application name for identification in pg_stat_activity
# Include database name to distinguish from kafssstore processes writing to other databases
$dbh->do("SET application_name = 'kafssstore:$output_db'");

# Verify table exists
ensure_table_exists($dbh, 'kafsss_data');

# Final verification with record count
my $final_count = $dbh->selectrow_array("SELECT COUNT(*) FROM kafsss_data");
unless (defined $final_count) {
    die "Table verification failed: kafsss_data table missing\n";
}
print "Final count: $final_count sequences.\n";

# Update statistics in kafsss_meta table
print "Updating statistics in kafsss_meta table...\n" if $verbose;
update_meta_statistics($dbh);

print "Processing completed successfully.\n";

$dbh->disconnect();

exit 0;

#
# Subroutines
#

sub print_help {
    print <<EOF;
kafssstore version $VERSION

Usage: kafssstore [options] input_file(s) output_database

Store multi-FASTA DNA sequences from multiple sources into PostgreSQL database using pg_kmersearch extension.

Required arguments:
  input_file(s)     Input FASTA file(s), patterns, or databases:
                    - Regular files: file1.fasta file2.fasta
                    - Wildcard patterns: 'data/*.fasta' (use quotes to prevent shell expansion)
                    - Compressed files: file.fasta.gz file.fasta.bz2 file.fasta.xz file.fasta.zst
                    - BLAST databases: mydb (requires mydb.nsq or mydb.nal)
                    - Standard input: '-', 'stdin', or 'STDIN'
  output_database   Output database name

Options:
  --host=HOST       PostgreSQL server host (default: \$PGHOST or localhost)
  --port=PORT       PostgreSQL server port (default: \$PGPORT or 5432)
  --username=USER   PostgreSQL username (default: \$PGUSER or current user)
  --datatype=TYPE   Data type DNA2 or DNA4 (default: DNA4)
  --minsplitlen=INT Minimum sequence length for splitting (default: 50000)
  --minlen=INT      Minimum sequence length filter, shorter sequences skipped (default: 64)
  --ovllen=INT      Overlap length between split sequences (default: 500)
  --numthreads=INT  Number of parallel threads (default: 1)
  --compress=TYPE   Column compression type: lz4, pglz, or disable (default: lz4)
  --batchsize=INT   Batch size for fragment processing (default: 100000)
  --subset=NAME     Subset name (can be specified multiple times or comma-separated)
  --tablespace=NAME Tablespace name for CREATE DATABASE (default: default tablespace)
  --workingmemory=SIZE        Work memory for each operation (default: 8GB)
  --maintenanceworkingmemory=SIZE  Maintenance work memory for operations (default: 8GB)
  --temporarybuffer=SIZE      Temporary buffer size (default: 512MB)
  --overwrite       Overwrite existing database (default: false)
  --verbose, -v     Show detailed processing messages (default: false)
  --help, -h        Show this help message

Environment variables:
  PGHOST           PostgreSQL server host
  PGPORT           PostgreSQL server port
  PGUSER           PostgreSQL username
  PGPASSWORD       PostgreSQL password

Examples:
  # Single file
  kafssstore input.fasta mydb
  
  # Multiple files
  kafssstore file1.fasta file2.fasta mydb
  
  # Wildcard pattern (use quotes to prevent shell expansion)
  kafssstore 'data/*.fasta' mydb
  kafssstore '/path/to/genomes/*.fna' mydb
  
  # Compressed files
  kafssstore genome.fasta.gz mydb
  kafssstore 'data/*.fasta.bz2' mydb
  kafssstore sequence.fna.xz mydb
  kafssstore genome.fasta.zst mydb
  
  # BLAST database
  kafssstore nr mydb
  kafssstore /databases/nt mydb
  
  # Mixed sources
  kafssstore file1.fasta 'data/*.fasta.gz' blastdb mydb
  
  # With options
  kafssstore --datatype=DNA2 --minsplitlen=100000 'genomes/*.fasta' mydb
  kafssstore --minlen=1000 --minsplitlen=50000 'genomes/*.fasta' mydb
  kafssstore --subset=bacteria,archaea 'bacteria/*.fasta' mydb
  kafssstore --overwrite --numthreads=4 'data/*.fasta.gz' mydb
  kafssstore --workingmemory=32GB --maintenanceworkingmemory=64GB 'genomes/*.fasta' mydb
  kafssstore --verbose --temporarybuffer=1GB 'genomes/*.fasta' mydb
  
  # Standard input
  cat input.fasta | kafssstore stdin mydb

EOF
}

sub check_database_exists {
    my ($dbh, $dbname) = @_;

    my $sth = $dbh->prepare("SELECT 1 FROM pg_database WHERE datname = ?");
    $sth->execute($dbname);
    my $result = $sth->fetchrow_array();
    $sth->finish();

    return defined $result;
}

sub check_tables_already_exist {
    my ($dbh) = @_;

    my $sth = $dbh->prepare(<<SQL);
SELECT COUNT(*) FROM information_schema.tables
WHERE table_schema = 'public' AND table_name IN ('kafsss_meta', 'kafsss_data')
SQL
    $sth->execute();
    my ($count) = $sth->fetchrow_array();
    $sth->finish();

    return $count == 2;
}

sub validate_existing_database {
    my ($dbh, $dbname) = @_;
    
    # Connect to target database for validation
    my $temp_dsn = "DBI:Pg:dbname=$dbname;host=$host;port=$port";
    my $password = $ENV{PGPASSWORD} || '';
    
    my $temp_dbh = DBI->connect($temp_dsn, $username, $password, {
        AutoCommit => 1,
        PrintError => 0,
        RaiseError => 0,
        ShowErrorStatement => 1,
        AutoInactiveDestroy => 1,
        pg_enable_utf8 => 1
    });
    
    return 0 unless $temp_dbh;
    
    # Check if pg_kmersearch extension exists
    # Return -2 if not exists (database initialization in progress by another process)
    my $sth = $temp_dbh->prepare("SELECT 1 FROM pg_extension WHERE extname = 'pg_kmersearch'");
    $sth->execute();
    my $ext_exists = $sth->fetchrow_array();
    $sth->finish();
    unless ($ext_exists) {
        $temp_dbh->disconnect();
        return -2;  # Extension not yet created, initialization in progress
    }

    # Check if required tables exist with correct schema
    # Return -2 if tables don't exist (initialization in progress)
    unless (check_meta_table_schema($temp_dbh)) {
        $temp_dbh->disconnect();
        return -2;  # Tables not yet created, initialization in progress
    }
    unless (check_main_table_schema($temp_dbh)) {
        $temp_dbh->disconnect();
        return -2;  # Tables not yet created, initialization in progress
    }

    # Check meta table values
    $sth = $temp_dbh->prepare("SELECT ver, minlen, minsplitlen, ovllen FROM kafsss_meta LIMIT 1");
    $sth->execute();
    my ($db_ver, $db_minlen, $db_minsplitlen, $db_ovllen) = $sth->fetchrow_array();
    $sth->finish();

    unless (defined $db_ver && defined $db_minlen && defined $db_minsplitlen && defined $db_ovllen) {
        $temp_dbh->disconnect();
        return -2;  # Meta data not yet inserted, initialization in progress
    }
    
    # Check parameter compatibility with detailed error messages
    if ($db_ver ne $VERSION) {
        print STDERR "Error: Database version mismatch. Database: '$db_ver', Tool: '$VERSION'\n";
        return 0;
    }
    if ($db_minlen != $minlen) {
        print STDERR "Error: minlen parameter mismatch. Database: $db_minlen, Specified: $minlen\n";
        print STDERR "       Please specify --minlen=$db_minlen to match existing database\n";
        return 0;
    }
    if ($db_minsplitlen != $minsplitlen) {
        print STDERR "Error: minsplitlen parameter mismatch. Database: $db_minsplitlen, Specified: $minsplitlen\n";
        print STDERR "       Please specify --minsplitlen=$db_minsplitlen to match existing database\n";
        return 0;
    }
    if ($db_ovllen != $ovllen) {
        print STDERR "Error: ovllen parameter mismatch. Database: $db_ovllen, Specified: $ovllen\n";
        print STDERR "       Please specify --ovllen=$db_ovllen to match existing database\n";
        return 0;
    }
    
    # Check that seq column datatype matches
    $sth = $temp_dbh->prepare(<<SQL);
SELECT CASE WHEN data_type = 'USER-DEFINED' THEN udt_name ELSE data_type END AS data_type
FROM information_schema.columns 
WHERE table_name = 'kafsss_data' AND column_name = 'seq'
SQL
    $sth->execute();
    my ($seq_datatype) = $sth->fetchrow_array();
    $sth->finish();
    
    unless (defined $seq_datatype) {
        print STDERR "Error: Could not determine seq column datatype from database\n";
        return 0;
    }
    
    my $expected_datatype = lc($datatype) eq 'dna2' ? 'dna2' : 'dna4';
    unless (lc($seq_datatype) eq $expected_datatype) {
        my $db_datatype_name = lc($seq_datatype) eq 'dna2' ? 'DNA2' : 'DNA4';
        print STDERR "Error: datatype parameter mismatch. Database: '" . lc($seq_datatype) . "', Specified: '" . lc($datatype) . "'\n";
        print STDERR "       Please specify --datatype=$db_datatype_name to match existing database\n";
        return 0;
    }
    
    # Check that no seq-related indexes exist on kafsss_data table
    $sth = $temp_dbh->prepare(
        "SELECT 1 FROM pg_indexes WHERE tablename = 'kafsss_data' AND indexname LIKE '%seq%' LIMIT 1"
    );
    $sth->execute();
    my $index_exists = $sth->fetchrow_array();
    $sth->finish();
    
    $temp_dbh->disconnect();
    
    # Return values:
    #   1: Valid database, ready to use
    #   0: Invalid (parameter mismatch)
    #  -1: Indexes exist (cannot add data)
    #  -2: Initialization in progress (tables/extension not ready)
    return $index_exists ? -1 : 1;
}

sub check_meta_table_schema {
    my ($dbh) = @_;

    my $sth = $dbh->prepare(<<SQL);
SELECT column_name, CASE WHEN data_type = 'USER-DEFINED' THEN udt_name ELSE data_type END AS data_type
FROM information_schema.columns
WHERE table_name = 'kafsss_meta'
ORDER BY column_name
SQL
    $sth->execute();

    my %expected = (
        'ver' => 'text',
        'minlen' => 'integer',
        'minsplitlen' => 'integer',
        'ovllen' => 'smallint',
        'nseq' => 'bigint',
        'nchar' => 'bigint',
        'subset' => 'jsonb'
    );

    my %actual = ();
    while (my ($col, $type) = $sth->fetchrow_array()) {
        $actual{$col} = $type;
    }
    $sth->finish();

    return %actual == %expected &&
           $actual{ver} eq $expected{ver} &&
           $actual{minlen} eq $expected{minlen} &&
           $actual{minsplitlen} eq $expected{minsplitlen} &&
           $actual{ovllen} eq $expected{ovllen} &&
           $actual{nseq} eq $expected{nseq} &&
           $actual{nchar} eq $expected{nchar} &&
           $actual{subset} eq $expected{subset};
}

sub check_main_table_schema {
    my ($dbh) = @_;
    
    my $sth = $dbh->prepare(<<SQL);
SELECT column_name, CASE WHEN data_type = 'USER-DEFINED' THEN udt_name ELSE data_type END AS data_type
FROM information_schema.columns 
WHERE table_name = 'kafsss_data' 
ORDER BY column_name
SQL
    $sth->execute();
    
    my %expected = (
        'seq' => lc($datatype),
        'subset' => 'ARRAY',
        'seqid' => 'ARRAY'
    );
    
    my %actual = ();
    while (my ($col, $type) = $sth->fetchrow_array()) {
        $actual{$col} = $type;
    }
    $sth->finish();
    
    return %actual == %expected && 
           $actual{seq} eq $expected{seq} && 
           $actual{subset} eq $expected{subset} && 
           $actual{seqid} eq $expected{seqid};
}

sub setup_database {
    my ($dbh) = @_;
    
    print "Setting up database schema...\n";
    
    # Check if pg_kmersearch extension is available and create if needed
    check_and_create_extension($dbh, "pg_kmersearch");
    
    
    # Create meta table
    $dbh->do(<<SQL);
CREATE TABLE IF NOT EXISTS kafsss_meta (
    ver TEXT NOT NULL,
    minlen INTEGER NOT NULL,
    minsplitlen INTEGER NOT NULL,
    ovllen SMALLINT NOT NULL,
    nseq BIGINT,
    nchar BIGINT,
    subset JSONB
)
SQL
    
    # Insert meta data
    my $sth = $dbh->prepare("DELETE FROM kafsss_meta");
    $sth->execute();
    $sth->finish();
    
    $sth = $dbh->prepare("INSERT INTO kafsss_meta (ver, minlen, minsplitlen, ovllen, nseq, nchar, subset) VALUES (?, ?, ?, ?, ?, ?, ?)");
    $sth->execute($VERSION, $minlen, $minsplitlen, $ovllen, 0, 0, '{}');
    $sth->finish();
    
    # Create main table (simple structure - hash functions handle efficiency)
    $dbh->do(<<SQL);
CREATE TABLE IF NOT EXISTS kafsss_data (
    seq $datatype NOT NULL,
    subset TEXT[],
    seqid TEXT[] NOT NULL
)
SQL
    
    
    # Configure compression for kafsss_data table columns
    configure_table_compression($dbh);
    
    print "Database schema setup completed.\n" if $verbose;
}

sub process_fasta_file {
    my ($filename, $dbh) = @_;
    
    my $fh = open_input_file($filename);
    my $sequence_count = 0;
    
    # Process sequences with streaming batch processing
    $sequence_count = process_streaming_with_batches($fh);
    
    # Close file handle unless it's STDIN
    unless ($filename eq '-' || $filename eq 'stdin' || $filename eq 'STDIN') {
        # For BLAST databases opened with pipe, ignore ECHILD error
        if (!close($fh)) {
            # Only warn if it's not a "No child processes" error (ECHILD)
            warn "Warning: Could not close file handle for '$filename': $!\n" unless $! =~ /No child processes/;
        }
    }
    
    return $sequence_count;
}

sub read_next_fasta_entry {
    my ($fh) = @_;
    
    # Set input record separator to read one FASTA entry at a time
    local $/ = "\n>";
    
    my $line = <$fh>;
    return undef unless defined $line;
    
    # Remove trailing '>' if present (from record separator)
    $line =~ s/>$//;
    
    # Parse the FASTA record using regex that handles optional leading '>'
    # and captures label (up to first newline) and sequence (rest, may contain newlines)
    if ($line =~ /^>?\s*(\S[^\r\n]*)\r?\n(.*)/s) {
        my $label = $1;
        my $sequence = uc($2);
        
        # Remove non-alphabetic characters from sequence (keeps only A-Z)
        $sequence =~ s/[^A-Z]//sg;
        
        return {
            label => $label,
            sequence => $sequence
        };
    }
    
    return undef;  # Invalid FASTA format
}

sub process_streaming_with_batches {
    my ($fh) = @_;
    
    my $sequence_count = 0;
    my @fragment_batch = ();
    my @active_children = ();
    
    # Read and process sequences one by one
    while (my $seq_entry = read_next_fasta_entry($fh)) {
        my $seq_data = {
            header => $seq_entry->{label},
            sequence => $seq_entry->{sequence}
        };
        
        # Process sequence to get fragments
        my @fragments = process_sequence_to_fragments($seq_data);
        push @fragment_batch, @fragments;
        $sequence_count++;
        
        # Check if batch is ready for processing
        if (scalar(@fragment_batch) >= $batchsize) {
            # Wait for available slot if we have too many active children
            while (scalar(@active_children) >= $numthreads) {
                wait_for_any_child(\@active_children);
            }
            
            # Start new child process for this batch
            my $pid = start_child_process(\@fragment_batch);
            push @active_children, $pid;
            
            # Clear batch for next round
            @fragment_batch = ();
        }
    }
    
    # Process remaining fragments
    if (@fragment_batch) {
        while (scalar(@active_children) >= $numthreads) {
            wait_for_any_child(\@active_children);
        }
        
        my $pid = start_child_process(\@fragment_batch);
        push @active_children, $pid;
    }
    
    # Wait for all remaining children to complete
    while (@active_children) {
        wait_for_any_child(\@active_children);
    }
    
    print "All batches completed successfully.\n" if $verbose;
    return $sequence_count;
}

sub wait_for_any_child {
    my ($active_children) = @_;
    
    my $finished_pid = wait();
    return unless $finished_pid > 0;
    
    if ($? != 0) {
        die "Child process $finished_pid failed with exit code " . ($? >> 8) . "\n";
    }
    
    # Remove finished child from active list
    @$active_children = grep { $_ != $finished_pid } @$active_children;
}

sub start_child_process {
    my ($fragments) = @_;
    
    # Make a copy of fragments for the child
    my @fragment_copy = @$fragments;
    
    my $pid = fork();
    
    if (!defined $pid) {
        die "Cannot fork: $!\n";
    } elsif ($pid == 0) {
        # Child process
        # Create new database connection for child
        my $child_dsn = "DBI:Pg:dbname=$output_db;host=$host;port=$port";
        my $password = $ENV{PGPASSWORD} || '';
        
        my $child_dbh = DBI->connect($child_dsn, $username, $password, {
            AutoCommit => 1,
            PrintError => 0,
            RaiseError => 1,
            ShowErrorStatement => 1,
            AutoInactiveDestroy => 1,
            pg_enable_utf8 => 1
        }) or die "Cannot connect to database in child process: $DBI::errstr\n";
        
        eval {
            insert_fragment_batch(\@fragment_copy, $child_dbh);
        };
        
        if ($@) {
            print STDERR "Error in child process: $@\n";
            $child_dbh->disconnect();
            exit 1;
        }
        
        $child_dbh->disconnect();
        exit 0;
    } else {
        # Parent process
        return $pid;
    }
}

sub process_sequence_to_fragments {
    my ($seq_data) = @_;
    
    my $header = $seq_data->{header};
    my $sequence = $seq_data->{sequence};
    
    # Check sequence length filter
    if ($minlen > 0 && length($sequence) < $minlen) {
        print STDERR "Skipping sequence '$header': length " . length($sequence) . " is below minimum length $minlen\n" if $verbose;
        return ();
    }
    
    # Extract accession numbers from header
    my @accessions = extract_accession($header);
    
    # Check if any accession numbers were found
    if (@accessions == 0) {
        print STDERR "Warning: No accession number could be extracted from '$header' - skipping database registration\n" if $verbose;
        return ();
    }
    
    # Split sequence into fragments (get positions only)
    my @fragment_positions = split_sequence(length($sequence), $sequence);
    
    # Create fragment records with sequence data
    my @fragments = ();
    for my $fragment_pos (@fragment_positions) {
        my $start = $fragment_pos->{start};
        my $end = $fragment_pos->{end};
        
        # Extract fragment sequence from main sequence
        my $fragment_seq = substr($sequence, $start - 1, $end - $start + 1);
        
        # Check fragment length filter
        if ($minlen > 0 && length($fragment_seq) < $minlen) {
            print STDERR "Skipping fragment: length " . length($fragment_seq) . " is below minimum length $minlen\n" if $verbose;
            next;
        }
        
        # Build seqids for all accessions
        my @seqids = ();
        for my $accession (@accessions) {
            my $seqid = sprintf("%s:%d:%d", $accession, $start, $end);
            push @seqids, $seqid;
        }
        
        push @fragments, {
            sequence => $fragment_seq,
            seqids => \@seqids
        };
    }
    
    return @fragments;
}

sub insert_fragment_batch {
    my ($fragments, $dbh) = @_;
    
    eval {
        $dbh->begin_work;
        
        my $sth = $dbh->prepare("INSERT INTO kafsss_data (seq, subset, seqid) VALUES (?, ?, ?)");
        
        for my $fragment (@$fragments) {
            my $fragment_seq = $fragment->{sequence};
            my $seqids = $fragment->{seqids};
            
            # Simple INSERT without duplicate checking
            $sth->execute($fragment_seq, $subset_array, $seqids);
        }
        
        $sth->finish();
        $dbh->commit;
    };
    
    if ($@) {
        print STDERR "Error in fragment batch insert: $@\n";
        eval { $dbh->rollback; };
        die "Fragment batch insert failed: $@\n";
    }
}

sub process_sequence {
    my ($seq_data, $dbh) = @_;
    
    my $header = $seq_data->{header};
    my $sequence = $seq_data->{sequence};
    
    # Check sequence length filter
    if ($minlen > 0 && length($sequence) < $minlen) {
        print STDERR "Skipping sequence '$header': length " . length($sequence) . " is below minimum length $minlen\n";
        return;
    }
    
    # Extract accession numbers from header
    my @accessions = extract_accession($header);
    
    # Check if any accession numbers were found
    if (@accessions == 0) {
        print STDERR "Warning: No accession number could be extracted from '$header' - skipping database registration\n";
        return;
    }
    
    # Split sequence into fragments (get positions only)
    my @fragments = split_sequence(length($sequence), $sequence);
    
    # Insert fragments into database for all accessions
    insert_sequence_fragment($sequence, \@fragments, \@accessions, $dbh);
}

sub extract_accession {
    my ($header) = @_;
    
    my @accessions = ();
    
    # Split by SOH (control character ^A) for merged FASTA labels
    my @header_subsets = split(/\cA+/, $header);
    
    for my $subset (@header_subsets) {
        # Remove everything after the first space
        $subset =~ s/ .+$//;
        
        my @subset_accessions = ();
        
        # Try to extract accession numbers from database-specific formats
        # Format: gb|U13106.1| or |gb|U13106.1| (multiple can exist)
        if ($subset =~ /^(?:gb|emb|dbj|ref|lcl)\|([^\|\s]+)/ || $subset =~ /\|(?:gb|emb|dbj|ref|lcl)\|([^\|\s]+)/) {
            my $temp_subset = $subset;
            while ($temp_subset =~ /(?:^|.)(?:gb|emb|dbj|ref|lcl)\|([^\|\s]+)/g) {
                my $acc = $1;
                $acc =~ s/\.\d+$//;  # Remove version
                push @subset_accessions, $acc;
            }
        }
        
        # Fallback: extract first token
        if (@subset_accessions == 0 && $subset =~ /^([^\|\s]+)/) {
            my $acc = $1;
            $acc =~ s/\.\d+$//;  # Remove version
            push @subset_accessions, $acc;
        }
        
        push @accessions, @subset_accessions;
    }
    
    # Remove duplicates and return all accession numbers
    my %seen = ();
    @accessions = grep { !$seen{$_}++ } @accessions;
    
    return @accessions;
}

sub split_sequence {
    my ($sequence_length, $sequence) = @_;
    
    # Check for invalid characters first
    my $valid_chars = ($datatype eq 'DNA2') ? 'ACGTUacgtu' : 'ACGTUMRWSYKVHDBNacgtumrwsykvhdbn';
    my $invalid_pattern = ($datatype eq 'DNA2') ? '[^ACGTUacgtu]+' : '[^ACGTUMRWSYKVHDBNacgtumrwsykvhdbn]+';
    
    # Check if sequence contains invalid characters
    if ($sequence =~ /$invalid_pattern/) {
        # Contains invalid characters, need to cut at invalid positions
        return split_long_sequence_positions($sequence_length, $sequence);
    }
    
    # No invalid characters, check if splitting is needed
    if ($sequence_length <= $minsplitlen * 2) {
        # Sequence is short enough and has no invalid characters, don't split
        return ({
            start => 1,
            end => $sequence_length
        });
    } else {
        # Split long sequence
        return split_long_sequence_positions($sequence_length, $sequence);
    }
}

sub split_long_sequence_positions {
    my ($seq_len, $sequence) = @_;
    
    my @fragments = ();
    
    # First, identify valid segments by cutting at invalid characters
    my $valid_chars = ($datatype eq 'DNA2') ? 'ACGTUacgtu' : 'ACGTUMRWSYKVHDBNacgtumrwsykvhdbn';
    my @valid_segments = ();
    
    if (defined $sequence) {
        # Find valid segments by scanning the sequence
        my $current_start = 1;
        my $current_end = 0;
        
        for my $i (0 .. length($sequence) - 1) {
            my $char = substr($sequence, $i, 1);
            my $pos = $i + 1;  # 1-based position
            
            if (index($valid_chars, $char) >= 0) {
                # Valid character
                if ($current_end == $pos - 1) {
                    # Continue current segment
                    $current_end = $pos;
                } else {
                    # Start new segment
                    if ($current_end > 0) {
                        # Save previous segment
                        push @valid_segments, {
                            start => $current_start,
                            end => $current_end
                        };
                    }
                    $current_start = $pos;
                    $current_end = $pos;
                }
            } else {
                # Invalid character - end current segment
                if ($current_end > 0) {
                    push @valid_segments, {
                        start => $current_start,
                        end => $current_end
                    };
                    $current_end = 0;
                }
            }
        }
        
        # Add final segment
        if ($current_end > 0) {
            push @valid_segments, {
                start => $current_start,
                end => $current_end
            };
        }
    } else {
        # No sequence provided, assume entire sequence is valid
        push @valid_segments, {
            start => 1,
            end => $seq_len
        };
    }
    
    # Apply overlapping splitting to each valid segment
    for my $segment (@valid_segments) {
        my $seg_start = $segment->{start};
        my $seg_end = $segment->{end};
        my $seg_len = $seg_end - $seg_start + 1;
        
        # Split long segment with overlap using calcsegment2.pl algorithm
        my $nsplit = int(($seg_len - $ovllen) / ($minsplitlen - $ovllen));
        
        if ($nsplit < 1) {
            # Cannot split, treat as single fragment
            push @fragments, {
                start => $seg_start,
                end => $seg_end
            };
        } else {
            # Calculate fragment distribution
            my $L = $seg_len - ($nsplit - 1) * $ovllen;
            my $q = int($L / $nsplit);
            my $r = $L % $nsplit;
            
            my $pos = 0;
            for my $i (0 .. $nsplit - 1) {
                my $len = $q + ($i < $r ? 1 : 0);
                $len += $ovllen if $i != $nsplit - 1;
                
                my $fragment_start = $seg_start + $pos;
                my $fragment_end = $seg_start + $pos + $len - 1;
                
                push @fragments, {
                    start => $fragment_start,
                    end => $fragment_end
                };
                
                $pos += $len - $ovllen;
            }
        }
    }
    
    return @fragments;
}

sub insert_sequence_fragment {
    my ($sequence, $fragments, $accessions, $dbh) = @_;
    
    # Process each fragment position
    for my $fragment (@$fragments) {
        my $start = $fragment->{start};
        my $end = $fragment->{end};
        
        # Extract fragment sequence from main sequence
        my $fragment_seq = substr($sequence, $start - 1, $end - $start + 1);
        
        # Check fragment length filter
        if ($minlen > 0 && length($fragment_seq) < $minlen) {
            print STDERR "Skipping fragment: length " . length($fragment_seq) . " is below minimum length $minlen\n" if $verbose;
            next;
        }
        
        # Build seqids for all accessions
        my @seqids = ();
        for my $accession (@$accessions) {
            my $seqid = sprintf("%s:%d:%d", $accession, $start, $end);
            push @seqids, $seqid;
        }
        
        # Simple INSERT for legacy compatibility (not used in new workflow)
        eval {
            my $sth = $dbh->prepare("INSERT INTO kafsss_data (seq, subset, seqid) VALUES (?, ?, ?)");
            $sth->execute($fragment_seq, $subset_array, \@seqids);
            $sth->finish();
        };
        
        if ($@) {
            print STDERR "Error inserting sequence fragment: $@\n";
        }
    }
}

sub ensure_table_exists {
    my ($dbh, $table_name) = @_;
    
    my $exists = $dbh->selectrow_array(
        "SELECT 1 FROM information_schema.tables WHERE table_name = ?",
        undef, $table_name
    );
    
    unless ($exists) {
        die "Critical error: Table '$table_name' does not exist\n";
    }
    
    print "Table '$table_name' verified to exist.\n";
}


sub update_meta_statistics {
    my ($dbh) = @_;

    print "Attempting to update statistics...\n";

    # Advisory Lock key for statistics update coordination
    my $advisory_lock_key = 1952867187;  # Arbitrary unique key for kafssstore stats update
    my $my_pid = $dbh->selectrow_array("SELECT pg_backend_pid()");

    # Try to acquire advisory lock (non-blocking first)
    my ($lock_acquired) = $dbh->selectrow_array(
        "SELECT pg_try_advisory_lock(?)", undef, $advisory_lock_key
    );

    unless ($lock_acquired) {
        # Another process has the lock - wait in queue for handoff
        print "Waiting in queue for statistics update role...\n";
        $dbh->do("SELECT pg_advisory_lock(?)", undef, $advisory_lock_key);
        print "Received statistics update role.\n";
    }

    # Now we have the lock - check if we are the last process
    # Match application_name pattern 'kafssstore:<dbname>' and same database
    my $app_name_pattern = "kafssstore:$output_db";
    my ($other_count) = $dbh->selectrow_array(
        "SELECT COUNT(*) FROM pg_stat_activity WHERE application_name = ? AND datname = current_database() AND pid != ?",
        undef, $app_name_pattern, $my_pid
    );

    if ($other_count > 0) {
        # Other processes still exist - hand off the role and exit
        print "Handing off statistics update role to next process ($other_count remaining).\n";
        $dbh->do("SELECT pg_advisory_unlock(?)", undef, $advisory_lock_key);
        return;
    }

    # We are the last process - perform statistics update
    print "This is the last process. Performing statistics update.\n";

    # Acquire table lock for statistics update
    eval {
        $dbh->begin_work;
        $dbh->do("LOCK TABLE kafsss_data IN ACCESS EXCLUSIVE MODE");
    };
    if ($@) {
        print STDERR "Failed to acquire table lock: $@\n";
        eval { $dbh->rollback; };
        $dbh->do("SELECT pg_advisory_unlock(?)", undef, $advisory_lock_key);
        return;
    }

    print "Exclusive lock acquired. Calculating total sequence statistics...\n";

    # Calculate total number of sequences and total bases using accurate nuc_length() function
    my $sth = $dbh->prepare(<<SQL);
SELECT
    COUNT(*) as nseq,
    SUM(nuc_length(seq)) as total_nchar
FROM kafsss_data
SQL

    $sth->execute();
    my ($nseq, $nchar) = $sth->fetchrow_array();
    $sth->finish();

    print "Total sequences: $nseq, Total bases: $nchar\n";

    # Calculate subset-specific statistics with single query
    print "Calculating subset-specific statistics...\n";

    $sth = $dbh->prepare(<<SQL);
SELECT
    subset_name,
    COUNT(*) AS nseq,
    SUM(nuc_length(seq)) AS total_nchar
FROM (
    SELECT unnest(subset) AS subset_name, seq
    FROM kafsss_data
    WHERE subset IS NOT NULL AND array_length(subset, 1) > 0
) AS unnested_subsets
GROUP BY subset_name
SQL

    $sth->execute();
    my %subset_stats = ();

    while (my ($subset, $subset_nseq, $subset_nchar) = $sth->fetchrow_array()) {
        $subset_stats{$subset} = {
            nseq => $subset_nseq,
            nchar => $subset_nchar
        };

        print "  Subset '$subset': $subset_nseq sequences, $subset_nchar bases\n";
    }
    $sth->finish();

    # Prepare subset statistics JSON
    my $subset_json = encode_json(\%subset_stats);

    # Update kafsss_meta table
    print "Updating kafsss_meta table with statistics...\n";

    $sth = $dbh->prepare(<<SQL);
UPDATE kafsss_meta
SET nseq = ?, nchar = ?, subset = ?
SQL

    eval {
        $sth->execute($nseq, $nchar, $subset_json);
        $dbh->commit;
        print "Transaction committed successfully for statistics update.\n";
    };

    if ($@) {
        print STDERR "Error updating kafsss_meta statistics: $@\n";
        eval { $dbh->rollback; };
        $sth->finish();
        # Release advisory lock before dying
        $dbh->do("SELECT pg_advisory_unlock(?)", undef, $advisory_lock_key);
        die "Statistics update failed: $@\n";
    }

    $sth->finish();

    # Release advisory lock
    $dbh->do("SELECT pg_advisory_unlock(?)", undef, $advisory_lock_key);

    print "Statistics update completed.\n";
}

sub configure_table_compression {
    my ($dbh) = @_;
    
    print "Configuring table compression: $compress\n";
    
    # Configure compression for seq, subset, and seqid columns
    my @columns = ('seq', 'subset', 'seqid');
    
    if ($compress eq 'lz4') {
        # Enable lz4 compression
        for my $column (@columns) {
            eval {
                $dbh->do("ALTER TABLE kafsss_data ALTER COLUMN $column SET STORAGE EXTENDED");
                $dbh->do("ALTER TABLE kafsss_data ALTER COLUMN $column SET COMPRESSION lz4");
            };
            if ($@) {
                print STDERR "Warning: Failed to set lz4 compression for column '$column': $@\n";
            }
        }
        print "LZ4 compression enabled for kafsss_data table columns.\n";
        
    } elsif ($compress eq 'pglz') {
        # Enable pglz compression
        for my $column (@columns) {
            eval {
                $dbh->do("ALTER TABLE kafsss_data ALTER COLUMN $column SET STORAGE EXTENDED");
                $dbh->do("ALTER TABLE kafsss_data ALTER COLUMN $column SET COMPRESSION pglz");
            };
            if ($@) {
                print STDERR "Warning: Failed to set pglz compression for column '$column': $@\n";
            }
        }
        print "PGLZ compression enabled for kafsss_data table columns.\n";
        
    } elsif ($compress eq 'disable') {
        # Disable compression
        for my $column (@columns) {
            eval {
                $dbh->do("ALTER TABLE kafsss_data ALTER COLUMN $column SET STORAGE EXTERNAL");
            };
            if ($@) {
                print STDERR "Warning: Failed to disable compression for column '$column': $@\n";
            }
        }
        print "Compression disabled for kafsss_data table columns.\n";
    }
}

sub expand_input_files {
    my (@patterns) = @_;
    my @files = ();
    
    for my $pattern (@patterns) {
        # Handle standard input
        if ($pattern eq '-' || $pattern eq 'stdin' || $pattern eq 'STDIN') {
            push @files, $pattern;
            next;
        }
        
        # Expand glob pattern
        my @expanded = glob($pattern);
        
        if (@expanded) {
            # Files found by glob expansion
            push @files, @expanded;
        } else {
            # No files found, check if it's a BLAST database or missing file
            if (!-f $pattern && (-f "$pattern.nsq" || -f "$pattern.nal")) {
                # BLAST database detected
                push @files, $pattern;
            } elsif (-f $pattern) {
                # Single file exists
                push @files, $pattern;
            } else {
                # File not found
                die "Input file or pattern '$pattern' not found\n";
            }
        }
    }
    
    return @files;
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
    
    # Check CREATE DATABASE permission
    $sth = $dbh->prepare("SELECT usesuper, usecreatedb FROM pg_user WHERE usename = ?");
    $sth->execute($username);
    my ($is_superuser, $can_create_db) = $sth->fetchrow_array();
    $sth->finish();
    
    unless ($is_superuser || $can_create_db) {
        die "Error: User '$username' does not have CREATE DATABASE permission.\n" .
            "Please grant the permission:\n" .
            "  sudo -u postgres psql\n" .
            "  ALTER USER $username CREATEDB;\n" .
            "  \\q\n";
    }
    
    print "User validation completed.\n" if $verbose;
}

sub check_and_create_extension {
    my ($dbh, $extension_name) = @_;
    
    print "Checking pg_kmersearch extension...\n";
    
    # Check if extension already exists
    my $sth = $dbh->prepare("SELECT 1 FROM pg_extension WHERE extname = ?");
    $sth->execute($extension_name);
    my $ext_exists = $sth->fetchrow_array();
    $sth->finish();
    
    if ($ext_exists) {
        print "Extension '$extension_name' already exists.\n";
        return;
    }
    
    # Check if extension is available
    $sth = $dbh->prepare("SELECT 1 FROM pg_available_extensions WHERE name = ?");
    $sth->execute($extension_name);
    my $ext_available = $sth->fetchrow_array();
    $sth->finish();
    
    unless ($ext_available) {
        die "Error: Extension '$extension_name' is not available.\n" .
            "Please install the pg_kmersearch extension:\n" .
            "  1. Install the extension package\n" .
            "  2. Restart PostgreSQL service\n" .
            "  3. Or ask your database administrator to install it\n";
    }
    
    # Check if user has permission to create extensions
    $sth = $dbh->prepare("SELECT usesuper FROM pg_user WHERE usename = CURRENT_USER");
    $sth->execute();
    my ($is_superuser) = $sth->fetchrow_array();
    $sth->finish();
    
    unless ($is_superuser) {
        die "Error: Current user does not have permission to create extensions.\n" .
            "Please have a PostgreSQL superuser create the extension:\n" .
            "  sudo -u postgres psql -d " . $dbh->{pg_db} . "\n" .
            "  CREATE EXTENSION IF NOT EXISTS $extension_name;\n" .
            "  \\q\n" .
            "Or grant superuser permission temporarily:\n" .
            "  sudo -u postgres psql\n" .
            "  ALTER USER " . $dbh->{pg_user} . " SUPERUSER;\n" .
            "  \\q\n" .
            "  (Remember to revoke after use: ALTER USER " . $dbh->{pg_user} . " NOSUPERUSER;)\n";
    }
    
    # Try to create the extension
    eval {
        $dbh->do("CREATE EXTENSION IF NOT EXISTS $extension_name");
        print "Extension '$extension_name' created successfully.\n";
    };
    if ($@) {
        die "Error: Failed to create extension '$extension_name': $@\n" .
            "Please contact your database administrator.\n";
    }
}

sub open_input_file {
    my ($filename) = @_;
    
    # Handle standard input
    if ($filename eq '-' || $filename eq 'stdin' || $filename eq 'STDIN') {
        return \*STDIN;
    }
    
    # Check for BLAST database
    if (!-f $filename && (-f "$filename.nsq" || -f "$filename.nal")) {
        # Test blastdbcmd availability and version
        my $version_output = `blastdbcmd -version 2>&1`;
        my $exit_code = $? >> 8;
        
        if ($exit_code != 0 || $version_output !~ /blastdbcmd:\s+[\d\.]+/) {
            die "blastdbcmd is not available or not working properly.\n" .
                "Version check failed: $version_output\n" .
                "Please install BLAST+ tools or check PATH.\n";
        }
        
        print "Using blastdbcmd for BLAST database: $filename\n" if $verbose;
        
        # BLAST nucleotide database
        open my $fh, '-|', 'blastdbcmd', '-db', $filename, '-dbtype', 'nucl', '-entry', 'all', '-out', '-', '-outfmt', ">\%a\n\%s", '-line_length', '1000000000', '-ctrl_a', '-get_dups' or die "Cannot open BLAST database '$filename': $!\n";
        return $fh;
    }
    
    # Check file existence
    die "Input file '$filename' does not exist\n" unless -f $filename;
    
    # Determine file type and open accordingly
    if ($filename =~ /\.gz$/i) {
        # gzip compressed file
        open my $fh, '-|', 'pigz', '-dc', $filename or die "Cannot open gzip file '$filename': $!\n";
        return $fh;
    } elsif ($filename =~ /\.bz2$/i) {
        # bzip2 compressed file
        open my $fh, '-|', 'pbzip2', '-dc', $filename or die "Cannot open bzip2 file '$filename': $!\n";
        return $fh;
    } elsif ($filename =~ /\.xz$/i) {
        # xz compressed file
        open my $fh, '-|', 'xz', '-dc', $filename or die "Cannot open xz file '$filename': $!\n";
        return $fh;
    } elsif ($filename =~ /\.(zst|zstd)$/i) {
        # zstd compressed file
        open my $fh, '-|', 'zstd', '-dc', $filename or die "Cannot open zstd file '$filename': $!\n";
        return $fh;
    } else {
        # Regular file
        open my $fh, '<', $filename or die "Cannot open file '$filename': $!\n";
        return $fh;
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