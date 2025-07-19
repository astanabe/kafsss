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
my $default_numthreads = 1;
my $default_batchsize = 1000000;

# Command line options
my $host = $default_host;
my $port = $default_port;
my $username = $default_user;
my $numthreads = $default_numthreads;
my $batchsize = $default_batchsize;
my @partitions = ();
my $mode = 'add';
my $help = 0;

# Parse command line options
GetOptions(
    'host=s' => \$host,
    'port=i' => \$port,
    'username=s' => \$username,
    'numthreads=i' => \$numthreads,
    'batchsize=i' => \$batchsize,
    'partition=s' => \@partitions,
    'mode=s' => \$mode,
    'help|h' => \$help,
) or die "Error in command line arguments\n";

# Show help
if ($help) {
    print_help();
    exit 0;
}

# Check required arguments
if (@ARGV != 2) {
    die "Usage: af_kmerpart [options] input_file database_name\n" .
        "Use --help for detailed usage information.\n";
}

my ($input_file, $database_name) = @ARGV;

# Validate input file
unless ($input_file eq '-' || $input_file eq 'stdin' || $input_file eq 'STDIN' || $input_file eq 'all') {
    die "Input file '$input_file' does not exist\n" unless -f $input_file;
}

# Validate required options
die "Partition name must be specified with --partition option\n" unless @partitions;

# Validate mode option
die "Mode must be 'add' or 'del'\n" unless $mode eq 'add' || $mode eq 'del';

# Validate numthreads
die "numthreads must be positive integer\n" unless $numthreads > 0;

# Validate batchsize
die "batchsize must be positive integer\n" unless $batchsize > 0;

# Parse partitions from comma-separated values
my @all_partitions = ();
for my $partition_spec (@partitions) {
    push @all_partitions, split(/,/, $partition_spec);
}

# Remove duplicates
my %seen = ();
@all_partitions = grep { !$seen{$_}++ } @all_partitions;

# Validate partition names
for my $partition (@all_partitions) {
    if ($mode eq 'add' && $partition eq 'all') {
        die "Partition name 'all' is not allowed in add mode\n";
    }
}

print "af_kmerpart version $VERSION\n";
print "Mode: $mode\n";
print "Input file: $input_file\n";
print "Database: $database_name\n";
print "Host: $host\n";
print "Port: $port\n";
print "Username: $username\n";
print "Number of threads: $numthreads\n";
print "Batch size: $batchsize\n";
print "Partitions: " . join(', ', @all_partitions) . "\n";

# Connect to PostgreSQL server for validation
my $password = $ENV{PGPASSWORD} || '';
my $server_dsn = "DBI:Pg:dbname=postgres;host=$host;port=$port";

my $server_dbh = DBI->connect($server_dsn, $username, $password, {
    RaiseError => 1,
    AutoCommit => 1,
    pg_enable_utf8 => 1
}) or die "Cannot connect to PostgreSQL server: $DBI::errstr\n";

# Validate user and database existence
validate_user_and_permissions($server_dbh, $username);
check_database_exists($server_dbh, $database_name);
$server_dbh->disconnect();

# Connect to target database
my $dsn = "DBI:Pg:dbname=$database_name;host=$host;port=$port";

my $dbh = DBI->connect($dsn, $username, $password, {
    RaiseError => 1,
    AutoCommit => 1,
    pg_enable_utf8 => 1
}) or die "Cannot connect to database '$database_name': $DBI::errstr\n";

# Validate database permissions and schema
validate_database_permissions($dbh, $username);
validate_database_schema($dbh);

# Verify database structure
verify_database_structure($dbh);

# Check meta table compatibility  
check_meta_table_compatibility($dbh);

# Acquire advisory lock for exclusive access to prevent conflicts with other tools
print "Acquiring exclusive lock...\n";
eval {
    $dbh->do("SELECT pg_advisory_xact_lock(999)");
    print "Exclusive lock acquired.\n";
};
if ($@) {
    die "Failed to acquire advisory lock: $@\n";
}

# Disconnect database connection before parallel processing to avoid connection sharing
$dbh->disconnect();
print "Database connection closed before parallel processing.\n";

# Process accession numbers with memory-efficient streaming
if ($input_file eq 'all') {
    print "Processing all rows in database...\n";
    process_all_rows();
} else {
    print "Processing accession numbers from input file...\n";
    process_accessions_batch_streaming($input_file);
}

# Update statistics in af_kmersearch_meta table
print "Updating statistics in af_kmersearch_meta table...\n";
update_meta_statistics($database_name);

# Advisory lock is automatically released when the original connection was closed
print "Exclusive lock released.\n";

print "Processing completed successfully.\n";

exit 0;

#
# Subroutines
#

sub print_help {
    print <<EOF;
af_kmerpart version $VERSION

Usage: af_kmerpart [options] input_file database_name

Update partition information in af_kmersearch database for specified accession numbers.

Required arguments:
  input_file        Input file with accession numbers (one per line), or 'all' for all rows
                    (use '-', 'stdin', or 'STDIN' for standard input)
  database_name     PostgreSQL database name

Required options:
  --partition=NAME  Partition name to add/remove (can be specified multiple times or comma-separated)
                    Use 'all' to target all partitions (only in del mode)

Other options:
  --mode=MODE       Operation mode: 'add' (default) or 'del'
  --host=HOST       PostgreSQL server host (default: \$PGHOST or localhost)
  --port=PORT       PostgreSQL server port (default: \$PGPORT or 5432)
  --username=USER   PostgreSQL username (default: \$PGUSER or current user)
  --numthreads=INT  Number of parallel threads (default: 1)
  --batchsize=INT   Batch size for processing (default: 1000000)
  --help, -h        Show this help message

Environment variables:
  PGHOST           PostgreSQL server host
  PGPORT           PostgreSQL server port
  PGUSER           PostgreSQL username
  PGPASSWORD       PostgreSQL password

Examples:
  # Add partitions
  af_kmerpart --partition=bacteria accessions.txt mydb
  af_kmerpart --partition=bacteria,archaea accessions.txt mydb
  af_kmerpart --numthreads=4 --partition=viruses accessions.txt mydb
  
  # Remove partitions
  af_kmerpart --mode=del --partition=bacteria accessions.txt mydb
  af_kmerpart --mode=del --partition=archaea all mydb
  af_kmerpart --mode=del --partition=all all mydb
  
  # Standard input
  echo -e "AB123456\nCD789012" | af_kmerpart --partition=bacteria stdin mydb

EOF
}

sub process_accessions_batch_streaming {
    my ($filename) = @_;
    
    # Open input file
    my $fh;
    if ($filename eq '-' || $filename eq 'stdin' || $filename eq 'STDIN') {
        $fh = \*STDIN;
    } else {
        open $fh, '<', $filename or die "Cannot open file '$filename': $!\n";
    }
    
    my @batch = ();
    my $total_processed = 0;
    my @active_children = ();
    
    while (my $line = <$fh>) {
        chomp $line;
        $line =~ s/^\s+|\s+$//g;  # Trim whitespace
        
        next if $line eq '';  # Skip empty lines
        next if $line =~ /^#/;  # Skip comment lines
        
        push @batch, $line;
        
        # Process batch when it reaches the specified size
        if (scalar(@batch) >= $batchsize) {
            # Wait for available slot if we have reached max threads
            while (scalar(@active_children) >= $numthreads) {
                wait_for_child(\@active_children);
            }
            
            # Process this batch
            my $pid = process_batch(\@batch);
            if ($pid > 0) {
                push @active_children, $pid;
            }
            
            $total_processed += scalar(@batch);
            print "Processed batch: " . scalar(@batch) . " items (total: $total_processed)\n";
            
            @batch = ();  # Clear batch
        }
    }
    
    # Process remaining items in the last batch
    if (@batch) {
        # Wait for available slot if we have reached max threads
        while (scalar(@active_children) >= $numthreads) {
            wait_for_child(\@active_children);
        }
        
        my $pid = process_batch(\@batch);
        if ($pid > 0) {
            push @active_children, $pid;
        }
        
        $total_processed += scalar(@batch);
        print "Processed final batch: " . scalar(@batch) . " items (total: $total_processed)\n";
    }
    
    # Wait for all remaining children to complete
    while (@active_children) {
        wait_for_child(\@active_children);
    }
    
    unless ($filename eq '-' || $filename eq 'stdin' || $filename eq 'STDIN') {
        close $fh;
    }
    
    print "Total accessions processed: $total_processed\n";
}

sub process_batch {
    my ($batch_ref) = @_;
    
    if ($numthreads == 1) {
        # Single-threaded: process directly in current process
        process_batch_items($batch_ref);
        return 0;  # No child process created
    } else {
        # Multi-threaded: fork a child process
        my $pid = fork();
        
        if (!defined $pid) {
            die "Cannot fork: $!\n";
        } elsif ($pid == 0) {
            # Child process
            process_batch_items($batch_ref);
            exit 0;
        } else {
            # Parent process
            return $pid;
        }
    }
}

sub process_batch_items {
    my ($batch_ref) = @_;
    
    # Create new database connection for child process
    my $password = $ENV{PGPASSWORD} || '';
    my $child_dsn = "DBI:Pg:dbname=$database_name;host=$host;port=$port";
                
    my $child_dbh = DBI->connect($child_dsn, $username, $password, {
        RaiseError => 1,
        AutoCommit => 1,
        pg_enable_utf8 => 1
    }) or die "Cannot connect to database in child process: $DBI::errstr\n";
    
    eval {
        $child_dbh->begin_work;
        
        # Process all items in this batch
        for my $accession (@$batch_ref) {
            process_single_accession($accession, $child_dbh);
        }
        
        $child_dbh->commit;
        print "Batch of " . scalar(@$batch_ref) . " items committed successfully\n";
    };
    
    if ($@) {
        eval { $child_dbh->rollback; };
        die "Error processing batch: $@\n";
    }
    
    $child_dbh->disconnect;
}

sub wait_for_child {
    my ($active_children_ref) = @_;
    
    my $pid = wait();
    if ($pid > 0) {
        # Remove completed child from active list
        @$active_children_ref = grep { $_ != $pid } @$active_children_ref;
        
        my $exit_status = $? >> 8;
        if ($exit_status != 0) {
            print STDERR "Child process $pid exited with status $exit_status\n";
        }
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
    
    unless ($result) {
        die "Error: Database '$dbname' does not exist.\n" .
            "Please create the database first using af_kmerstore.\n";
    }
    
    print "Database '$dbname' exists.\n";
}

sub validate_database_permissions {
    my ($dbh, $username) = @_;
    
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
    
    # Check table permissions - af_kmerpart needs SELECT, INSERT, UPDATE, DELETE
    $sth = $dbh->prepare("SELECT has_table_privilege(?, 'af_kmersearch', 'SELECT, INSERT, UPDATE, DELETE')");
    $sth->execute($username);
    my $has_table_perm = $sth->fetchrow_array();
    $sth->finish();
    
    unless ($has_table_perm) {
        die "Error: User '$username' does not have sufficient permissions on af_kmersearch table.\n" .
            "Please grant permissions:\n" .
            "  sudo -u postgres psql -d " . $dbh->{pg_db} . "\n" .
            "  GRANT SELECT, INSERT, UPDATE, DELETE ON af_kmersearch TO $username;\n" .
            "  \\q\n";
    }
    
    print "Database permissions validation completed.\n";
}

sub validate_database_schema {
    my ($dbh) = @_;
    
    print "Validating database schema...\n";
    
    # Check if required tables exist
    my @required_tables = ('af_kmersearch_meta', 'af_kmersearch');
    
    for my $table (@required_tables) {
        my $sth = $dbh->prepare("SELECT 1 FROM information_schema.tables WHERE table_name = ?");
        $sth->execute($table);
        my $table_exists = $sth->fetchrow_array();
        $sth->finish();
        
        unless ($table_exists) {
            die "Error: Required table '$table' does not exist in database.\n" .
                "This database may not have been created with af_kmerstore.\n" .
                "Please create the database properly using af_kmerstore first.\n";
        }
    }
    
    print "Database schema validation completed.\n";
}

sub verify_database_structure {
    my ($dbh) = @_;
    
    # Check if af_kmersearch table exists
    my $sth = $dbh->prepare(<<SQL);
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
SELECT column_name, CASE WHEN data_type = 'USER-DEFINED' THEN udt_name ELSE data_type END AS data_type
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

sub check_meta_table_compatibility {
    my ($dbh) = @_;
    
    print "Checking af_kmersearch_meta table compatibility...\n";
    
    # Check if af_kmersearch_meta table exists
    my $sth = $dbh->prepare(<<SQL);
SELECT COUNT(*)
FROM information_schema.tables 
WHERE table_name = 'af_kmersearch_meta'
SQL
    $sth->execute();
    my ($meta_table_count) = $sth->fetchrow_array();
    $sth->finish();
    
    die "Table 'af_kmersearch_meta' does not exist in database '$database_name'\n" 
        unless $meta_table_count > 0;
    
    # Check if new columns exist (added by updated af_kmerstore)
    $sth = $dbh->prepare(<<SQL);
SELECT column_name
FROM information_schema.columns 
WHERE table_name = 'af_kmersearch_meta'
AND column_name IN ('kmer_size', 'occur_bitlen', 'max_appearance_rate', 'max_appearance_nrow')
ORDER BY column_name
SQL
    $sth->execute();
    
    my @new_columns = ();
    while (my ($col) = $sth->fetchrow_array()) {
        push @new_columns, $col;
    }
    $sth->finish();
    
    if (@new_columns > 0) {
        print "Found new meta columns: " . join(', ', @new_columns) . "\n";
        print "Database appears to be updated for new pg_kmersearch specification.\n";
    } else {
        print "Warning: New meta columns not found. Database may be from older version.\n";
        print "Consider updating with latest af_kmerstore\n";
    }
    
    print "Meta table compatibility check completed.\n";
}

sub process_accessions_single_threaded {
    my ($accessions) = @_;
    
    # Connect to database
    my $password = $ENV{PGPASSWORD} || '';
    my $dsn = "DBI:Pg:dbname=$database_name;host=$host;port=$port";
        
    my $dbh = DBI->connect($dsn, $username, $password, {
        RaiseError => 1,
        AutoCommit => 1,
        pg_enable_utf8 => 1
    }) or die "Cannot connect to database '$database_name': $DBI::errstr\n";
    
    eval {
        $dbh->begin_work;
        for my $accession (@$accessions) {
            process_single_accession($accession, $dbh);
        }
        $dbh->commit;
        print "Transaction committed successfully for batch processing.\n";
    };
    
    if ($@) {
        print STDERR "Error during batch processing: $@\n";
        eval { $dbh->rollback; };
        $dbh->disconnect();
        die "Batch processing failed: $@\n";
    }
    
    $dbh->disconnect();
}


sub process_single_accession {
    my ($accession, $dbh) = @_;
    
    # Remove version number from accession
    my $clean_accession = remove_version_number($accession);
    
    if ($mode eq 'add') {
        # Add partitions: combine existing partitions with new ones
        my $update_sth = $dbh->prepare(<<SQL);
UPDATE af_kmersearch 
SET part = (
    SELECT array_agg(DISTINCT e) 
    FROM unnest(part || ?) AS e
) 
WHERE EXISTS (
    SELECT 1 
    FROM unnest(seqid) AS s 
    WHERE split_part(s, ':', 1) = ?
)
SQL
        
        eval {
            my $rows_updated = $update_sth->execute(\@all_partitions, $clean_accession);
            
            if ($rows_updated && $rows_updated > 0) {
                print "Added partitions to $rows_updated rows for accession '$accession' (cleaned: '$clean_accession')\n";
            } else {
                print "No rows found for accession '$accession' (cleaned: '$clean_accession')\n";
            }
        };
        
        if ($@) {
            print STDERR "Error processing accession '$accession': $@\n";
        }
        
        $update_sth->finish();
        
    } elsif ($mode eq 'del') {
        # Remove partitions
        if (grep { $_ eq 'all' } @all_partitions) {
            # Remove all partitions (set part to empty array)
            my $update_sth = $dbh->prepare(<<SQL);
UPDATE af_kmersearch 
SET part = '{}'::text[]
WHERE EXISTS (
    SELECT 1 
    FROM unnest(seqid) AS s 
    WHERE split_part(s, ':', 1) = ?
)
SQL
            
            eval {
                my $rows_updated = $update_sth->execute($clean_accession);
                
                if ($rows_updated && $rows_updated > 0) {
                    print "Removed all partitions from $rows_updated rows for accession '$accession' (cleaned: '$clean_accession')\n";
                } else {
                    print "No rows found for accession '$accession' (cleaned: '$clean_accession')\n";
                }
            };
            
            if ($@) {
                print STDERR "Error processing accession '$accession': $@\n";
            }
            
            $update_sth->finish();
            
        } else {
            # Remove specific partitions
            my $update_sth = $dbh->prepare(<<SQL);
UPDATE af_kmersearch 
SET part = (
    SELECT array_agg(e) 
    FROM unnest(part) AS e 
    WHERE e != ALL(?)
) 
WHERE EXISTS (
    SELECT 1 
    FROM unnest(seqid) AS s 
    WHERE split_part(s, ':', 1) = ?
)
SQL
            
            eval {
                my $rows_updated = $update_sth->execute(\@all_partitions, $clean_accession);
                
                if ($rows_updated && $rows_updated > 0) {
                    print "Removed specified partitions from $rows_updated rows for accession '$accession' (cleaned: '$clean_accession')\n";
                } else {
                    print "No rows found for accession '$accession' (cleaned: '$clean_accession')\n";
                }
            };
            
            if ($@) {
                print STDERR "Error processing accession '$accession': $@\n";
            }
            
            $update_sth->finish();
        }
    }
}

sub remove_version_number {
    my ($accession) = @_;
    
    # Remove version number (e.g., .1, .2, etc.) from the end
    $accession =~ s/\.\d+$//;
    
    return $accession;
}

sub process_all_rows {
    # Process all rows in the database using batch processing with threading
    # Note: This function assumes database connection was already closed before parallel processing
    
    # Create temporary connection to get total row count
    my $password = $ENV{PGPASSWORD} || '';
    my $dsn = "DBI:Pg:dbname=$database_name;host=$host;port=$port";
    
    my $temp_dbh = DBI->connect($dsn, $username, $password, {
        RaiseError => 1,
        AutoCommit => 1,
        pg_enable_utf8 => 1
    }) or die "Cannot connect to database '$database_name': $DBI::errstr\n";
    
    # Get total number of rows for progress tracking
    my $sth = $temp_dbh->prepare("SELECT COUNT(*) FROM af_kmersearch");
    $sth->execute();
    my ($total_rows) = $sth->fetchrow_array();
    $sth->finish();
    
    print "Total rows to process: $total_rows\n";
    
    # Close temporary connection before starting parallel processing
    $temp_dbh->disconnect();
    print "Temporary database connection closed before batch processing.\n";
    
    # Process all rows in batches
    my $offset = 0;
    my $total_processed = 0;
    my @active_children = ();
    
    while ($offset < $total_rows) {
        # Wait for available slot if we have reached max threads
        while (scalar(@active_children) >= $numthreads) {
            wait_for_child(\@active_children);
        }
        
        # Process this batch
        my $pid = process_all_rows_batch($offset, $batchsize);
        if ($pid > 0) {
            push @active_children, $pid;
        }
        
        my $batch_size = ($offset + $batchsize <= $total_rows) ? $batchsize : ($total_rows - $offset);
        $total_processed += $batch_size;
        $offset += $batchsize;
        
        print "Processed batch: $batch_size items (total: $total_processed/$total_rows)\n";
    }
    
    # Wait for all remaining children to complete
    while (@active_children) {
        wait_for_child(\@active_children);
    }
    
    print "Total rows processed: $total_processed\n";
}

sub process_all_rows_batch {
    my ($offset, $limit) = @_;
    
    if ($numthreads == 1) {
        # Single-threaded: process directly in current process
        process_all_rows_batch_items($offset, $limit);
        return 0;  # No child process created
    } else {
        # Multi-threaded: fork a child process
        my $pid = fork();
        
        if (!defined $pid) {
            die "Cannot fork: $!\n";
        } elsif ($pid == 0) {
            # Child process
            process_all_rows_batch_items($offset, $limit);
            exit 0;
        } else {
            # Parent process
            return $pid;
        }
    }
}

sub process_all_rows_batch_items {
    my ($offset, $limit) = @_;
    
    # Create new database connection for child process
    my $password = $ENV{PGPASSWORD} || '';
    my $child_dsn = "DBI:Pg:dbname=$database_name;host=$host;port=$port";
                
    my $child_dbh = DBI->connect($child_dsn, $username, $password, {
        RaiseError => 1,
        AutoCommit => 1,
        pg_enable_utf8 => 1
    }) or die "Cannot connect to database in child process: $DBI::errstr\n";
    
    eval {
        $child_dbh->begin_work;
        
        if ($mode eq 'add') {
            # Add partitions to all rows
            my $update_sth = $child_dbh->prepare(<<SQL);
UPDATE af_kmersearch 
SET part = (
    SELECT array_agg(DISTINCT e) 
    FROM unnest(part || ?) AS e
)
WHERE ctid IN (
    SELECT ctid FROM af_kmersearch 
    ORDER BY ctid 
    LIMIT ? OFFSET ?
)
SQL
            
            my $rows_updated = $update_sth->execute(\@all_partitions, $limit, $offset);
            print "Added partitions to $rows_updated rows in batch (offset: $offset)\n";
            $update_sth->finish();
            
        } elsif ($mode eq 'del') {
            if (grep { $_ eq 'all' } @all_partitions) {
                # Remove all partitions from all rows
                my $update_sth = $child_dbh->prepare(<<SQL);
UPDATE af_kmersearch 
SET part = '{}'::text[]
WHERE ctid IN (
    SELECT ctid FROM af_kmersearch 
    ORDER BY ctid 
    LIMIT ? OFFSET ?
)
SQL
                
                my $rows_updated = $update_sth->execute($limit, $offset);
                print "Removed all partitions from $rows_updated rows in batch (offset: $offset)\n";
                $update_sth->finish();
                
            } else {
                # Remove specific partitions from all rows
                my $update_sth = $child_dbh->prepare(<<SQL);
UPDATE af_kmersearch 
SET part = (
    SELECT array_agg(e) 
    FROM unnest(part) AS e 
    WHERE e != ALL(?)
)
WHERE ctid IN (
    SELECT ctid FROM af_kmersearch 
    ORDER BY ctid 
    LIMIT ? OFFSET ?
)
SQL
                
                my $rows_updated = $update_sth->execute(\@all_partitions, $limit, $offset);
                print "Removed specified partitions from $rows_updated rows in batch (offset: $offset)\n";
                $update_sth->finish();
            }
        }
        
        $child_dbh->commit;
        print "Batch committed successfully (offset: $offset, limit: $limit)\n";
    };
    
    if ($@) {
        eval { $child_dbh->rollback; };
        die "Error processing all rows batch: $@\n";
    }
    
    $child_dbh->disconnect;
}


sub update_meta_statistics {
    my ($database_name) = @_;
    
    # Parent process reconnects to database after parallel processing is complete
    my $password = $ENV{PGPASSWORD} || '';
    my $dsn = "DBI:Pg:dbname=$database_name;host=$host;port=$port";
        
    my $dbh = DBI->connect($dsn, $username, $password, {
        RaiseError => 1,
        AutoCommit => 1,
        pg_enable_utf8 => 1
    }) or die "Cannot connect to database '$database_name': $DBI::errstr\n";
    
    print "Calculating total sequence statistics...\n";
    
    # Get datatype from existing meta table to determine bit calculation
    my $sth = $dbh->prepare("SELECT column_name, CASE WHEN data_type = 'USER-DEFINED' THEN udt_name ELSE data_type END AS data_type FROM information_schema.columns WHERE table_name = 'af_kmersearch' AND column_name = 'seq'");
    $sth->execute();
    my ($col_name, $datatype) = $sth->fetchrow_array();
    $sth->finish();
    
    if (!$datatype) {
        die "Cannot determine datatype from af_kmersearch table\n";
    }
    
    # Calculate total number of sequences and total bases using accurate nuc_length() function
    $sth = $dbh->prepare(<<SQL);
SELECT 
    COUNT(*) as nseq,
    SUM(nuc_length(seq)) as total_nchar
FROM af_kmersearch
SQL
    
    $sth->execute();
    my ($nseq, $nchar) = $sth->fetchrow_array();
    $sth->finish();
    
    print "Total sequences: $nseq, Total bases: $nchar\n";
    
    # Calculate partition-specific statistics with single query
    print "Calculating partition-specific statistics...\n";
    
    $sth = $dbh->prepare(<<SQL);
SELECT 
    partition_name, 
    COUNT(*) AS nseq, 
    SUM(nuc_length(seq)) AS total_nchar 
FROM (
    SELECT unnest(part) AS partition_name, seq 
    FROM af_kmersearch 
    WHERE part IS NOT NULL AND array_length(part, 1) > 0
) AS unnested_parts 
GROUP BY partition_name
SQL
    
    $sth->execute();
    my %partition_stats = ();
    
    while (my ($partition, $part_nseq, $part_nchar) = $sth->fetchrow_array()) {
        $partition_stats{$partition} = {
            nseq => $part_nseq,
            nchar => $part_nchar
        };
        
        print "  Partition '$partition': $part_nseq sequences, $part_nchar bases\n";
    }
    $sth->finish();
    
    # Prepare partition statistics JSON
    my $part_json = encode_json(\%partition_stats);
    
    # Update af_kmersearch_meta table (preserve existing kmer-related columns)
    print "Updating af_kmersearch_meta table with statistics...\n";
    
    $sth = $dbh->prepare(<<SQL);
UPDATE af_kmersearch_meta 
SET nseq = ?, nchar = ?, part = ?
SQL
    
    eval {
        $dbh->begin_work;
        $sth->execute($nseq, $nchar, $part_json);
        $dbh->commit;
        print "Transaction committed successfully for statistics update.\n";
    };
    
    if ($@) {
        print STDERR "Error updating af_kmersearch_meta statistics: $@\n";
        eval { $dbh->rollback; };
        $sth->finish();
        $dbh->disconnect();
        die "Statistics update failed: $@\n";
    }
    
    $sth->finish();
    $dbh->disconnect();
    
    print "Statistics update completed.\n";
}

sub parse_pg_array {
    my ($pg_array_str) = @_;
    
    return () unless defined $pg_array_str;
    
    # Remove outer braces
    $pg_array_str =~ s/^\{//;
    $pg_array_str =~ s/\}$//;
    
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