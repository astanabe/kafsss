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

# Command line options
my $host = $default_host;
my $port = $default_port;
my $username = $default_user;
my $numthreads = $default_numthreads;
my @partitions = ();
my $help = 0;

# Parse command line options
GetOptions(
    'host=s' => \$host,
    'port=i' => \$port,
    'username=s' => \$username,
    'numthreads=i' => \$numthreads,
    'partition=s' => \@partitions,
    'help|h' => \$help,
) or die "Error in command line arguments\n";

# Show help
if ($help) {
    print_help();
    exit 0;
}

# Check required arguments
if (@ARGV != 2) {
    die "Usage: perl af_kmerpart.pl [options] input_file database_name\n" .
        "Use --help for detailed usage information.\n";
}

my ($input_file, $database_name) = @ARGV;

# Validate input file
unless ($input_file eq '-' || $input_file eq 'stdin' || $input_file eq 'STDIN') {
    die "Input file '$input_file' does not exist\n" unless -f $input_file;
}

# Validate required options
die "Partition name must be specified with --partition option\n" unless @partitions;

# Validate numthreads
die "numthreads must be positive integer\n" unless $numthreads > 0;

# Parse partitions from comma-separated values
my @all_partitions = ();
for my $partition_spec (@partitions) {
    push @all_partitions, split(/,/, $partition_spec);
}

# Remove duplicates
my %seen = ();
@all_partitions = grep { !$seen{$_}++ } @all_partitions;

print "af_kmerpart.pl version $VERSION\n";
print "Input file: $input_file\n";
print "Database: $database_name\n";
print "Host: $host\n";
print "Port: $port\n";
print "Username: $username\n";
print "Number of threads: $numthreads\n";
print "Partitions: " . join(', ', @all_partitions) . "\n";

# Connect to PostgreSQL database
my $password = $ENV{PGPASSWORD} || '';
my $dsn = "DBI:Pg:dbname=$database_name;host=$host;port=$port";

my $dbh = DBI->connect($dsn, $username, $password, {
    RaiseError => 1,
    AutoCommit => 1,
    pg_enable_utf8 => 1
}) or die "Cannot connect to database '$database_name': $DBI::errstr\n";

# Verify database structure
verify_database_structure($dbh);

# Acquire advisory lock for exclusive access to prevent conflicts with other tools
print "Acquiring exclusive lock...\n";
eval {
    $dbh->do("SELECT pg_advisory_xact_lock(999)");
    print "Exclusive lock acquired.\n";
};
if ($@) {
    die "Failed to acquire advisory lock: $@\n";
}

# Keep this connection open to maintain the lock during processing
my $lock_dbh = $dbh;

# Process accession numbers with memory-efficient streaming
print "Processing accession numbers from input file...\n";
if ($numthreads == 1) {
    # Single-threaded processing - stream one by one
    process_accessions_streaming($input_file);
} else {
    # Multi-threaded processing - need to batch accessions
    my @accession_numbers = read_accession_file($input_file);
    print "Read " . scalar(@accession_numbers) . " accession numbers.\n";
    process_accessions_parallel(\@accession_numbers);
}

# Update statistics in af_kmersearch_meta table
print "Updating statistics in af_kmersearch_meta table...\n";
update_meta_statistics($database_name);

# Release advisory lock
$lock_dbh->disconnect();
print "Exclusive lock released.\n";

print "Processing completed successfully.\n";

exit 0;

#
# Subroutines
#

sub print_help {
    print <<EOF;
af_kmerpart.pl version $VERSION

Usage: perl af_kmerpart.pl [options] input_file database_name

Update partition information in af_kmersearch database for specified accession numbers.

Required arguments:
  input_file        Input file with accession numbers (one per line) (use '-', 'stdin', or 'STDIN' for standard input)
  database_name     PostgreSQL database name

Required options:
  --partition=NAME  Partition name to add (can be specified multiple times or comma-separated)

Other options:
  --host=HOST       PostgreSQL server host (default: \$PGHOST or localhost)
  --port=PORT       PostgreSQL server port (default: \$PGPORT or 5432)
  --username=USER   PostgreSQL username (default: \$PGUSER or current user)
  --numthreads=INT  Number of parallel threads (default: 1)
  --help, -h        Show this help message

Environment variables:
  PGHOST           PostgreSQL server host
  PGPORT           PostgreSQL server port
  PGUSER           PostgreSQL username
  PGPASSWORD       PostgreSQL password

Examples:
  perl af_kmerpart.pl --partition=bacteria accessions.txt mydb
  perl af_kmerpart.pl --partition=bacteria,archaea accessions.txt mydb
  perl af_kmerpart.pl --partition=bacteria --partition=archaea accessions.txt mydb
  perl af_kmerpart.pl --numthreads=4 --partition=viruses accessions.txt mydb
  echo -e "AB123456\nCD789012" | perl af_kmerpart.pl --partition=bacteria stdin mydb

EOF
}

sub read_accession_file {
    my ($filename) = @_;
    
    my $fh;
    if ($filename eq '-' || $filename eq 'stdin' || $filename eq 'STDIN') {
        $fh = \*STDIN;
    } else {
        open $fh, '<', $filename or die "Cannot open file '$filename': $!\n";
    }
    
    my @accessions = ();
    
    while (my $line = <$fh>) {
        chomp $line;
        $line =~ s/^\s+|\s+$//g;  # Trim whitespace
        
        next if $line eq '';  # Skip empty lines
        next if $line =~ /^#/;  # Skip comment lines
        
        push @accessions, $line;
    }
    
    unless ($filename eq '-' || $filename eq 'stdin' || $filename eq 'STDIN') {
        close $fh;
    }
    
    return @accessions;
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
    
    # Check if required columns exist
    $sth = $dbh->prepare(<<SQL);
SELECT column_name
FROM information_schema.columns 
WHERE table_name = 'af_kmersearch'
AND column_name IN ('seq', 'part', 'seqid')
ORDER BY column_name
SQL
    $sth->execute();
    
    my @columns = ();
    while (my ($col) = $sth->fetchrow_array()) {
        push @columns, $col;
    }
    $sth->finish();
    
    die "Required columns (seq, part, seqid) not found in table 'af_kmersearch'\n"
        unless @columns == 3 && $columns[0] eq 'part' && $columns[1] eq 'seq' && $columns[2] eq 'seqid';
    
    print "Database structure verified.\n";
}

sub process_accessions_single_threaded {
    my ($accessions) = @_;
    
    # Connect to database
    my $password = $ENV{PGPASSWORD} || '';
    my $dsn = "DBI:Pg:dbname=$database_name;host=$host;port=$port";
        
    my $dbh = DBI->connect($dsn, $username, $password, {
        RaiseError => 1,
        AutoCommit => 0,
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

sub process_accessions_streaming {
    my ($filename) = @_;
    
    # Open input file
    my $fh;
    if ($filename eq '-' || $filename eq 'stdin' || $filename eq 'STDIN') {
        $fh = \*STDIN;
    } else {
        open $fh, '<', $filename or die "Cannot open file '$filename': $!\n";
    }
    
    # Connect to database
    my $password = $ENV{PGPASSWORD} || '';
    my $dsn = "DBI:Pg:dbname=$database_name;host=$host;port=$port";
        
    my $dbh = DBI->connect($dsn, $username, $password, {
        RaiseError => 1,
        AutoCommit => 0,
        pg_enable_utf8 => 1
    }) or die "Cannot connect to database '$database_name': $DBI::errstr\n";
    
    my $count = 0;
    eval {
        $dbh->begin_work;
        
        # Process accessions one by one as they are read
        while (my $line = <$fh>) {
            chomp $line;
            $line =~ s/^\s+|\s+$//g;  # Trim whitespace
            
            next if $line eq '';  # Skip empty lines
            next if $line =~ /^#/;  # Skip comment lines
            
            process_single_accession($line, $dbh);
            $count++;
            
            # Progress indicator for large files
            if ($count % 10000 == 0) {
                print "Processed $count accession numbers...\n";
            }
        }
        
        $dbh->commit;
        print "Transaction committed successfully for streaming processing.\n";
    };
    
    if ($@) {
        print STDERR "Error during streaming processing: $@\n";
        eval { $dbh->rollback; };
        $dbh->disconnect();
        die "Streaming processing failed: $@\n";
    }
    
    print "Processed $count accession numbers total.\n";
    
    $dbh->disconnect();
    
    # Close file handle unless it's STDIN
    unless ($filename eq '-' || $filename eq 'stdin' || $filename eq 'STDIN') {
        close $fh;
    }
}

sub process_accessions_parallel {
    my ($accessions) = @_;
    
    my $acc_count = scalar(@$accessions);
    my $chunk_size = int($acc_count / $numthreads) + 1;
    
    my @children = ();
    
    for my $thread_id (0 .. $numthreads - 1) {
        my $start_idx = $thread_id * $chunk_size;
        last if $start_idx >= $acc_count;
        
        my $end_idx = ($thread_id + 1) * $chunk_size - 1;
        $end_idx = $acc_count - 1 if $end_idx >= $acc_count;
        
        my $pid = fork();
        
        if (!defined $pid) {
            die "Cannot fork: $!\n";
        } elsif ($pid == 0) {
            # Child process
            # Create new database connection for child
            my $password = $ENV{PGPASSWORD} || '';
            my $child_dsn = "DBI:Pg:dbname=$database_name;host=$host;port=$port";
                        
            my $child_dbh = DBI->connect($child_dsn, $username, $password, {
                RaiseError => 1,
                AutoCommit => 0,
                pg_enable_utf8 => 1
            }) or die "Cannot connect to database in child process: $DBI::errstr\n";
            
            eval {
                $child_dbh->begin_work;
                
                # Process assigned accessions
                for my $i ($start_idx .. $end_idx) {
                    process_single_accession($accessions->[$i], $child_dbh);
                }
                
                $child_dbh->commit;
            };
            
            if ($@) {
                print STDERR "Error in child process: $@\n";
                eval { $child_dbh->rollback; };
                $child_dbh->disconnect();
                exit 1;
            }
            
            $child_dbh->disconnect();
            exit 0;
        } else {
            # Parent process
            push @children, $pid;
        }
    }
    
    # Wait for all children to complete
    for my $pid (@children) {
        waitpid($pid, 0);
        if ($? != 0) {
            die "Child process $pid failed with exit code " . ($? >> 8) . "\n";
        }
    }
    
    print "All threads completed successfully.\n";
}

sub process_single_accession {
    my ($accession, $dbh) = @_;
    
    # Remove version number from accession
    my $clean_accession = remove_version_number($accession);
    
    # Optimized single UPDATE query that finds and updates matching rows in one operation
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
            print "Updated $rows_updated rows for accession '$accession' (cleaned: '$clean_accession')\n";
        } else {
            print "No rows found for accession '$accession' (cleaned: '$clean_accession')\n";
        }
    };
    
    if ($@) {
        print STDERR "Error processing accession '$accession': $@\n";
    }
    
    $update_sth->finish();
}

sub remove_version_number {
    my ($accession) = @_;
    
    # Remove version number (e.g., .1, .2, etc.) from the end
    $accession =~ s/\.\d+$//;
    
    return $accession;
}


sub update_meta_statistics {
    my ($database_name) = @_;
    
    # Connect to database
    my $password = $ENV{PGPASSWORD} || '';
    my $dsn = "DBI:Pg:dbname=$database_name;host=$host;port=$port";
        
    my $dbh = DBI->connect($dsn, $username, $password, {
        RaiseError => 1,
        AutoCommit => 0,
        pg_enable_utf8 => 1
    }) or die "Cannot connect to database '$database_name': $DBI::errstr\n";
    
    print "Calculating total sequence statistics...\n";
    
    # Get datatype from existing meta table to determine bit calculation
    my $sth = $dbh->prepare("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'af_kmersearch' AND column_name = 'seq'");
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
    
    # Update af_kmersearch_meta table
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