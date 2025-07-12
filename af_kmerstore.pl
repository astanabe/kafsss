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
my $default_datatype = 'DNA4';
my $default_minsplitlen = 50000;
my $default_minlen = 64;
my $default_ovllen = 500;
my $default_numthreads = 1;
my $default_compress = 'lz4';

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
my @partitions = ();
my $tablespace = '';
my $overwrite = 0;
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
    'partition=s' => \@partitions,
    'tablespace=s' => \$tablespace,
    'overwrite:s' => sub {
        my ($name, $value) = @_;
        $overwrite = (!defined $value || $value eq '' || $value eq 'enable') ? 1 : 0;
    },
    'help|h' => \$help,
) or die "Error in command line arguments\n";

# Show help
if ($help) {
    print_help();
    exit 0;
}

# Check required arguments
if (@ARGV < 2) {
    die "Usage: perl af_kmerstore.pl [options] input_file(s) output_database\n" .
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
die "numthreads must be positive integer\n" unless $numthreads > 0;

# Parse partitions from comma-separated values
my @all_partitions = ();
for my $partition_spec (@partitions) {
    push @all_partitions, split(/,/, $partition_spec);
}

# Create partition array for PostgreSQL
my $partition_array = \@all_partitions;

print "af_kmerstore.pl version $VERSION\n";
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
print "Partitions: " . (@all_partitions ? join(', ', @all_partitions) : 'none') . "\n";
print "Tablespace: " . ($tablespace ? $tablespace : 'default') . "\n";
print "Overwrite: " . ($overwrite ? 'yes' : 'no') . "\n";

# Connect to PostgreSQL server
my $password = $ENV{PGPASSWORD} || '';
my $dsn = "DBI:Pg:host=$host;port=$port";

my $dbh = DBI->connect($dsn, $username, $password, {
    RaiseError => 1,
    AutoCommit => 1,
    pg_enable_utf8 => 1
}) or die "Cannot connect to PostgreSQL server: $DBI::errstr\n";

# Validate tablespace if specified
if ($tablespace) {
    validate_tablespace_exists($dbh, $tablespace);
}

# Check if database exists
my $db_exists = check_database_exists($dbh, $output_db);

if ($db_exists && !$overwrite) {
    # Validate existing database
    my $validation_result = validate_existing_database($dbh, $output_db);
    if ($validation_result == 1) {
        print "Using existing database '$output_db'\n";
    } elsif ($validation_result == -1) {
        die "Existing database '$output_db' has indexes. Cannot add data while indexes exist.\n" .
            "To add data, first drop indexes using: perl af_kmerindex.pl --mode=drop $output_db\n";
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
    $dbh->do($create_db_sql);
}

# Disconnect from server and connect to target database
$dbh->disconnect();

$dsn = "DBI:Pg:dbname=$output_db;host=$host;port=$port";

$dbh = DBI->connect($dsn, $username, $password, {
    RaiseError => 1,
    AutoCommit => 0,
    pg_enable_utf8 => 1
}) or die "Cannot connect to database '$output_db': $DBI::errstr\n";

# Acquire advisory lock for exclusive access to prevent conflicts with other tools
print "Acquiring exclusive lock...\n";
eval {
    $dbh->do("SELECT pg_advisory_xact_lock(999)");
    print "Exclusive lock acquired.\n";
};
if ($@) {
    die "Failed to acquire advisory lock: $@\n";
}

# Setup database if new or overwritten
if (!$db_exists || $overwrite) {
    setup_database($dbh);
}

# Process FASTA files
print "Processing FASTA files...\n";
my $total_sequences = 0;

eval {
    $dbh->begin_work;
    
    for my $i (0..$#input_files) {
        my $input_file = $input_files[$i];
        print "Processing file " . ($i + 1) . "/" . scalar(@input_files) . ": $input_file\n";
        my $file_sequences = process_fasta_file($input_file, $dbh);
        $total_sequences += $file_sequences;
        print "  Processed $file_sequences sequences from this file.\n";
    }
    
    $dbh->commit;
    print "Transaction committed successfully for FASTA file processing.\n";
};

if ($@) {
    print STDERR "Error during FASTA file processing: $@\n";
    eval { $dbh->rollback; };
    $dbh->disconnect();
    die "FASTA file processing failed: $@\n";
}

print "Total sequences processed: $total_sequences\n";

# Update statistics in af_kmersearch_meta table
print "Updating statistics in af_kmersearch_meta table...\n";
update_meta_statistics($dbh);

print "Processing completed successfully.\n";

$dbh->disconnect();

exit 0;

#
# Subroutines
#

sub print_help {
    print <<EOF;
af_kmerstore.pl version $VERSION

Usage: perl af_kmerstore.pl [options] input_file(s) output_database

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
  --partition=NAME  Partition name (can be specified multiple times or comma-separated)
  --tablespace=NAME Tablespace name for CREATE DATABASE (default: default tablespace)
  --overwrite       Overwrite existing database (default: false)
  --help, -h        Show this help message

Environment variables:
  PGHOST           PostgreSQL server host
  PGPORT           PostgreSQL server port
  PGUSER           PostgreSQL username
  PGPASSWORD       PostgreSQL password

Examples:
  # Single file
  perl af_kmerstore.pl input.fasta mydb
  
  # Multiple files
  perl af_kmerstore.pl file1.fasta file2.fasta mydb
  
  # Wildcard pattern (use quotes to prevent shell expansion)
  perl af_kmerstore.pl 'data/*.fasta' mydb
  perl af_kmerstore.pl '/path/to/genomes/*.fna' mydb
  
  # Compressed files
  perl af_kmerstore.pl genome.fasta.gz mydb
  perl af_kmerstore.pl 'data/*.fasta.bz2' mydb
  perl af_kmerstore.pl sequence.fna.xz mydb
  perl af_kmerstore.pl genome.fasta.zst mydb
  
  # BLAST database
  perl af_kmerstore.pl nr mydb
  perl af_kmerstore.pl /databases/nt mydb
  
  # Mixed sources
  perl af_kmerstore.pl file1.fasta 'data/*.fasta.gz' blastdb mydb
  
  # With options
  perl af_kmerstore.pl --datatype=DNA2 --minsplitlen=100000 'genomes/*.fasta' mydb
  perl af_kmerstore.pl --minlen=1000 --minsplitlen=50000 'genomes/*.fasta' mydb
  perl af_kmerstore.pl --partition=bacteria,archaea 'bacteria/*.fasta' mydb
  perl af_kmerstore.pl --overwrite --numthreads=4 'data/*.fasta.gz' mydb
  
  # Standard input
  cat input.fasta | perl af_kmerstore.pl stdin mydb

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

sub validate_existing_database {
    my ($dbh, $dbname) = @_;
    
    # Connect to target database for validation
    my $temp_dsn = "DBI:Pg:dbname=$dbname;host=$host;port=$port";
    my $password = $ENV{PGPASSWORD} || '';
    
    my $temp_dbh = DBI->connect($temp_dsn, $username, $password, {
        RaiseError => 0,
        AutoCommit => 1,
        pg_enable_utf8 => 1
    });
    
    return 0 unless $temp_dbh;
    
    # Check if pg_kmersearch extension exists
    my $sth = $temp_dbh->prepare("SELECT 1 FROM pg_extension WHERE extname = 'pg_kmersearch'");
    $sth->execute();
    my $ext_exists = $sth->fetchrow_array();
    $sth->finish();
    return 0 unless $ext_exists;
    
    # Check if required tables exist with correct schema
    return 0 unless check_meta_table_schema($temp_dbh);
    return 0 unless check_main_table_schema($temp_dbh);
    
    # Check meta table values
    $sth = $temp_dbh->prepare("SELECT ver, minlen, minsplitlen, ovllen FROM af_kmersearch_meta LIMIT 1");
    $sth->execute();
    my ($db_ver, $db_minlen, $db_minsplitlen, $db_ovllen) = $sth->fetchrow_array();
    $sth->finish();
    
    return 0 unless defined $db_ver && defined $db_minlen && defined $db_minsplitlen && defined $db_ovllen;
    
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
SELECT data_type 
FROM information_schema.columns 
WHERE table_name = 'af_kmersearch' AND column_name = 'seq'
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
    
    # Check that no indexes exist on af_kmersearch table
    $sth = $temp_dbh->prepare(
        "SELECT 1 FROM pg_indexes WHERE tablename = 'af_kmersearch' LIMIT 1"
    );
    $sth->execute();
    my $index_exists = $sth->fetchrow_array();
    $sth->finish();
    
    $temp_dbh->disconnect();
    
    # Return -1 if indexes exist, 1 if valid, 0 if invalid
    return $index_exists ? -1 : 1;
}

sub check_meta_table_schema {
    my ($dbh) = @_;
    
    my $sth = $dbh->prepare(<<SQL);
SELECT column_name, data_type 
FROM information_schema.columns 
WHERE table_name = 'af_kmersearch_meta' 
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
        'part' => 'jsonb',
        'kmer_size' => 'integer'
    );
    
    my %actual = ();
    while (my ($col, $type) = $sth->fetchrow_array()) {
        $actual{$col} = $type;
    }
    $sth->finish();
    
    return %actual == %expected && 
           $actual{ver} eq $expected{ver} && 
           $actual{minlen} eq $expected{minlen} && 
           $actual{ovllen} eq $expected{ovllen} &&
           $actual{nseq} eq $expected{nseq} &&
           $actual{nchar} eq $expected{nchar} &&
           $actual{part} eq $expected{part} &&
           $actual{kmer_size} eq $expected{kmer_size};
}

sub check_main_table_schema {
    my ($dbh) = @_;
    
    my $sth = $dbh->prepare(<<SQL);
SELECT column_name, data_type 
FROM information_schema.columns 
WHERE table_name = 'af_kmersearch' 
ORDER BY column_name
SQL
    $sth->execute();
    
    my %expected = (
        'seq' => lc($datatype),
        'part' => 'ARRAY',
        'seqid' => 'ARRAY'
    );
    
    my %actual = ();
    while (my ($col, $type) = $sth->fetchrow_array()) {
        $actual{$col} = $type;
    }
    $sth->finish();
    
    return %actual == %expected && 
           $actual{seq} eq $expected{seq} && 
           $actual{part} eq $expected{part} && 
           $actual{seqid} eq $expected{seqid};
}

sub setup_database {
    my ($dbh) = @_;
    
    print "Setting up database schema...\n";
    
    # Enable pg_kmersearch extension
    $dbh->do("CREATE EXTENSION IF NOT EXISTS pg_kmersearch");
    
    # Create meta table
    $dbh->do(<<SQL);
CREATE TABLE IF NOT EXISTS af_kmersearch_meta (
    ver TEXT NOT NULL,
    minlen INTEGER NOT NULL,
    minsplitlen INTEGER NOT NULL,
    ovllen SMALLINT NOT NULL,
    nseq BIGINT,
    nchar BIGINT,
    part JSONB,
    kmer_size INTEGER
)
SQL
    
    # Insert meta data
    my $sth = $dbh->prepare("DELETE FROM af_kmersearch_meta");
    $sth->execute();
    $sth->finish();
    
    $sth = $dbh->prepare("INSERT INTO af_kmersearch_meta (ver, minlen, minsplitlen, ovllen, nseq, nchar, part, kmer_size) VALUES (?, ?, ?, ?, ?, ?, ?, ?)");
    $sth->execute($VERSION, $minlen, $minsplitlen, $ovllen, 0, 0, '{}', undef);
    $sth->finish();
    
    # Create main table
    $dbh->do(<<SQL);
CREATE TABLE IF NOT EXISTS af_kmersearch (
    seq $datatype PRIMARY KEY NOT NULL,
    part TEXT[],
    seqid TEXT[] NOT NULL
)
SQL
    
    # Configure compression for af_kmersearch table columns
    configure_table_compression($dbh);
    
    print "Database schema setup completed.\n";
}

sub process_fasta_file {
    my ($filename, $dbh) = @_;
    
    my $fh = open_input_file($filename);
    my $sequence_count = 0;
    
    # Process sequences with parallel processing
    if ($numthreads == 1) {
        # Single-threaded processing - stream one by one
        while (my $seq_entry = read_next_fasta_entry($fh)) {
            my $seq_data = {
                header => $seq_entry->{label},
                sequence => $seq_entry->{sequence}
            };
            process_sequence($seq_data, $dbh);
            $sequence_count++;
        }
    } else {
        # Multi-threaded processing - need to batch sequences
        my @sequences = ();
        while (my $seq_entry = read_next_fasta_entry($fh)) {
            my $seq_data = {
                header => $seq_entry->{label},
                sequence => $seq_entry->{sequence}
            };
            push @sequences, $seq_data;
        }
        $sequence_count = scalar(@sequences);
        process_sequences_parallel(\@sequences, $dbh);
    }
    
    # Close file handle unless it's STDIN
    unless ($filename eq '-' || $filename eq 'stdin' || $filename eq 'STDIN') {
        close $fh or warn "Warning: Could not close file handle for '$filename': $!\n";
    }
    
    return $sequence_count;
}

sub read_next_fasta_entry {
    my ($fh) = @_;
    
    # Set input record separator to read one FASTA entry at a time
    local $/ = "\n>";
    
    my $line = <$fh>;
    return undef unless defined $line;
    
    # Parse the FASTA record using regex that handles optional leading '>'
    # and captures label (up to first newline) and sequence (rest, may contain newlines)
    if ($line =~ /^>?\s*(\S[^\r\n]*)\r?\n(.*)/s) {
        my $label = $1;
        my $sequence = $2;
        
        # Remove all whitespace (including newlines) from sequence
        $sequence =~ s/\s+//gs;
        
        return {
            label => $label,
            sequence => $sequence
        };
    }
    
    return undef;  # Invalid FASTA format
}

sub process_sequences_parallel {
    my ($sequences, $dbh) = @_;
    
    my $seq_count = scalar(@$sequences);
    my $chunk_size = int($seq_count / $numthreads) + 1;
    
    my @children = ();
    
    for my $thread_id (0 .. $numthreads - 1) {
        my $start_idx = $thread_id * $chunk_size;
        last if $start_idx >= $seq_count;
        
        my $end_idx = ($thread_id + 1) * $chunk_size - 1;
        $end_idx = $seq_count - 1 if $end_idx >= $seq_count;
        
        my $pid = fork();
        
        if (!defined $pid) {
            die "Cannot fork: $!\n";
        } elsif ($pid == 0) {
            # Child process
            # Create new database connection for child
            my $child_dsn = "DBI:Pg:dbname=$output_db;host=$host;port=$port";
            my $password = $ENV{PGPASSWORD} || '';
            
            my $child_dbh = DBI->connect($child_dsn, $username, $password, {
                RaiseError => 1,
                AutoCommit => 0,
                pg_enable_utf8 => 1
            }) or die "Cannot connect to database in child process: $DBI::errstr\n";
            
            eval {
                $child_dbh->begin_work;
                
                # Process assigned sequences
                for my $i ($start_idx .. $end_idx) {
                    process_sequence($sequences->[$i], $child_dbh);
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
    my @header_parts = split(/\cA+/, $header);
    
    for my $part (@header_parts) {
        # Remove everything after the first space
        $part =~ s/ .+$//;
        
        my @part_accessions = ();
        
        # Try to extract accession numbers from database-specific formats
        # Format: gb|U13106.1| or |gb|U13106.1| (multiple can exist)
        if ($part =~ /^(?:gb|emb|dbj|ref|lcl)\|([^\|\s]+)/ || $part =~ /\|(?:gb|emb|dbj|ref|lcl)\|([^\|\s]+)/) {
            my $temp_part = $part;
            while ($temp_part =~ /(?:^|.)(?:gb|emb|dbj|ref|lcl)\|([^\|\s]+)/g) {
                my $acc = $1;
                $acc =~ s/\.\d+$//;  # Remove version
                push @part_accessions, $acc;
            }
        }
        
        # Fallback: extract first token
        if (@part_accessions == 0 && $part =~ /^([^\|\s]+)/) {
            my $acc = $1;
            $acc =~ s/\.\d+$//;  # Remove version
            push @part_accessions, $acc;
        }
        
        push @accessions, @part_accessions;
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
        
        if ($seg_len <= $minsplitlen * 2) {
            # Segment is short enough, don't split
            push @fragments, {
                start => $seg_start,
                end => $seg_end
            };
        } else {
            # Split long segment with overlap
            my $target_size = int($minsplitlen * 2);
            my $overlap_step = $target_size - $ovllen;
            my $num_fragments = $overlap_step > 0 ? int(($seg_len - $ovllen) / $overlap_step) + 1 : 1;
            
            # Adjust fragment size to distribute evenly
            my $fragment_size = int(($seg_len + ($num_fragments - 1) * $ovllen) / $num_fragments);
            
            # Ensure fragment size is within bounds
            $fragment_size = $minsplitlen if $fragment_size < $minsplitlen;
            $fragment_size = $minsplitlen * 2 if $fragment_size > $minsplitlen * 2;
            
            my $pos = 0;
            
            while ($pos < $seg_len) {
                my $end_pos = $pos + $fragment_size;
                $end_pos = $seg_len if $end_pos > $seg_len;
                
                my $fragment_start = $seg_start + $pos;
                my $fragment_end = $seg_start + $end_pos - 1;
                
                push @fragments, {
                    start => $fragment_start,
                    end => $fragment_end
                };
                
                # Move to next position with overlap
                $pos = $end_pos - $ovllen;
                last if $pos >= $seg_len;
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
            print STDERR "Skipping fragment: length " . length($fragment_seq) . " is below minimum length $minlen\n";
            next;
        }
        
        # Build seqids for all accessions
        my @seqids = ();
        for my $accession (@$accessions) {
            my $seqid = sprintf("%s:%d:%d", $accession, $start, $end);
            push @seqids, $seqid;
        }
        
        # Try to insert new record
        my $sth = $dbh->prepare(<<SQL);
INSERT INTO af_kmersearch (seq, part, seqid) 
VALUES (?, ?, ?) 
ON CONFLICT (seq) DO UPDATE SET 
    part = array(SELECT DISTINCT unnest(af_kmersearch.part || ?)), 
    seqid = array(SELECT DISTINCT unnest(af_kmersearch.seqid || ?))
SQL
        
        eval {
            $sth->execute(
                $fragment_seq, 
                $partition_array, 
                \@seqids,
                $partition_array,  # for UPDATE part
                \@seqids           # for UPDATE seqid
            );
        };
        
        if ($@) {
            print STDERR "Error inserting sequence fragment: $@\n";
        }
        
        $sth->finish();
    }
}

sub update_meta_statistics {
    my ($dbh) = @_;
    
    print "Calculating total sequence statistics...\n";
    
    # Calculate total number of sequences and total bases using accurate nuc_length() function
    my $sth = $dbh->prepare(<<SQL);
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
        die "Statistics update failed: $@\n";
    }
    
    $sth->finish();
    
    print "Statistics update completed.\n";
}

sub configure_table_compression {
    my ($dbh) = @_;
    
    print "Configuring table compression: $compress\n";
    
    # Configure compression for seq, part, and seqid columns
    my @columns = ('seq', 'part', 'seqid');
    
    if ($compress eq 'lz4') {
        # Enable lz4 compression
        for my $column (@columns) {
            eval {
                $dbh->do("ALTER TABLE af_kmersearch ALTER COLUMN $column SET STORAGE EXTENDED");
                $dbh->do("ALTER TABLE af_kmersearch ALTER COLUMN $column SET COMPRESSION lz4");
            };
            if ($@) {
                print STDERR "Warning: Failed to set lz4 compression for column '$column': $@\n";
            }
        }
        print "LZ4 compression enabled for af_kmersearch table columns.\n";
        
    } elsif ($compress eq 'pglz') {
        # Enable pglz compression
        for my $column (@columns) {
            eval {
                $dbh->do("ALTER TABLE af_kmersearch ALTER COLUMN $column SET STORAGE EXTENDED");
                $dbh->do("ALTER TABLE af_kmersearch ALTER COLUMN $column SET COMPRESSION pglz");
            };
            if ($@) {
                print STDERR "Warning: Failed to set pglz compression for column '$column': $@\n";
            }
        }
        print "PGLZ compression enabled for af_kmersearch table columns.\n";
        
    } elsif ($compress eq 'disable') {
        # Disable compression
        for my $column (@columns) {
            eval {
                $dbh->do("ALTER TABLE af_kmersearch ALTER COLUMN $column SET STORAGE EXTERNAL");
            };
            if ($@) {
                print STDERR "Warning: Failed to disable compression for column '$column': $@\n";
            }
        }
        print "Compression disabled for af_kmersearch table columns.\n";
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

sub open_input_file {
    my ($filename) = @_;
    
    # Handle standard input
    if ($filename eq '-' || $filename eq 'stdin' || $filename eq 'STDIN') {
        return \*STDIN;
    }
    
    # Check for BLAST database
    if (!-f $filename && (-f "$filename.nsq" || -f "$filename.nal")) {
        # BLAST nucleotide database
        open my $fh, '-|', 'blastdbcmd', '-db', $filename, '-dbtype', 'nucl', '-entry', 'all', '-out', '-', '-outfmt', '>%a\\n%s\\n', '-line_length', '1000000', '-target_only' or die "Cannot open BLAST database '$filename': $!\n";
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