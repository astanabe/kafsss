#!/usr/bin/perl

use strict;
use warnings;
use Getopt::Long;
use DBI;
use JSON;
use POSIX qw(strftime);
use Sys::Hostname;
use File::Basename;
use File::Temp qw(tempfile);

# Version number
my $VERSION = "1.0.0";

# Default values
my $default_host = $ENV{PGHOST} || 'localhost';
my $default_port = $ENV{PGPORT} || 5432;
my $default_user = $ENV{PGUSER} || getpwuid($<);
my $default_maxnseq = 0;
my $default_numthreads = 1;
my $default_mode = 'sequence';
my $default_minpsharedkmer = 0.5;
my $default_minscore = 1;
my $default_outfmt = 'TSV';

# Command line options
my $host = $default_host;
my $port = $default_port;
my $username = $default_user;
my $database = '';
my $subset = '';
my $maxnseq = $default_maxnseq;
my $minscore = $default_minscore;
my $minpsharedkmer = $default_minpsharedkmer;
my $numthreads = $default_numthreads;
my $mode = $default_mode;
my $outfmt = $default_outfmt;
my $seqid_db = '';
my $verbose = 0;
my $help = 0;

# Parse command line options
GetOptions(
    'host=s' => \$host,
    'port=i' => \$port,
    'username=s' => \$username,
    'db=s' => \$database,
    'subset=s' => \$subset,
    'maxnseq=i' => \$maxnseq,
    'minscore=i' => \$minscore,
    'minpsharedkmer=f' => \$minpsharedkmer,
    'numthreads=i' => \$numthreads,
    'mode=s' => \$mode,
    'outfmt=s' => \$outfmt,
    'seqid_db=s' => \$seqid_db,
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
    die "Usage: kafsssearch [options] input_file(s) output_file\n" .
        "Use --help for detailed usage information.\n";
}

my $output_file = pop @ARGV;
my @input_patterns = @ARGV;

# Expand input file patterns (glob)
my @input_files = expand_input_files(@input_patterns);

# Validate that we have at least one input file
if (@input_files == 0) {
    die "No input files found for pattern(s): " . join(', ', @input_patterns) . "\n";
}

# Validate required options
die "Database name must be specified with --db option\n" unless $database;
die "maxnseq must be non-negative integer\n" unless $maxnseq >= 0;
die "minscore must be positive integer\n" if defined $minscore && $minscore <= 0;
die "minpsharedkmer must be between 0.0 and 1.0\n" unless $minpsharedkmer >= 0.0 && $minpsharedkmer <= 1.0;
die "numthreads must be positive integer\n" unless $numthreads > 0;

# Validate mode
my $normalized_mode = normalize_mode($mode);
if (!$normalized_mode) {
    die "Invalid mode: $mode. Must be 'minimum', 'matchscore', 'sequence', or 'maximum'\n";
}
$mode = $normalized_mode;

# Validate outfmt
my $normalized_outfmt = normalize_outfmt($outfmt);
if (!$normalized_outfmt) {
    die "Invalid outfmt: $outfmt. Must be 'TSV', 'multiTSV', 'FASTA', 'multiFASTA', or 'BLASTDB'\n";
}
$outfmt = $normalized_outfmt;

# Validate outfmt and mode combination
if (($outfmt eq 'FASTA' || $outfmt eq 'multiFASTA') && $mode ne 'sequence') {
    die "FASTA output format requires --mode=sequence\n";
}

# Validate BLASTDB output format requirements
if ($outfmt eq 'BLASTDB') {
    if ($mode ne 'minimum' && $mode ne 'sequence') {
        die "BLASTDB output format requires --mode=minimum or --mode=sequence\n";
    }
    if ($mode eq 'minimum' && !$seqid_db) {
        die "BLASTDB output format with --mode=minimum requires --seqid_db option\n";
    }
    if ($output_file eq '-' || $output_file eq 'stdout' || $output_file eq 'STDOUT') {
        die "BLASTDB output format requires a file prefix, cannot use stdout\n";
    }
    # Check for existing files with ${output_file}_*.* pattern
    my @existing_files = glob("${output_file}_*.*");
    if (@existing_files > 0) {
        die "Output files already exist: " . join(', ', @existing_files) . "\n" .
            "Please remove existing files or use a different output prefix.\n";
    }
    # Check required BLAST+ tools availability
    if ($mode eq 'minimum') {
        # Check blastdb_aliastool availability
        my $version_output = `blastdb_aliastool -version 2>&1`;
        my $exit_code = $? >> 8;
        if ($exit_code != 0 || $version_output !~ /blastdb_aliastool:\s+[\d\.]+/) {
            die "blastdb_aliastool is not available or not working properly.\n" .
                "Version check failed: $version_output\n" .
                "Please install BLAST+ tools or check PATH.\n";
        }
    } elsif ($mode eq 'sequence') {
        # Check makeblastdb availability
        my $version_output = `makeblastdb -version 2>&1`;
        my $exit_code = $? >> 8;
        if ($exit_code != 0 || $version_output !~ /makeblastdb:\s+[\d\.]+/) {
            die "makeblastdb is not available or not working properly.\n" .
                "Version check failed: $version_output\n" .
                "Please install BLAST+ tools or check PATH.\n";
        }
    }
}

# Validate outfmt and output file combination
if (($outfmt eq 'multiTSV' || $outfmt eq 'FASTA' || $outfmt eq 'multiFASTA') &&
    ($output_file eq '-' || $output_file eq 'stdout' || $output_file eq 'STDOUT')) {
    die "Output format '$outfmt' requires a file prefix, cannot use stdout\n";
}

print "kafsssearch version $VERSION\n";
print "Input files (" . scalar(@input_files) . "):\n";
for my $i (0..$#input_files) {
    print "  " . ($i + 1) . ". $input_files[$i]\n";
}
print "Output file: $output_file\n";
print "Database: $database\n";
print "Host: $host\n";
print "Port: $port\n";
print "Username: $username\n";
print "Subset: " . ($subset ? $subset : 'all') . "\n";
print "Max sequences: " . ($maxnseq == 0 ? "unlimited" : $maxnseq) . "\n";
print "Min score: $minscore\n";
print "Min shared k-mer rate: $minpsharedkmer\n";
print "Number of threads: $numthreads\n";
print "Mode: $mode\n";
print "Output format: $outfmt\n";

# Connect to PostgreSQL server first for validation
my $password = $ENV{PGPASSWORD} || '';
my $server_dsn = "DBI:Pg:host=$host;port=$port";

my $server_dbh = DBI->connect($server_dsn, $username, $password, {
    AutoCommit => 1,
    PrintError => 0,
    RaiseError => 1,
    ShowErrorStatement => 1,
    AutoInactiveDestroy => 1,
    pg_enable_utf8 => 1
}) or die "Cannot connect to PostgreSQL server: $DBI::errstr\n";

# Validate user existence and permissions
validate_user_and_permissions($server_dbh, $username);

# Check if database exists
unless (check_database_exists($server_dbh, $database)) {
    $server_dbh->disconnect();
    die "Error: Database '$database' does not exist.\n" .
        "Please create it first using kafssstore.\n";
}

$server_dbh->disconnect();

# Connect to target database
my $dsn = "DBI:Pg:dbname=$database;host=$host;port=$port";
my $dbh = DBI->connect($dsn, $username, $password, {
    AutoCommit => 1,
    PrintError => 0,
    RaiseError => 1,
    ShowErrorStatement => 1,
    AutoInactiveDestroy => 1,
    pg_enable_utf8 => 1
}) or die "Cannot connect to database '$database': $DBI::errstr\n";

print "Connected to database successfully.\n";

# Validate database permissions and schema
validate_database_permissions($dbh, $username);
validate_database_schema($dbh);

# Verify database structure
verify_database_structure($dbh);

# Get all metadata from kafsss_meta table
my $metadata = get_metadata_from_meta($dbh);
print "Retrieved metadata - k-mer size: $metadata->{kmer_size}, ovllen: $metadata->{ovllen}\n";

# Check for matching high-frequency k-mer data
my $use_highfreq_cache = check_highfreq_kmer_exists($dbh, $metadata);

# Set k-mer size for pg_kmersearch
print "Setting k-mer size to $metadata->{kmer_size}...\n";
eval {
    $dbh->do("SET kmersearch.kmer_size = $metadata->{kmer_size}");
    print "K-mer size set to $metadata->{kmer_size} successfully.\n";
};
if ($@) {
    die "Failed to set k-mer size: $@\n";
}

# Set additional kmersearch parameters if available
if (defined $metadata->{occur_bitlen}) {
    print "Setting occur_bitlen to $metadata->{occur_bitlen}...\n";
    eval {
        $dbh->do("SET kmersearch.occur_bitlen = $metadata->{occur_bitlen}");
        print "Occur_bitlen set to $metadata->{occur_bitlen} successfully.\n";
    };
    if ($@) {
        warn "Warning: Failed to set occur_bitlen: $@\n";
    }
}

if (defined $metadata->{max_appearance_rate}) {
    print "Setting max_appearance_rate to $metadata->{max_appearance_rate}...\n";
    eval {
        $dbh->do("SET kmersearch.max_appearance_rate = $metadata->{max_appearance_rate}");
        print "Max_appearance_rate set to $metadata->{max_appearance_rate} successfully.\n";
    };
    if ($@) {
        warn "Warning: Failed to set max_appearance_rate: $@\n";
    }
}

if (defined $metadata->{max_appearance_nrow}) {
    print "Setting max_appearance_nrow to $metadata->{max_appearance_nrow}...\n";
    eval {
        $dbh->do("SET kmersearch.max_appearance_nrow = $metadata->{max_appearance_nrow}");
        print "Max_appearance_nrow set to $metadata->{max_appearance_nrow} successfully.\n";
    };
    if ($@) {
        warn "Warning: Failed to set max_appearance_nrow: $@\n";
    }
}

# Set high-frequency k-mer exclusion parameters
if ($use_highfreq_cache) {
    print "Enabling high-frequency k-mer exclusion...\n";
    eval {
        $dbh->do("SET kmersearch.preclude_highfreq_kmer = true");
        $dbh->do("SET kmersearch.force_use_parallel_highfreq_kmer_cache = true");
        print "High-frequency k-mer exclusion enabled.\n";
    };
    if ($@) {
        warn "Warning: Failed to enable high-frequency k-mer exclusion: $@\n";
    }
} else {
    print "Disabling high-frequency k-mer exclusion (no matching data)...\n";
    eval {
        $dbh->do("SET kmersearch.preclude_highfreq_kmer = false");
        $dbh->do("SET kmersearch.force_use_parallel_highfreq_kmer_cache = false");
        print "High-frequency k-mer exclusion disabled.\n";
    };
    if ($@) {
        warn "Warning: Failed to disable high-frequency k-mer exclusion: $@\n";
    }
}

# Set minimum score if specified
if (defined $minscore) {
    print "Setting minimum score to $minscore...\n";
    eval {
        $dbh->do("SET kmersearch.min_score = $minscore");
        print "Minimum score set to $minscore successfully.\n";
    };
    if ($@) {
        die "Failed to set minimum score: $@\n";
    }
}

# Set minimum shared k-mer rate
print "Setting minimum shared k-mer rate to $minpsharedkmer...\n";
eval {
    $dbh->do("SET kmersearch.min_shared_kmer_rate = $minpsharedkmer");
    print "Minimum shared k-mer rate set to $minpsharedkmer successfully.\n";
};
if ($@) {
    die "Failed to set minimum shared k-mer rate: $@\n";
}

# Parent process disconnects from database after metadata retrieval (child processes will reconnect)
if ($numthreads > 1) {
    $dbh->disconnect();
    print "Parent process disconnected from database.\n";
    $dbh = undef;  # Clear the handle to prevent accidental use
}

# Prepare output handles based on format
my $output_handles = {};
my $output_fh = undef;

if ($outfmt eq 'TSV') {
    # Single TSV file
    $output_fh = open_output_file($output_file);
} elsif ($outfmt eq 'multiTSV' || $outfmt eq 'FASTA' || $outfmt eq 'multiFASTA') {
    # Multiple files - handles will be created per query
    # Store the prefix for later use
    $output_handles->{prefix} = $output_file;
    $output_handles->{format} = $outfmt;
    $output_handles->{mode} = $mode;
} elsif ($outfmt eq 'BLASTDB') {
    # BLAST database output - uses multiTSV or multiFASTA internally
    $output_handles->{prefix} = $output_file;
    $output_handles->{format} = 'BLASTDB';
    $output_handles->{mode} = $mode;
    $output_handles->{seqid_db} = $seqid_db;
    $output_handles->{query_numbers} = [];  # Track query numbers for post-processing
}

# Process FASTA sequences from multiple files
print "Processing FASTA sequences...\n" if $verbose;
my $total_results = 0;
my $total_queries = 0;

for my $i (0..$#input_files) {
    my $input_file = $input_files[$i];
    print "Processing file " . ($i + 1) . "/" . scalar(@input_files) . ": $input_file\n" if $verbose;
    
    # Open current input file
    my $input_fh = open_input_file($input_file);
    
    my $file_results;
    if ($numthreads == 1) {
        # Single-threaded processing
        $file_results = process_sequences_single_threaded($input_fh, $output_fh, $dbh, $total_queries, $metadata, $output_handles);
    } else {
        # Multi-threaded processing with process pool
        $file_results = process_sequences_parallel_streaming($input_fh, $output_fh, $total_queries, $metadata, $output_handles);
    }
    
    $total_results += $file_results->{results};
    $total_queries += $file_results->{queries};
    
    close_input_file($input_fh, $input_file);
    
    print "  Processed " . $file_results->{queries} . " queries, " . $file_results->{results} . " results from this file.\n";
}

# Close files and database connection
if ($output_fh) {
    close_output_file($output_fh, $output_file);
}
# Close any remaining output handles for multi-file formats
if ($output_handles && ref($output_handles) eq 'HASH') {
    for my $key (keys %$output_handles) {
        next if $key eq 'prefix' || $key eq 'format' || $key eq 'mode';
        if (ref($output_handles->{$key}) eq 'GLOB' || ref($output_handles->{$key}) eq 'REF') {
            close($output_handles->{$key});
        }
    }
}
if (defined $dbh) {
    $dbh->disconnect();
}

# Post-processing for BLASTDB format
if ($outfmt eq 'BLASTDB' && $output_handles->{query_numbers} && @{$output_handles->{query_numbers}} > 0) {
    print "Creating BLAST databases...\n";
    create_blastdb_files($output_handles);
}

print "Processing completed successfully.\n";
print "Total queries processed: $total_queries\n";
print "Total results output: $total_results\n";

exit 0;

#
# Subroutines
#

sub print_help {
    print <<EOF;
kafsssearch version $VERSION

Usage: kafsssearch [options] input_file(s) output_file

Search DNA sequences from multiple sources against kafsss database using k-mer similarity.

Required arguments:
  input_file(s)     Input FASTA file(s), patterns, or databases:
                    - Regular files: file1.fasta file2.fasta
                    - Wildcard patterns: 'data/*.fasta' (use quotes to prevent shell expansion)
                    - Compressed files: file.fasta.gz file.fasta.bz2 file.fasta.xz file.fasta.zst
                    - BLAST databases: mydb (requires mydb.nsq or mydb.nal)
                    - Standard input: '-', 'stdin', or 'STDIN'
  output_file       Output TSV file (use '-', 'stdout', or 'STDOUT' for standard output)

Required options:
  --db=DATABASE     PostgreSQL database name

Other options:
  --host=HOST       PostgreSQL server host (default: \$PGHOST or localhost)
  --port=PORT       PostgreSQL server port (default: \$PGPORT or 5432)
  --username=USER   PostgreSQL username (default: \$PGUSER or current user)
  --subset=NAME     Limit search to specific subset (optional)
  --maxnseq=INT     Maximum number of results per query (default: 1000, 0=unlimited)
  --minscore=INT    Minimum score threshold (default: 1)
  --minpsharedkmer=REAL  Minimum percentage of shared k-mers (0.0-1.0, default: 0.5)
  --numthreads=INT  Number of parallel threads (default: 1)
  --mode=MODE       Output mode: minimum, matchscore, sequence, maximum (default: matchscore)
  --outfmt=FORMAT   Output format: TSV, multiTSV, FASTA, multiFASTA, BLASTDB (default: TSV)
  --seqid_db=DB     BLAST database name for BLASTDB output with --mode=minimum
  --verbose, -v     Show detailed processing messages (default: false)
  --help, -h        Show this help message

Environment variables:
  PGHOST           PostgreSQL server host
  PGPORT           PostgreSQL server port
  PGUSER           PostgreSQL username
  PGPASSWORD       PostgreSQL password

Output format (--outfmt option):
  TSV:       Single tab-separated values file with columns:
             1. Query sequence number (1-based integer)
             2. Query FASTA label
             3. Comma-separated seqid list from seqid column
             4. Match score (only in matchscore and maximum modes)
             5. Sequence data (only in sequence and maximum modes)
  
  multiTSV:  Multiple TSV files, one per query sequence
             Output files: output_prefix_N.tsv (N = query sequence number)
             Same columns as TSV format
  
  FASTA/multiFASTA: Multiple FASTA files, one per query sequence
             (requires --mode=sequence)
             Output files: output_prefix_N.fasta (N = query sequence number)
             Format: >seqid1[^Aseqid2[^A...]]
                     sequence_data
             Multiple seqids are separated by SOH (^A) character

  BLASTDB:   Create BLAST database files for each query sequence
             Requires --mode=minimum or --mode=sequence

             With --mode=minimum (requires --seqid_db):
               Creates alias database referencing existing BLAST database
               Output files per query:
                 - output_prefix_N.tsv     (TSV results)
                 - output_prefix_N.acclist (accession list)
                 - output_prefix_N.bsl     (binary seqid list)
                 - output_prefix_N.nal     (nucleotide alias database)

             With --mode=sequence:
               Creates new BLAST database from FASTA sequences
               Output files per query:
                 - output_prefix_N.fasta   (FASTA sequences)
                 - output_prefix_N.n*      (BLAST database files)

Examples:
  # Single file
  kafsssearch --db=mydb query.fasta results.tsv
  
  # Multiple files
  kafsssearch --db=mydb file1.fasta file2.fasta results.tsv
  
  # Wildcard pattern (use quotes to prevent shell expansion)
  kafsssearch --db=mydb 'queries/*.fasta' results.tsv
  
  # Compressed files
  kafsssearch --db=mydb query.fasta.gz results.tsv
  kafsssearch --db=mydb 'data/*.fasta.bz2' results.tsv
  
  # BLAST database
  kafsssearch --db=mydb nr results.tsv
  
  # Mixed sources
  kafsssearch --db=mydb file1.fasta 'data/*.gz' blastdb results.tsv
  
  # With options
  kafsssearch --db=mydb --subset=bacteria 'queries/*.fasta' results.tsv
  kafsssearch --db=mydb --maxnseq=500 --minscore=10 query.fasta results.tsv
  kafsssearch --db=mydb --numthreads=4 --mode=maximum 'data/*.fasta' results.tsv
  
  # Standard input
  cat query.fasta | kafsssearch --db=mydb stdin stdout > results.tsv

  # BLAST database output with alias (requires existing BLAST database)
  kafsssearch --db=mydb --mode=minimum --outfmt=BLASTDB --seqid_db=nt query.fasta results

  # BLAST database output with new database creation
  kafsssearch --db=mydb --mode=sequence --outfmt=BLASTDB query.fasta results

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
    
    die "pg_kmersearch extension is not installed in database '$database'\n" 
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
    
    die "Table 'kafsss_data' does not exist in database '$database'\n" 
        unless $table_count > 0;
    
    print "Database structure verified.\n";
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

sub close_input_file {
    my ($fh, $filename) = @_;
    
    # Don't close STDIN
    return if $filename eq '-' || $filename eq 'stdin' || $filename eq 'STDIN';
    
    # Close file handle with error checking
    # For BLAST databases opened with pipe, ignore ECHILD error
    if (!close($fh)) {
        # Only warn if it's not a "No child processes" error (ECHILD)
        warn "Warning: Could not close file handle for '$filename': $!\n" unless $! =~ /No child processes/;
    }
}

sub open_output_file {
    my ($filename) = @_;
    
    if ($filename eq '-' || $filename eq 'stdout' || $filename eq 'STDOUT') {
        return \*STDOUT;
    } else {
        open my $fh, '>', $filename or die "Cannot open output file '$filename': $!\n";
        return $fh;
    }
}

sub close_output_file {
    my ($fh, $filename) = @_;
    
    return if $filename eq '-' || $filename eq 'stdout' || $filename eq 'STDOUT';
    close $fh;
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
        
        # Replace tab characters in label with 4 spaces to prevent TSV format corruption
        $label =~ s/\t/    /g;
        
        # Remove non-alphabetic characters from sequence (keeps only A-Z)
        $sequence =~ s/[^A-Z]//sg;
        
        return {
            label => $label,
            sequence => $sequence
        };
    }
    
    return undef;  # Invalid FASTA format
}

sub process_sequences_single_threaded {
    my ($input_fh, $output_fh, $dbh, $query_offset, $metadata, $output_handles) = @_;
    $query_offset ||= 0;
    
    my $total_results = 0;
    my $query_count = 0;
    
    while (my $fasta_entry = read_next_fasta_entry($input_fh)) {
        $query_count++;
        my $global_query_number = $query_offset + $query_count;
        my $results = search_sequence_with_validation($fasta_entry, $dbh, $global_query_number, $metadata->{ovllen}, $mode, $metadata->{kmer_size});
        
        if (@$results > 0) {
            if ($output_handles && $output_handles->{format}) {
                # Multi-file output
                write_results_to_file($results, $global_query_number, $output_handles);
                $total_results += scalar(@$results);
            } else {
                # Single file output (TSV)
                for my $result (@$results) {
                    print $output_fh join("\t", @$result) . "\n";
                    $total_results++;
                }
            }
            print "Processed query $global_query_number: " . $fasta_entry->{label} . 
                  " (" . scalar(@$results) . " results)\n";
        } else {
            print STDERR "No matches found for query $global_query_number: " . $fasta_entry->{label} . "\n";
            print "Processed query $global_query_number: " . $fasta_entry->{label} . " (0 results)\n";
        }
    }
    
    return { results => $total_results, queries => $query_count };
}

sub process_sequences_parallel_streaming {
    my ($input_fh, $output_fh, $query_offset, $metadata, $output_handles) = @_;
    $query_offset ||= 0;
    
    my %active_children = ();  # pid => {query_number, temp_file}
    my %completed_results = (); # query_number => [result_lines]
    my $query_count = 0;
    my $next_output_query = $query_offset + 1;
    my $total_results = 0;
    
    while (1) {
        # Read next FASTA entry if we have available slots
        my $fasta_entry = undef;
        if (scalar(keys %active_children) < $numthreads) {
            $fasta_entry = read_next_fasta_entry($input_fh);
        }
        
        # Fork new process if we have a sequence and available slots
        if ($fasta_entry && scalar(keys %active_children) < $numthreads) {
            $query_count++;
            my $global_query_number = $query_offset + $query_count;
            my ($temp_fh, $temp_file) = tempfile("kafsssearch_$$" . "_$global_query_number" . "_XXXXXX", UNLINK => 0);
            close($temp_fh);
            
            my $pid = fork();
            
            if (!defined $pid) {
                die "Cannot fork: $!\n";
            } elsif ($pid == 0) {
                # Child process
                process_single_sequence($fasta_entry, $global_query_number, $temp_file, $metadata, $output_handles);
                exit 0;
            } else {
                # Parent process
                $active_children{$pid} = {
                    query_number => $global_query_number,
                    temp_file => $temp_file
                };
                print "Started query $query_count: " . $fasta_entry->{label} . "\n";
            }
        }
        
        # Check for completed children (non-blocking)
        my $pid = waitpid(-1, 1);  # WNOHANG = 1
        if ($pid > 0 && exists $active_children{$pid}) {
            my $child_info = delete $active_children{$pid};
            my $query_number = $child_info->{query_number};
            my $temp_file = $child_info->{temp_file};
            
            if ($? != 0) {
                die "Child process $pid (query $query_number) failed with exit code " . ($? >> 8) . "\n";
            }
            
            # Count results and store temp file info for memory-efficient processing
            my $result_count = 0;
            if (-f $temp_file) {
                open my $temp_fh, '<', $temp_file or die "Cannot open temporary file '$temp_file': $!\n";
                while (<$temp_fh>) {
                    $result_count++;
                }
                close $temp_fh;
                
                # Store temp file path and count instead of loading all data into memory
                $completed_results{$query_number} = {
                    file => $temp_file,
                    count => $result_count
                };
            } else {
                $completed_results{$query_number} = {
                    file => undef,
                    count => 0
                };
            }
            
            if ($result_count > 0) {
                print "Completed query $query_number ($result_count results)\n";
            } else {
                print "Completed query $query_number (0 results)\n";
            }
        }
        
        # Output completed results in order
        while (exists $completed_results{$next_output_query}) {
            my $result_info = delete $completed_results{$next_output_query};
            
            if ($result_info->{file} && -f $result_info->{file}) {
                if ($output_handles && $output_handles->{format}) {
                    # Multi-file output - read and write to separate files
                    process_temp_file_for_multifile($result_info->{file}, $next_output_query, $output_handles);
                    $total_results += $result_info->{count};
                    unlink $result_info->{file};
                } else {
                    # Single file output - stream directly
                    open my $temp_fh, '<', $result_info->{file} or die "Cannot open temporary file '$result_info->{file}': $!\n";
                    while (my $line = <$temp_fh>) {
                        chomp $line;
                        print $output_fh "$line\n";
                        $total_results++;
                    }
                    close $temp_fh;
                    unlink $result_info->{file};
                }
            }
            $next_output_query++;
        }
        
        # Exit condition: no more input and no active children
        if (!$fasta_entry && scalar(keys %active_children) == 0) {
            last;
        }
        
        # If we have active children but no available slots, wait for at least one to complete
        if (scalar(keys %active_children) >= $numthreads) {
            my $pid = waitpid(-1, 0);  # Blocking wait
            if ($pid > 0 && exists $active_children{$pid}) {
                my $child_info = delete $active_children{$pid};
                my $query_number = $child_info->{query_number};
                my $temp_file = $child_info->{temp_file};
                
                if ($? != 0) {
                    die "Child process $pid (query $query_number) failed with exit code " . ($? >> 8) . "\n";
                }
                
                # Count results and store temp file info for memory-efficient processing
                my $result_count = 0;
                if (-f $temp_file) {
                    open my $temp_fh, '<', $temp_file or die "Cannot open temporary file '$temp_file': $!\n";
                    while (<$temp_fh>) {
                        $result_count++;
                    }
                    close $temp_fh;
                    
                    # Store temp file path and count instead of loading all data into memory
                    $completed_results{$query_number} = {
                        file => $temp_file,
                        count => $result_count
                    };
                } else {
                    $completed_results{$query_number} = {
                        file => undef,
                        count => 0
                    };
                }
                
                if ($result_count > 0) {
                    print "Completed query $query_number ($result_count results)\n";
                } else {
                    print "Completed query $query_number (0 results)\n";
                }
            }
        }
    }
    
    # Output any remaining completed results
    while (exists $completed_results{$next_output_query}) {
        my $result_info = delete $completed_results{$next_output_query};
        
        if ($result_info->{file} && -f $result_info->{file}) {
            if ($output_handles && $output_handles->{format}) {
                # Multi-file output - read and write to separate files
                process_temp_file_for_multifile($result_info->{file}, $next_output_query, $output_handles);
                $total_results += $result_info->{count};
                unlink $result_info->{file};
            } else {
                # Single file output - stream directly
                open my $temp_fh, '<', $result_info->{file} or die "Cannot open temporary file '$result_info->{file}': $!\n";
                while (my $line = <$temp_fh>) {
                    chomp $line;
                    print $output_fh "$line\n";
                    $total_results++;
                }
                close $temp_fh;
                unlink $result_info->{file};
            }
        }
        $next_output_query++;
    }
    
    print "All sequences processed.\n";
    return { results => $total_results, queries => $query_count };
}

sub process_single_sequence {
    my ($fasta_entry, $query_number, $temp_file, $metadata, $output_handles) = @_;
    
    # Create new database connection for child process
    my $password = $ENV{PGPASSWORD} || '';
    my $child_dsn = "DBI:Pg:dbname=$database;host=$host;port=$port";
        
    my $child_dbh = DBI->connect($child_dsn, $username, $password, {
        AutoCommit => 1,
        PrintError => 0,
        RaiseError => 1,
        ShowErrorStatement => 1,
        AutoInactiveDestroy => 1,
        pg_enable_utf8 => 1
    }) or die "Cannot connect to database in child process: $DBI::errstr\n";
    
    # Set k-mer size for pg_kmersearch (using metadata from parent)
    eval {
        $child_dbh->do("SET kmersearch.kmer_size = $metadata->{kmer_size}");
    };
    if ($@) {
        die "Failed to set k-mer size in child process: $@\n";
    }
    
    # Set additional kmersearch parameters if available (using metadata from parent)
    if (defined $metadata->{occur_bitlen}) {
        eval {
            $child_dbh->do("SET kmersearch.occur_bitlen = $metadata->{occur_bitlen}");
        };
        if ($@) {
            warn "Warning: Failed to set occur_bitlen in child process: $@\n";
        }
    }
    
    if (defined $metadata->{max_appearance_rate}) {
        eval {
            $child_dbh->do("SET kmersearch.max_appearance_rate = $metadata->{max_appearance_rate}");
        };
        if ($@) {
            warn "Warning: Failed to set max_appearance_rate in child process: $@\n";
        }
    }
    
    if (defined $metadata->{max_appearance_nrow}) {
        eval {
            $child_dbh->do("SET kmersearch.max_appearance_nrow = $metadata->{max_appearance_nrow}");
        };
        if ($@) {
            warn "Warning: Failed to set max_appearance_nrow in child process: $@\n";
        }
    }
    
    # Set high-frequency k-mer exclusion parameters (using metadata from parent)
    if ($metadata->{use_highfreq_cache}) {
        eval {
            $child_dbh->do("SET kmersearch.preclude_highfreq_kmer = true");
            $child_dbh->do("SET kmersearch.force_use_parallel_highfreq_kmer_cache = true");
        };
        if ($@) {
            warn "Warning: Failed to enable high-frequency k-mer exclusion in child process: $@\n";
        }
    } else {
        eval {
            $child_dbh->do("SET kmersearch.preclude_highfreq_kmer = false");
            $child_dbh->do("SET kmersearch.force_use_parallel_highfreq_kmer_cache = false");
        };
        if ($@) {
            warn "Warning: Failed to disable high-frequency k-mer exclusion in child process: $@\n";
        }
    }
    
    # Set minimum score if specified
    if (defined $minscore) {
        eval {
            $child_dbh->do("SET kmersearch.min_score = $minscore");
        };
        if ($@) {
            die "Failed to set minimum score in child process: $@\n";
        }
    }
    
    # Set minimum shared k-mer rate
    eval {
        $child_dbh->do("SET kmersearch.min_shared_kmer_rate = $minpsharedkmer");
    };
    if ($@) {
        die "Failed to set minimum shared k-mer rate in child process: $@\n";
    }
    
    # Search sequence (using metadata from parent, no need to retrieve again)
    my $results = search_sequence_with_validation($fasta_entry, $child_dbh, $query_number, $metadata->{ovllen}, $mode, $metadata->{kmer_size});
    
    # Check for no matches and report to STDERR
    if (@$results == 0) {
        print STDERR "No matches found for query $query_number: " . $fasta_entry->{label} . "\n";
    }
    
    # Write results to temporary file
    open my $temp_fh, '>', $temp_file or die "Cannot open temporary file '$temp_file': $!\n";
    if ($output_handles && $output_handles->{format}) {
        # For multi-file formats, store results in a format that can be parsed later
        # Store format info on first line
        print $temp_fh "#FORMAT:" . $output_handles->{format} . "\n";
        print $temp_fh "#MODE:" . $output_handles->{mode} . "\n";
    }
    for my $result (@$results) {
        print $temp_fh join("\t", @$result) . "\n";
    }
    close $temp_fh;
    
    $child_dbh->disconnect();
}

sub search_sequence_with_validation {
    my ($fasta_entry, $dbh, $query_number, $ovllen_value, $search_mode, $kmer_size) = @_;
    
    my $label = $fasta_entry->{label};
    my $sequence = $fasta_entry->{sequence};
    
    # Validate query sequence
    my $validation_result = validate_query_sequence($sequence, $ovllen_value, $kmer_size);
    if (!$validation_result->{valid}) {
        print STDERR "Warning: Skipping query $query_number '$label': $validation_result->{reason}\n";
        return [];  # Return empty results
    }
    
    # Build search query
    my $sql;
    my @params;
    
    # Build WHERE clause
    my $where_clause = "WHERE seq =% ?";
    @params = ($sequence);
    if ($subset) {
        $where_clause .= " AND ? = ANY(subset)";
        push @params, $subset;
    }
    
    # Build query based on mode and requirements
    if ($search_mode eq 'minimum') {
        if ($maxnseq == 0) {
            # No limit, no score needed
            $sql = "SELECT seqid FROM kafsss_data $where_clause";
        } else {
            # With limit, need score for ordering but don't output it
            $sql = "SELECT seqid FROM kafsss_data $where_clause ORDER BY kmersearch_matchscore(seq, ?) DESC LIMIT ?";
            push @params, $sequence, $maxnseq;
        }
    } elsif ($search_mode eq 'sequence') {
        if ($maxnseq == 0) {
            # No limit, no score needed
            $sql = "SELECT seqid, seq FROM kafsss_data $where_clause";
        } else {
            # With limit, need score for ordering but don't output it
            $sql = "SELECT seqid, seq FROM kafsss_data $where_clause ORDER BY kmersearch_matchscore(seq, ?) DESC LIMIT ?";
            push @params, $sequence, $maxnseq;
        }
    } elsif ($search_mode eq 'matchscore') {
        if ($maxnseq == 0) {
            $sql = "SELECT seqid, kmersearch_matchscore(seq, ?) AS score FROM kafsss_data $where_clause ORDER BY score DESC";
            unshift @params, $sequence;
        } else {
            $sql = "SELECT seqid, kmersearch_matchscore(seq, ?) AS score FROM kafsss_data $where_clause ORDER BY score DESC LIMIT ?";
            unshift @params, $sequence;
            push @params, $maxnseq;
        }
    } else {
        # maximum mode
        if ($maxnseq == 0) {
            $sql = "SELECT seqid, kmersearch_matchscore(seq, ?) AS score, seq FROM kafsss_data $where_clause ORDER BY score DESC";
            unshift @params, $sequence;
        } else {
            $sql = "SELECT seqid, kmersearch_matchscore(seq, ?) AS score, seq FROM kafsss_data $where_clause ORDER BY score DESC LIMIT ?";
            unshift @params, $sequence;
            push @params, $maxnseq;
        }
    }
    
    my @results = ();
    
    eval {
        my $sth = $dbh->prepare($sql);
        $sth->execute(@params);
        
        if ($search_mode eq 'maximum') {
            while (my ($seqid_array, $score, $seq) = $sth->fetchrow_array()) {
                # Parse PostgreSQL array and extract seqid
                my $seqid_str = extract_seqid_string($seqid_array);
                
                push @results, [$query_number, $label, $seqid_str, $score, $seq];
            }
        } elsif ($search_mode eq 'minimum') {
            while (my ($seqid_array) = $sth->fetchrow_array()) {
                # Parse PostgreSQL array and extract seqid
                my $seqid_str = extract_seqid_string($seqid_array);
                
                push @results, [$query_number, $label, $seqid_str];
            }
        } elsif ($search_mode eq 'sequence') {
            while (my ($seqid_array, $seq) = $sth->fetchrow_array()) {
                # Parse PostgreSQL array and extract seqid
                my $seqid_str = extract_seqid_string($seqid_array);
                
                push @results, [$query_number, $label, $seqid_str, $seq];
            }
        } else {
            # matchscore mode
            while (my ($seqid_array, $score) = $sth->fetchrow_array()) {
                # Parse PostgreSQL array and extract seqid
                my $seqid_str = extract_seqid_string($seqid_array);
                
                push @results, [$query_number, $label, $seqid_str, $score];
            }
        }
        
        $sth->finish();
    };
    
    if ($@) {
        print STDERR "Error searching sequence '$label': $@\n";
        return [];
    }
    
    return \@results;
}

sub extract_seqid_string {
    my ($seqid_array) = @_;
    
    return '' unless defined $seqid_array;
    
    # Check if already a Perl array reference (DBD::Pg automatic conversion)
    if (ref($seqid_array) eq 'ARRAY') {
        # Remove quotes and spaces from each seqid
        my @clean_seqids = ();
        for my $seqid (@$seqid_array) {
            $seqid =~ s/["'\s]//g;  # Remove double quotes, single quotes, and spaces
            push @clean_seqids, $seqid;
        }
        return join(',', @clean_seqids);
    }
    
    # Parse PostgreSQL array format {"elem1","elem2",...}
    my @seqids = parse_pg_array($seqid_array);
    
    # Remove quotes and spaces from each seqid
    my @clean_seqids = ();
    for my $seqid (@seqids) {
        $seqid =~ s/["'\s]//g;  # Remove double quotes, single quotes, and spaces
        push @clean_seqids, $seqid;
    }
    
    return join(',', @clean_seqids);
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

sub get_metadata_from_meta {
    my ($dbh) = @_;
    
    my $sth = $dbh->prepare(<<SQL);
SELECT ovllen, kmer_size, occur_bitlen, max_appearance_rate, max_appearance_nrow
FROM kafsss_meta LIMIT 1
SQL
    
    $sth->execute();
    my ($ovllen, $kmer_size, $occur_bitlen, $max_appearance_rate, $max_appearance_nrow) = $sth->fetchrow_array();
    $sth->finish();
    
    if (!defined $ovllen || !defined $kmer_size) {
        die "No metadata found in kafsss_meta table. Please run kafssindex to create indexes first.\n";
    }
    
    return {
        ovllen => $ovllen,
        kmer_size => $kmer_size,
        occur_bitlen => $occur_bitlen,
        max_appearance_rate => $max_appearance_rate,
        max_appearance_nrow => $max_appearance_nrow,
        use_highfreq_cache => 0  # Will be updated by check_highfreq_kmer_exists
    };
}

sub check_highfreq_kmer_exists {
    my ($dbh, $metadata) = @_;
    
    # Check if matching high-frequency k-mer data exists
    my $check_sql = <<SQL;
SELECT COUNT(*) 
FROM kmersearch_highfreq_kmer_meta 
WHERE table_oid = 'kafsss_data'::regclass 
  AND column_name = 'seq'
  AND kmer_size = ?
  AND occur_bitlen = ?
  AND max_appearance_rate = ?
  AND max_appearance_nrow = ?
SQL
    
    my $use_highfreq_cache = 0;
    
    eval {
        my $sth = $dbh->prepare($check_sql);
        $sth->execute(
            $metadata->{kmer_size},
            $metadata->{occur_bitlen} // 0,
            $metadata->{max_appearance_rate} // 0,
            $metadata->{max_appearance_nrow} // 0
        );
        my ($count) = $sth->fetchrow_array();
        $sth->finish();
        
        if ($count > 0) {
            print "Found matching high-frequency k-mer metadata in kmersearch_highfreq_kmer_meta table.\n";
            $use_highfreq_cache = 1;
        } else {
            print "No matching high-frequency k-mer metadata found in kmersearch_highfreq_kmer_meta table.\n";
        }
    };
    
    if ($@) {
        warn "Warning: Failed to check kmersearch_highfreq_kmer_meta table: $@\n";
        warn "Proceeding without high-frequency k-mer exclusion.\n";
    }
    
    # Update metadata with the cache flag
    $metadata->{use_highfreq_cache} = $use_highfreq_cache;
    
    return $use_highfreq_cache;
}

sub get_ovllen_from_meta {
    my ($dbh) = @_;
    
    # Query kafsss_meta table to get ovllen value
    my $sth = $dbh->prepare("SELECT ovllen FROM kafsss_meta LIMIT 1");
    my $ovllen;
    eval {
        $sth->execute();
        ($ovllen) = $sth->fetchrow_array();
        $sth->finish();
        
        if (defined $ovllen) {
            return $ovllen;
        } else {
            die "No ovllen value found in kafsss_meta table\n";
        }
    };
    
    if ($@) {
        die "Failed to retrieve ovllen from kafsss_meta table: $@\n";
    }
    
    return $ovllen;
}

sub get_kmer_size_from_meta {
    my ($dbh) = @_;
    
    # Query kafsss_meta table to get kmer_size value
    my $sth = $dbh->prepare("SELECT kmer_size FROM kafsss_meta LIMIT 1");
    my $kmer_size;
    eval {
        $sth->execute();
        ($kmer_size) = $sth->fetchrow_array();
        $sth->finish();
        
        if (defined $kmer_size) {
            return $kmer_size;
        } else {
            die "No k-mer index found. Please run kafssindex to create indexes first.\n";
        }
    };
    
    if ($@) {
        die "Failed to retrieve kmer_size from kafsss_meta table: $@\n";
    }
    
    return $kmer_size;
}

sub validate_query_sequence {
    my ($sequence, $ovllen, $kmer_size) = @_;
    
    # Check for invalid characters (allow all degenerate nucleotide codes)
    my @invalid_char = $sequence =~ /[^ACGTUMRWSYKVHDBN]/ig;
    if (@invalid_char) {
        my $invalid_chars_str = join('', @invalid_char);
        return {
            valid => 0,
            reason => "Query sequence contains invalid characters '$invalid_chars_str' (only A, C, G, T, U, M, R, W, S, Y, K, V, H, D, B, N are allowed)"
        };
    }
    
    # Check sequence length against ovllen
    my $seq_length = length($sequence);
    if ($seq_length > $ovllen) {
        return {
            valid => 0,
            reason => "Query sequence length ($seq_length bases) exceeds ovllen ($ovllen bases)"
        };
    }
    
    # Check minimum length for pg_kmersearch (requires at least kmer_size bases)
    if ($seq_length < $kmer_size) {
        return {
            valid => 0,
            reason => "Query sequence length ($seq_length bases) is too short (minimum $kmer_size bases required)"
        };
    }
    
    return {
        valid => 1,
        reason => "Valid query sequence"
    };
}

sub normalize_mode {
    my ($mode) = @_;
    return '' unless defined $mode;
    
    # Normalize mode aliases
    my %mode_aliases = (
        'min' => 'minimum',
        'minimize' => 'minimum',
        'minimum' => 'minimum',
        'matchscore' => 'matchscore',
        'score' => 'matchscore',
        'sequence' => 'sequence',
        'seq' => 'sequence',
        'max' => 'maximum',
        'maximize' => 'maximum',
        'maximum' => 'maximum'
    );
    
    my $normalized = $mode_aliases{lc($mode)};
    return '' unless $normalized;
    
    # All modes are accepted for kafsssearch
    return $normalized;
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
    
    # Check if user has SELECT permission on required tables
    my @required_tables = ('kafsss_meta', 'kafsss_data');
    
    for my $table (@required_tables) {
        $sth = $dbh->prepare("SELECT has_table_privilege(?, ?, 'SELECT')");
        $sth->execute($username, $table);
        my $has_select = $sth->fetchrow_array();
        $sth->finish();
        
        unless ($has_select) {
            die "Error: User '$username' does not have SELECT permission on table '$table'.\n" .
                "Please grant permissions:\n" .
                "  sudo -u postgres psql -d " . $dbh->{pg_db} . "\n" .
                "  GRANT SELECT ON $table TO $username;\n" .
                "  \\q\n";
        }
    }
    
    print "Database permissions validated.\n";
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
    
    # Check if database has k-mer indexes (seq column GIN index)
    my $sth = $dbh->prepare("SELECT 1 FROM pg_indexes WHERE tablename = 'kafsss_data' AND indexname = 'idx_kafsss_data_seq_gin' LIMIT 1");
    $sth->execute();
    my $has_kmer_index = $sth->fetchrow_array();
    $sth->finish();
    
    unless ($has_kmer_index) {
        die "Error: Database does not have k-mer indexes.\n" .
            "Please create indexes first using: kafssindex --mode=create $database\n";
    }
    
    print "Database schema validation completed.\n";
}

sub normalize_outfmt {
    my ($outfmt) = @_;
    return '' unless defined $outfmt;

    # Normalize format names (case-insensitive)
    my %format_aliases = (
        'tsv' => 'TSV',
        'TSV' => 'TSV',
        'multitsv' => 'multiTSV',
        'multiTSV' => 'multiTSV',
        'fasta' => 'FASTA',
        'FASTA' => 'FASTA',
        'multifasta' => 'multiFASTA',
        'multiFASTA' => 'multiFASTA',
        'blastdb' => 'BLASTDB',
        'BLASTDB' => 'BLASTDB'
    );

    my $normalized = $format_aliases{$outfmt};
    return $normalized || '';
}

sub write_results_to_file {
    my ($results, $query_number, $output_handles) = @_;

    return unless @$results > 0;

    my $format = $output_handles->{format};
    my $prefix = $output_handles->{prefix};
    my $mode = $output_handles->{mode};

    if ($format eq 'multiTSV') {
        # Write to individual TSV file
        my $filename = "${prefix}_${query_number}.tsv";
        open my $fh, '>', $filename or die "Cannot open output file '$filename': $!\n";
        for my $result (@$results) {
            print $fh join("\t", @$result) . "\n";
        }
        close $fh;
    } elsif ($format eq 'FASTA' || $format eq 'multiFASTA') {
        # Write to individual FASTA file (requires --mode=sequence)
        my $filename = "${prefix}_${query_number}.fasta";
        open my $fh, '>', $filename or die "Cannot open output file '$filename': $!\n";

        # Group results by sequence (in case multiple results have the same sequence)
        my %seq_to_ids = ();
        for my $result (@$results) {
            # Extract seqid list and sequence from result
            my $seqid_str = $result->[2];  # Third column is seqid list
            my $sequence = $result->[3];   # Fourth column in sequence mode

            if ($sequence) {
                push @{$seq_to_ids{$sequence}}, $seqid_str;
            }
        }

        # Write FASTA entries
        for my $sequence (keys %seq_to_ids) {
            my @seqids = @{$seq_to_ids{$sequence}};
            # Replace comma separators within each seqid string with SOH (^A)
            my @converted_seqids = map { s/,/\cA/g; $_ } @seqids;
            # Join seqids with SOH (^A) character
            my $header = join("\cA", @converted_seqids);
            print $fh ">$header\n";
            print $fh "$sequence\n";
        }
        close $fh;
    } elsif ($format eq 'BLASTDB') {
        # Track query number for post-processing
        push @{$output_handles->{query_numbers}}, $query_number;

        if ($mode eq 'minimum') {
            # Write TSV file (same as multiTSV)
            my $filename = "${prefix}_${query_number}.tsv";
            open my $fh, '>', $filename or die "Cannot open output file '$filename': $!\n";
            for my $result (@$results) {
                print $fh join("\t", @$result) . "\n";
            }
            close $fh;
        } elsif ($mode eq 'sequence') {
            # Write FASTA file for makeblastdb
            my $filename = "${prefix}_${query_number}.fasta";
            open my $fh, '>', $filename or die "Cannot open output file '$filename': $!\n";

            for my $result (@$results) {
                my $seqid_str = $result->[2];  # Third column is seqid list
                my $sequence = $result->[3];  # Fourth column in sequence mode

                if ($sequence && $seqid_str) {
                    # Use the first seqid as FASTA header (without position suffix)
                    my @seqids = split(/,/, $seqid_str);
                    my $first_seqid = $seqids[0];
                    $first_seqid =~ s/:\d+:\d+$//;  # Remove position suffix
                    print $fh ">$first_seqid\n";
                    print $fh "$sequence\n";
                }
            }
            close $fh;
        }
    }
}

sub process_temp_file_for_multifile {
    my ($temp_file, $query_number, $output_handles) = @_;
    
    open my $temp_fh, '<', $temp_file or die "Cannot open temporary file '$temp_file': $!\n";
    
    my $format = undef;
    my $mode = undef;
    my @results = ();
    
    while (my $line = <$temp_fh>) {
        chomp $line;
        
        # Check for format header
        if ($line =~ /^#FORMAT:(.+)$/) {
            $format = $1;
            next;
        }
        if ($line =~ /^#MODE:(.+)$/) {
            $mode = $1;
            next;
        }
        
        # Parse TSV result line
        my @fields = split /\t/, $line;
        push @results, \@fields;
    }
    close $temp_fh;
    
    # If format info was in temp file, use it; otherwise use from output_handles
    $format ||= $output_handles->{format};
    $mode ||= $output_handles->{mode};
    
    # Write results to appropriate output file
    if (@results > 0) {
        write_results_to_file(\@results, $query_number, {
            format => $format,
            prefix => $output_handles->{prefix},
            mode => $mode,
            query_numbers => $output_handles->{query_numbers},
            seqid_db => $output_handles->{seqid_db}
        });
    }
}

sub create_blastdb_files {
    my ($output_handles) = @_;

    my $prefix = $output_handles->{prefix};
    my $mode = $output_handles->{mode};
    my $seqid_db = $output_handles->{seqid_db};
    my @query_numbers = @{$output_handles->{query_numbers}};

    print "Post-processing " . scalar(@query_numbers) . " query results...\n";

    # Fork child processes for parallel execution
    my %children = ();
    my $max_parallel = $numthreads > 0 ? $numthreads : 1;

    for my $query_number (@query_numbers) {
        # Wait if we have too many children
        while (scalar(keys %children) >= $max_parallel) {
            my $finished_pid = waitpid(-1, 0);
            if ($finished_pid > 0) {
                my $exit_code = $? >> 8;
                my $qn = delete $children{$finished_pid};
                if ($exit_code != 0) {
                    warn "Warning: BLASTDB creation for query $qn failed with exit code $exit_code\n";
                } else {
                    print "  Completed BLASTDB for query $qn\n";
                }
            }
        }

        my $pid = fork();
        if (!defined $pid) {
            die "Cannot fork for BLASTDB creation: $!\n";
        } elsif ($pid == 0) {
            # Child process
            if ($mode eq 'minimum') {
                create_blastdb_from_tsv($prefix, $query_number, $seqid_db);
            } elsif ($mode eq 'sequence') {
                create_blastdb_from_fasta($prefix, $query_number);
            }
            exit 0;
        } else {
            # Parent process
            $children{$pid} = $query_number;
        }
    }

    # Wait for remaining children
    while (scalar(keys %children) > 0) {
        my $finished_pid = waitpid(-1, 0);
        if ($finished_pid > 0) {
            my $exit_code = $? >> 8;
            my $qn = delete $children{$finished_pid};
            if ($exit_code != 0) {
                warn "Warning: BLASTDB creation for query $qn failed with exit code $exit_code\n";
            } else {
                print "  Completed BLASTDB for query $qn\n";
            }
        }
    }

    print "BLASTDB creation completed.\n";
}

sub create_blastdb_from_tsv {
    my ($prefix, $query_number, $seqid_db) = @_;

    my $tsv_file = "${prefix}_${query_number}.tsv";
    my $acclist_file = "${prefix}_${query_number}.acclist";
    my $bsl_file = "${prefix}_${query_number}.bsl";
    my $nal_file = "${prefix}_${query_number}";

    # Step 1: Extract accession numbers from TSV file
    my %acc = ();
    open my $tsv_fh, '<', $tsv_file or die "Cannot open TSV file '$tsv_file': $!\n";
    while (my $line = <$tsv_fh>) {
        chomp $line;
        my @fields = split /\t/, $line;
        next unless @fields >= 3;
        my $seqidlist = $fields[2];  # Third column is seqid list
        foreach my $seqid (split(/,/, $seqidlist)) {
            $seqid =~ s/:\d+:\d+$//;  # Remove position suffix
            $acc{$seqid} = 1;
        }
    }
    close $tsv_fh;

    # Step 2: Write accession list file
    open my $acc_fh, '>', $acclist_file or die "Cannot open accession list file '$acclist_file': $!\n";
    for my $accession (sort keys %acc) {
        print $acc_fh "$accession\n";
    }
    close $acc_fh;

    # Step 3: Create BSL file using blastdb_aliastool
    my @bsl_cmd = (
        'blastdb_aliastool',
        '-seqid_dbtype', 'nucl',
        '-seqid_db', $seqid_db,
        '-seqid_file_in', $acclist_file,
        '-seqid_title', "${prefix}_${query_number}",
        '-seqid_file_out', $bsl_file
    );
    my $bsl_result = system(@bsl_cmd);
    if ($bsl_result != 0) {
        die "blastdb_aliastool (BSL) failed for query $query_number: exit code " . ($bsl_result >> 8) . "\n";
    }

    # Step 4: Create NAL file using blastdb_aliastool
    my @nal_cmd = (
        'blastdb_aliastool',
        '-dbtype', 'nucl',
        '-db', $seqid_db,
        '-seqidlist', $bsl_file,
        '-out', $nal_file,
        '-title', "${prefix}_${query_number}"
    );
    my $nal_result = system(@nal_cmd);
    if ($nal_result != 0) {
        die "blastdb_aliastool (NAL) failed for query $query_number: exit code " . ($nal_result >> 8) . "\n";
    }
}

sub create_blastdb_from_fasta {
    my ($prefix, $query_number) = @_;

    my $fasta_file = "${prefix}_${query_number}.fasta";
    my $db_name = "${prefix}_${query_number}";

    # Create BLASTDB using makeblastdb
    my @cmd = (
        'makeblastdb',
        '-dbtype', 'nucl',
        '-input_type', 'fasta',
        '-hash_index',
        '-parse_seqids',
        '-max_file_sz', '4G',
        '-in', $fasta_file,
        '-out', $db_name,
        '-title', "${prefix}_${query_number}"
    );
    my $result = system(@cmd);
    if ($result != 0) {
        die "makeblastdb failed for query $query_number: exit code " . ($result >> 8) . "\n";
    }
}