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
my $VERSION = "__VERSION__";

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
my $tmpdir = $ENV{TMPDIR} || '/tmp';
my $verbose = 0;
my $help = 0;

# GIN index selection options (all optional)
my $opt_kmersize;
my $opt_occurbitlen;
my $opt_maxpappear;
my $opt_maxnappear;
my $opt_precludehighfreqkmer;

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
    'tmpdir=s' => \$tmpdir,
    'kmersize=i' => \$opt_kmersize,
    'occurbitlen=i' => \$opt_occurbitlen,
    'maxpappear=f' => \$opt_maxpappear,
    'maxnappear=i' => \$opt_maxnappear,
    'precludehighfreqkmer' => \$opt_precludehighfreqkmer,
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
die "tmpdir '$tmpdir' does not exist or is not a directory\n" unless -d $tmpdir;

# Validate maxpappear precision (max 3 decimal places)
if (defined $opt_maxpappear) {
    my $maxpappear_str = sprintf("%.10f", $opt_maxpappear);
    if ($maxpappear_str =~ /\.\d{4,}[1-9]/) {
        die "Error: --maxpappear value '$opt_maxpappear' has more than 3 decimal places.\n" .
            "Maximum 3 decimal places allowed (e.g., 0.050, 0.125).\n";
    }
}

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

# Get ovllen from kafsss_meta table
my $ovllen = get_ovllen_from_meta($dbh);

# Get available GIN indexes
my $gin_indexes = get_gin_indexes($dbh);

# Build target parameters from command line options
my $target_params = {
    kmer_size => $opt_kmersize,
    occur_bitlen => $opt_occurbitlen,
    max_appearance_rate => $opt_maxpappear,
    max_appearance_nrow => $opt_maxnappear,
    preclude_highfreq_kmer => $opt_precludehighfreqkmer
};

# Select appropriate GIN index
my $selected_index = select_gin_index($dbh, $gin_indexes, $target_params);
my $index_params = $selected_index->{params};

print "Selected GIN index: $selected_index->{index_name}\n";
print "Index parameters: kmer_size=$index_params->{kmer_size}, occur_bitlen=$index_params->{occur_bitlen}, " .
      "max_appearance_rate=$index_params->{max_appearance_rate}, max_appearance_nrow=$index_params->{max_appearance_nrow}, " .
      "preclude_highfreq_kmer=" . ($index_params->{preclude_highfreq_kmer} ? 'true' : 'false') . "\n";

# Build metadata structure for compatibility with existing code
my $metadata = {
    ovllen => $ovllen,
    kmer_size => $index_params->{kmer_size},
    occur_bitlen => $index_params->{occur_bitlen},
    max_appearance_rate => $index_params->{max_appearance_rate},
    max_appearance_nrow => $index_params->{max_appearance_nrow},
    use_highfreq_cache => $index_params->{preclude_highfreq_kmer},
    index_name => $selected_index->{index_name}
};

# Set all kmersearch GUC variables based on selected index
print "Setting kmersearch GUC variables based on selected index...\n";
eval {
    $dbh->do("SET kmersearch.kmer_size = $metadata->{kmer_size}");
    $dbh->do("SET kmersearch.occur_bitlen = $metadata->{occur_bitlen}");
    $dbh->do("SET kmersearch.max_appearance_rate = $metadata->{max_appearance_rate}");
    $dbh->do("SET kmersearch.max_appearance_nrow = $metadata->{max_appearance_nrow}");

    if ($metadata->{use_highfreq_cache}) {
        $dbh->do("SET kmersearch.preclude_highfreq_kmer = true");
        $dbh->do("SET kmersearch.force_use_parallel_highfreq_kmer_cache = true");
    } else {
        $dbh->do("SET kmersearch.preclude_highfreq_kmer = false");
        $dbh->do("SET kmersearch.force_use_parallel_highfreq_kmer_cache = false");
    }

    $dbh->do("SET kmersearch.min_score = $minscore");
    $dbh->do("SET kmersearch.min_shared_kmer_rate = $minpsharedkmer");

    print "GUC variables set successfully.\n";
};
if ($@) {
    die "Failed to set kmersearch GUC variables: $@\n";
}

# Parent process disconnects from database after metadata retrieval (child processes will reconnect)
$dbh->disconnect();
print "Parent process disconnected from database.\n";
$dbh = undef;  # Clear the handle to prevent accidental use

# Prepare output handles based on format
my $output_handles = {};
my $output_fh = undef;

# Get base format for comparison (strip compression suffix)
my $base_outfmt = get_base_format($outfmt);
my $compression_type = get_compression_type($outfmt);

if ($base_outfmt eq 'TSV') {
    # Single TSV file (possibly compressed)
    $output_fh = open_compressed_output_file($output_file, $compression_type);
    # Store compression info for child processes
    if ($compression_type) {
        $output_handles->{compression_type} = $compression_type;
    }
} elsif ($base_outfmt eq 'multiTSV' || $base_outfmt eq 'FASTA' || $base_outfmt eq 'multiFASTA') {
    # Multiple files - handles will be created per query
    # Store the prefix for later use
    $output_handles->{prefix} = $output_file;
    $output_handles->{format} = $outfmt;
    $output_handles->{mode} = $mode;
} elsif ($base_outfmt eq 'BLASTDB') {
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
    
    # Use parallel processing for all cases (fork overhead is negligible)
    my $file_results = process_sequences_parallel_streaming($input_fh, $output_fh, $total_queries, $metadata, $output_handles);
    
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

# Post-processing for BLASTDB format (BLASTDB is created directly by child processes)
if ($outfmt eq 'BLASTDB' && $output_handles->{query_numbers} && @{$output_handles->{query_numbers}} > 0) {
    print "BLASTDB creation completed for " . scalar(@{$output_handles->{query_numbers}}) . " queries.\n";
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
  --tmpdir=DIR      Directory for temporary files (default: \$TMPDIR or /tmp)

GIN index selection options (for databases with multiple indexes):
  --kmersize=INT    K-mer size for index selection
  --occurbitlen=INT Occurrence bit length for index selection
  --maxpappear=REAL Max appearance rate for index selection (max 3 decimal places)
  --maxnappear=INT  Max appearance nrow for index selection
  --precludehighfreqkmer  Select index with preclude_highfreq_kmer=true

  --verbose, -v     Show detailed processing messages (default: false)
  --help, -h        Show this help message

Environment variables:
  PGHOST           PostgreSQL server host
  PGPORT           PostgreSQL server port
  PGUSER           PostgreSQL username
  PGPASSWORD       PostgreSQL password
  TMPDIR           Directory for temporary files (default: /tmp)

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

  # Multiple GIN indexes - specify parameters to select index
  kafsssearch --db=mydb --kmersize=8 query.fasta results.tsv
  kafsssearch --db=mydb --kmersize=8 --precludehighfreqkmer query.fasta results.tsv

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

# Helper: Handle completed child process and store result info
sub handle_completed_child {
    my ($pid, $active_children_ref, $completed_results_ref) = @_;

    return unless exists $active_children_ref->{$pid};

    my $child_info = delete $active_children_ref->{$pid};
    my $query_number = $child_info->{query_number};
    my $temp_file = $child_info->{temp_file};

    if ($? != 0) {
        die "Child process $pid (query $query_number) failed with exit code " . ($? >> 8) . "\n";
    }

    # Store temp file path only - count will be done during output
    $completed_results_ref->{$query_number} = { file => $temp_file };
    print "Completed query $query_number\n";
}

# Helper: Output one query result and return count
sub output_query_result {
    my ($result_info, $query_number, $output_fh, $output_handles) = @_;
    my $result_count = 0;

    return 0 unless $result_info->{file} && -f $result_info->{file};

    if ($output_handles && $output_handles->{format}) {
        # Multi-file output - read and write to separate files
        $result_count = process_temp_file_for_multifile($result_info->{file}, $query_number, $output_handles);
    } elsif ($output_handles && $output_handles->{compression_type}) {
        # Compressed single file output - binary concatenate (gzip streams are concatenatable)
        concatenate_binary_file($result_info->{file}, $output_fh);
        # Read count from meta file if exists
        my $count_file = $result_info->{file} . '.count';
        if (-f $count_file) {
            open my $cfh, '<', $count_file;
            $result_count = <$cfh> + 0;
            close $cfh;
            unlink $count_file;
        }
    } else {
        # Single file output - stream directly and count
        open my $temp_fh, '<', $result_info->{file} or die "Cannot open temporary file '$result_info->{file}': $!\n";
        while (my $line = <$temp_fh>) {
            chomp $line;
            print $output_fh "$line\n";
            $result_count++;
        }
        close $temp_fh;
    }
    unlink $result_info->{file};

    print "Output query $query_number ($result_count results)\n" if $result_count > 0;
    return $result_count;
}

sub process_sequences_parallel_streaming {
    my ($input_fh, $output_fh, $query_offset, $metadata, $output_handles) = @_;
    $query_offset ||= 0;

    my %active_children = ();  # pid => {query_number, temp_file}
    my %completed_results = (); # query_number => {file => path}
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
            my $base_format = get_base_format($outfmt);
            my $temp_file;
            if ($base_format eq 'BLASTDB') {
                # BLASTDB: No temp file needed, just need a path for count file
                $temp_file = "$tmpdir/kafsssearch_$$" . "_$global_query_number";
            } else {
                # Other formats: Create actual temp file in tmpdir
                my ($temp_fh, $tf) = tempfile("kafsssearch_$$" . "_$global_query_number" . "_XXXXXX", DIR => $tmpdir, UNLINK => 0);
                close($temp_fh);
                $temp_file = $tf;
            }

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
        handle_completed_child($pid, \%active_children, \%completed_results) if $pid > 0;

        # Output completed results in order
        while (exists $completed_results{$next_output_query}) {
            my $result_info = delete $completed_results{$next_output_query};
            $total_results += output_query_result($result_info, $next_output_query, $output_fh, $output_handles);
            $next_output_query++;
        }

        # Exit condition: no more input and no active children
        if (!$fasta_entry && scalar(keys %active_children) == 0) {
            last;
        }

        # If we have active children but no available slots, wait for at least one to complete
        if (scalar(keys %active_children) >= $numthreads) {
            my $pid = waitpid(-1, 0);  # Blocking wait
            handle_completed_child($pid, \%active_children, \%completed_results) if $pid > 0;
        }
    }

    # Output any remaining completed results
    while (exists $completed_results{$next_output_query}) {
        my $result_info = delete $completed_results{$next_output_query};
        $total_results += output_query_result($result_info, $next_output_query, $output_fh, $output_handles);
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

    # Set all kmersearch GUC variables based on metadata from parent
    eval {
        $child_dbh->do("SET kmersearch.kmer_size = $metadata->{kmer_size}");
        $child_dbh->do("SET kmersearch.occur_bitlen = $metadata->{occur_bitlen}");
        $child_dbh->do("SET kmersearch.max_appearance_rate = $metadata->{max_appearance_rate}");
        $child_dbh->do("SET kmersearch.max_appearance_nrow = $metadata->{max_appearance_nrow}");

        if ($metadata->{use_highfreq_cache}) {
            $child_dbh->do("SET kmersearch.preclude_highfreq_kmer = true");
            $child_dbh->do("SET kmersearch.force_use_parallel_highfreq_kmer_cache = true");
        } else {
            $child_dbh->do("SET kmersearch.preclude_highfreq_kmer = false");
            $child_dbh->do("SET kmersearch.force_use_parallel_highfreq_kmer_cache = false");
        }

        $child_dbh->do("SET kmersearch.min_score = $minscore");
        $child_dbh->do("SET kmersearch.min_shared_kmer_rate = $minpsharedkmer");
    };
    if ($@) {
        die "Failed to set kmersearch GUC variables in child process: $@\n";
    }

    # Search and write results directly to temp file (streaming)
    my $result_count = search_and_write_results($fasta_entry, $child_dbh, $query_number, $temp_file, $metadata, $output_handles);

    # Check for no matches and report to STDERR
    if ($result_count == 0) {
        print STDERR "No matches found for query $query_number: " . $fasta_entry->{label} . "\n";
    }

    $child_dbh->disconnect();
}

# Streaming search: write results directly to file instead of buffering in memory
sub search_and_write_results {
    my ($fasta_entry, $dbh, $query_number, $temp_file, $metadata, $output_handles) = @_;

    my $label = $fasta_entry->{label};
    my $sequence = $fasta_entry->{sequence};
    my $ovllen_value = $metadata->{ovllen};
    my $kmer_size = $metadata->{kmer_size};
    my $search_mode = $mode;

    # Validate query sequence
    my $validation_result = validate_query_sequence($sequence, $ovllen_value, $kmer_size);
    if (!$validation_result->{valid}) {
        print STDERR "Warning: Skipping query $query_number '$label': $validation_result->{reason}\n";
        # Create empty temp file
        open my $temp_fh, '>', $temp_file or die "Cannot open temporary file '$temp_file': $!\n";
        close $temp_fh;
        return 0;
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
            $sql = "SELECT seqid FROM kafsss_data $where_clause";
        } else {
            $sql = "SELECT seqid FROM kafsss_data $where_clause ORDER BY kmersearch_matchscore(seq, ?) DESC LIMIT ?";
            push @params, $sequence, $maxnseq;
        }
    } elsif ($search_mode eq 'sequence') {
        if ($maxnseq == 0) {
            $sql = "SELECT seqid, seq FROM kafsss_data $where_clause";
        } else {
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

    my $result_count = 0;

    # Determine output format and compression
    my $base_format = '';
    my $compression_type = undef;
    my $output_format = 'TSV';  # Default

    if ($output_handles) {
        if ($output_handles->{format}) {
            $base_format = get_base_format($output_handles->{format});
            $compression_type = get_compression_type($output_handles->{format});
            $output_format = $base_format;
        } elsif ($output_handles->{compression_type}) {
            # Single TSV with compression
            $compression_type = $output_handles->{compression_type};
            $output_format = 'TSV';
        }
    }

    # For BLASTDB, write directly to makeblastdb (no temp file)
    if ($output_format eq 'BLASTDB') {
        return write_blastdb_directly($dbh, $sql, \@params, $search_mode, $query_number, $label, $temp_file, $output_handles);
    }

    # Open temp file for streaming write (with compression if needed)
    my $temp_fh;
    if ($compression_type) {
        my $cmd = get_compression_command($compression_type);
        open($temp_fh, "| $cmd > '$temp_file'") or die "Cannot open compressed temp file '$temp_file': $!\n";
    } else {
        open($temp_fh, '>', $temp_file) or die "Cannot open temporary file '$temp_file': $!\n";
    }

    eval {
        my $sth = $dbh->prepare($sql);
        $sth->execute(@params);

        # Write in final output format
        if ($output_format eq 'multiFASTA' || $output_format eq 'FASTA') {
            # FASTA format: group by sequence, output as FASTA
            my %seq_to_ids = ();
            if ($search_mode eq 'maximum' || $search_mode eq 'sequence') {
                while (my @row = $sth->fetchrow_array()) {
                    my $seqid_str = extract_seqid_string($row[0]);
                    my $seq = ($search_mode eq 'maximum') ? $row[2] : $row[1];
                    push @{$seq_to_ids{$seq}}, $seqid_str;
                    $result_count++;
                }
            }
            # Write FASTA entries
            for my $sequence (keys %seq_to_ids) {
                my @seqids = @{$seq_to_ids{$sequence}};
                my @converted_seqids = map { my $s = $_; $s =~ s/,/\cA/g; $s } @seqids;
                my $header = join("\cA", @converted_seqids);
                print $temp_fh ">$header\n$sequence\n";
            }
        } else {
            # TSV format (TSV, multiTSV)
            if ($search_mode eq 'maximum') {
                while (my ($seqid_array, $score, $seq) = $sth->fetchrow_array()) {
                    my $seqid_str = extract_seqid_string($seqid_array);
                    print $temp_fh join("\t", $query_number, $label, $seqid_str, $score, $seq) . "\n";
                    $result_count++;
                }
            } elsif ($search_mode eq 'minimum') {
                while (my ($seqid_array) = $sth->fetchrow_array()) {
                    my $seqid_str = extract_seqid_string($seqid_array);
                    print $temp_fh join("\t", $query_number, $label, $seqid_str) . "\n";
                    $result_count++;
                }
            } elsif ($search_mode eq 'sequence') {
                while (my ($seqid_array, $seq) = $sth->fetchrow_array()) {
                    my $seqid_str = extract_seqid_string($seqid_array);
                    print $temp_fh join("\t", $query_number, $label, $seqid_str, $seq) . "\n";
                    $result_count++;
                }
            } else {
                # matchscore mode
                while (my ($seqid_array, $score) = $sth->fetchrow_array()) {
                    my $seqid_str = extract_seqid_string($seqid_array);
                    print $temp_fh join("\t", $query_number, $label, $seqid_str, $score) . "\n";
                    $result_count++;
                }
            }
        }

        $sth->finish();
    };

    close $temp_fh;

    if ($@) {
        print STDERR "Error searching sequence '$label': $@\n";
        return 0;
    }

    # Write result count to meta file (parent needs it for compressed or multifile output)
    if ($compression_type || $output_format =~ /^multi/) {
        open my $cfh, '>', "$temp_file.count" or die "Cannot write count file: $!\n";
        print $cfh $result_count;
        close $cfh;
    }

    return $result_count;
}

# Write BLASTDB directly via pipe to makeblastdb (no temp file needed)
sub write_blastdb_directly {
    my ($dbh, $sql, $params, $search_mode, $query_number, $label, $temp_file, $output_handles) = @_;

    my $result_count = 0;
    my $prefix = $output_handles->{prefix};
    my $db_name = "${prefix}_${query_number}";

    if ($search_mode eq 'sequence' || $search_mode eq 'maximum') {
        # Create BLASTDB from sequences via pipe
        my @cmd = (
            'makeblastdb',
            '-dbtype', 'nucl',
            '-input_type', 'fasta',
            '-hash_index',
            '-parse_seqids',
            '-in', '-',
            '-out', $db_name,
            '-title', $db_name
        );

        open my $pipe, '|-', @cmd or die "Cannot open pipe to makeblastdb: $!\n";

        eval {
            my $sth = $dbh->prepare($sql);
            $sth->execute(@$params);

            while (my @row = $sth->fetchrow_array()) {
                my $seqid_str = extract_seqid_string($row[0]);
                my $seq = ($search_mode eq 'maximum') ? $row[2] : $row[1];
                # Use first seqid for FASTA header
                my $first_seqid = (split /,/, $seqid_str)[0];
                print $pipe ">$first_seqid\n$seq\n";
                $result_count++;
            }
            $sth->finish();
        };

        close $pipe or warn "makeblastdb failed: $!\n";
    } else {
        # minimum/matchscore mode - create alias using blastdb_aliastool
        # This requires seqid_db to be set
        my $seqid_db = $output_handles->{seqid_db};
        if ($seqid_db) {
            my @seqids = ();
            eval {
                my $sth = $dbh->prepare($sql);
                $sth->execute(@$params);
                while (my ($seqid_array) = $sth->fetchrow_array()) {
                    my $seqid_str = extract_seqid_string($seqid_array);
                    push @seqids, split /,/, $seqid_str;
                    $result_count++;
                }
                $sth->finish();
            };

            if (@seqids > 0) {
                my $seqid_list = join(',', @seqids);
                system('blastdb_aliastool', '-dbtype', 'nucl', '-db', $seqid_db,
                       '-seqidlist_filter', $seqid_list, '-out', $db_name, '-title', $db_name);
            }
        }
    }

    # Write result count (no temp file needed for BLASTDB, only count file)
    open my $cfh, '>', "$temp_file.count" or die "Cannot write count file: $!\n";
    print $cfh $result_count;
    close $cfh;

    return $result_count;
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
    my $sth = $dbh->prepare(<<SQL);
SELECT 1 FROM pg_indexes
WHERE tablename = 'kafsss_data'
  AND indexname LIKE 'idx_kafsss_data_seq_gin_km%'
LIMIT 1
SQL
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
        # Uncompressed formats
        'tsv' => 'TSV',
        'TSV' => 'TSV',
        'multitsv' => 'multiTSV',
        'multiTSV' => 'multiTSV',
        'fasta' => 'FASTA',
        'FASTA' => 'FASTA',
        'multifasta' => 'multiFASTA',
        'multiFASTA' => 'multiFASTA',
        'blastdb' => 'BLASTDB',
        'BLASTDB' => 'BLASTDB',

        # Compressed TSV formats
        'tsv.gz' => 'TSV.gz',
        'TSV.gz' => 'TSV.gz',
        'tsv.bz2' => 'TSV.bz2',
        'TSV.bz2' => 'TSV.bz2',
        'tsv.xz' => 'TSV.xz',
        'TSV.xz' => 'TSV.xz',
        'tsv.zst' => 'TSV.zst',
        'TSV.zst' => 'TSV.zst',
        'tsv.zstd' => 'TSV.zst',
        'TSV.zstd' => 'TSV.zst',

        # Compressed multiTSV formats
        'multitsv.gz' => 'multiTSV.gz',
        'multiTSV.gz' => 'multiTSV.gz',
        'multitsv.bz2' => 'multiTSV.bz2',
        'multiTSV.bz2' => 'multiTSV.bz2',
        'multitsv.xz' => 'multiTSV.xz',
        'multiTSV.xz' => 'multiTSV.xz',
        'multitsv.zst' => 'multiTSV.zst',
        'multiTSV.zst' => 'multiTSV.zst',
        'multitsv.zstd' => 'multiTSV.zst',
        'multiTSV.zstd' => 'multiTSV.zst',

        # Compressed FASTA formats
        'fasta.gz' => 'FASTA.gz',
        'FASTA.gz' => 'FASTA.gz',
        'fasta.bz2' => 'FASTA.bz2',
        'FASTA.bz2' => 'FASTA.bz2',
        'fasta.xz' => 'FASTA.xz',
        'FASTA.xz' => 'FASTA.xz',
        'fasta.zst' => 'FASTA.zst',
        'FASTA.zst' => 'FASTA.zst',
        'fasta.zstd' => 'FASTA.zst',
        'FASTA.zstd' => 'FASTA.zst',

        # Compressed multiFASTA formats
        'multifasta.gz' => 'multiFASTA.gz',
        'multiFASTA.gz' => 'multiFASTA.gz',
        'multifasta.bz2' => 'multiFASTA.bz2',
        'multiFASTA.bz2' => 'multiFASTA.bz2',
        'multifasta.xz' => 'multiFASTA.xz',
        'multiFASTA.xz' => 'multiFASTA.xz',
        'multifasta.zst' => 'multiFASTA.zst',
        'multiFASTA.zst' => 'multiFASTA.zst',
        'multifasta.zstd' => 'multiFASTA.zst',
        'multiFASTA.zstd' => 'multiFASTA.zst',
    );

    my $normalized = $format_aliases{$outfmt};
    return $normalized || '';
}

# Get compression type from format (e.g., 'TSV.gz' -> 'gz')
sub get_compression_type {
    my ($format) = @_;
    if ($format =~ /\.(gz|bz2|xz|zst)$/i) {
        return lc($1);
    }
    return undef;
}

# Get base format without compression suffix (e.g., 'TSV.gz' -> 'TSV')
sub get_base_format {
    my ($format) = @_;
    $format =~ s/\.(gz|bz2|xz|zst)$//i;
    return $format;
}

# Get compression command for a given compression type
sub get_compression_command {
    my ($type) = @_;

    my %commands = (
        'gz'  => 'pigz',
        'bz2' => 'pbzip2',
        'xz'  => 'xz',
        'zst' => 'zstd',
    );

    return $commands{$type} || die "Unknown compression type: $type\n";
}

# Validate output filename extension matches compression type
sub validate_output_filename {
    my ($filename, $compression_type) = @_;

    # Skip check for stdout
    return if ($filename eq '-' || $filename eq 'stdout' || $filename eq 'STDOUT');

    my %expected_extensions = (
        'gz'  => qr/\.gz$/i,
        'bz2' => qr/\.bz2$/i,
        'xz'  => qr/\.xz$/i,
        'zst' => qr/\.zstd?$/i,  # Allow both .zst and .zstd
    );

    if ($compression_type && exists $expected_extensions{$compression_type}) {
        unless ($filename =~ $expected_extensions{$compression_type}) {
            my $ext_hint = $compression_type;
            $ext_hint = 'zst or .zstd' if $compression_type eq 'zst';
            die "Error: Output file '$filename' must have .$ext_hint extension " .
                "when using compressed output format.\n";
        }
    }
}

# Check if format supports stdout output
sub check_stdout_compatibility {
    my ($filename, $format) = @_;

    if ($filename eq '-' || $filename eq 'stdout' || $filename eq 'STDOUT') {
        my $base_format = get_base_format($format);
        if ($base_format =~ /^(multiTSV|FASTA|multiFASTA)$/i) {
            die "Error: Format '$format' does not support output to stdout.\n" .
                "Please specify an output file path.\n";
        }
    }
}

# Open output file, potentially with compression
sub open_compressed_output_file {
    my ($filename, $compression_type) = @_;
    my $fh;

    if ($filename eq '-' || $filename eq 'stdout' || $filename eq 'STDOUT') {
        # Standard output
        if ($compression_type) {
            # Compressed stdout
            my $cmd = get_compression_command($compression_type);
            open($fh, "| $cmd") or die "Cannot pipe to $cmd: $!\n";
        } else {
            $fh = \*STDOUT;
        }
    } else {
        # File output
        if ($compression_type) {
            my $cmd = get_compression_command($compression_type);
            open($fh, "| $cmd > '$filename'") or die "Cannot open pipe to $cmd for $filename: $!\n";
        } else {
            open($fh, '>', $filename) or die "Cannot open $filename for writing: $!\n";
        }
    }

    return $fh;
}

# Get file extension for output based on format (e.g., 'multiTSV.gz' -> '.tsv.gz')
sub get_output_extension {
    my ($format) = @_;

    my $compression_type = get_compression_type($format);
    my $base_format = get_base_format($format);

    my %base_extensions = (
        'TSV' => '.tsv',
        'multiTSV' => '.tsv',
        'FASTA' => '.fasta',
        'multiFASTA' => '.fasta',
        'BLASTDB' => '',
    );

    my $ext = $base_extensions{$base_format} || '';
    if ($compression_type) {
        $ext .= ".$compression_type";
    }
    return $ext;
}

# Concatenate binary file to destination (for compressed file concatenation)
sub concatenate_binary_file {
    my ($src_file, $dst_fh) = @_;
    open my $src_fh, '<:raw', $src_file or die "Cannot open '$src_file' for binary read: $!\n";
    binmode $dst_fh;
    while (read($src_fh, my $buffer, 65536)) {
        print $dst_fh $buffer;
    }
    close $src_fh;
}

# Rename temp file to final destination (same filesystem) or copy+delete (cross filesystem)
sub move_temp_to_final {
    my ($temp_file, $final_file) = @_;
    if (!rename($temp_file, $final_file)) {
        # rename failed, likely cross-filesystem - use copy and delete
        require File::Copy;
        File::Copy::move($temp_file, $final_file)
            or die "Cannot move '$temp_file' to '$final_file': $!\n";
    }
}

sub process_temp_file_for_multifile {
    my ($temp_file, $query_number, $output_handles) = @_;

    my $format = $output_handles->{format};
    my $base_format = get_base_format($format);
    my $compression_type = get_compression_type($format);

    # Read count from meta file
    my $result_count = 0;
    my $count_file = "$temp_file.count";
    if (-f $count_file) {
        open my $cfh, '<', $count_file;
        $result_count = <$cfh> + 0;
        close $cfh;
        unlink $count_file;
    }

    # BLASTDB is already created directly by child process
    if ($base_format eq 'BLASTDB') {
        unlink $temp_file if -f $temp_file;  # Remove marker file
        return $result_count;
    }

    # Determine file extension based on format
    my $ext;
    if ($base_format eq 'multiTSV') {
        $ext = $compression_type ? ".tsv.$compression_type" : ".tsv";
    } elsif ($base_format eq 'multiFASTA' || $base_format eq 'FASTA') {
        $ext = $compression_type ? ".fasta.$compression_type" : ".fasta";
    } else {
        $ext = $compression_type ? ".$compression_type" : "";
    }

    my $final_file = $output_handles->{prefix} . "_${query_number}${ext}";

    # Rename temp file to final file (temp file is already in final format)
    if (-f $temp_file && -s $temp_file) {
        move_temp_to_final($temp_file, $final_file);
    } else {
        unlink $temp_file if -f $temp_file;  # Remove empty file
    }

    return $result_count;
}

# Get list of GIN indexes on kafsss_data.seq column
sub get_gin_indexes {
    my ($dbh) = @_;

    my $sth = $dbh->prepare(<<SQL);
SELECT indexname
FROM pg_indexes
WHERE tablename = 'kafsss_data'
  AND indexname LIKE 'idx_kafsss_data_seq_gin_km%'
ORDER BY indexname
SQL
    $sth->execute();

    my @indexes = ();
    while (my ($indexname) = $sth->fetchrow_array()) {
        push @indexes, $indexname;
    }
    $sth->finish();

    return \@indexes;
}

# Parse GIN index name to extract parameters
sub parse_gin_index_name {
    my ($indexname) = @_;

    if ($indexname =~ /idx_kafsss_data_seq_gin_km(\d+)_ob(\d+)_mar(\d{4})_man(\d+)_phk([TF])/) {
        return {
            kmer_size => int($1),
            occur_bitlen => int($2),
            max_appearance_rate => $3 / 1000,
            max_appearance_nrow => int($4),
            preclude_highfreq_kmer => ($5 eq 'T' ? 1 : 0)
        };
    }

    return undef;
}

# Select appropriate GIN index based on target parameters
sub select_gin_index {
    my ($dbh, $indexes, $target_params) = @_;

    my $index_count = scalar(@$indexes);

    if ($index_count == 0) {
        die "Error: No GIN indexes found on kafsss_data.seq column.\n" .
            "Please create indexes first using: kafssindex --mode=create $database\n";
    }

    # If only one index exists, use it
    if ($index_count == 1) {
        my $parsed = parse_gin_index_name($indexes->[0]);
        if (!$parsed) {
            die "Error: Cannot parse GIN index name: $indexes->[0]\n";
        }
        return {
            index_name => $indexes->[0],
            params => $parsed
        };
    }

    # Multiple indexes - filter by target parameters
    my @matching_indexes = ();

    for my $indexname (@$indexes) {
        my $parsed = parse_gin_index_name($indexname);
        next unless $parsed;

        my $matches = 1;

        if (defined $target_params->{kmer_size}) {
            $matches = 0 if $parsed->{kmer_size} != $target_params->{kmer_size};
        }
        if (defined $target_params->{occur_bitlen}) {
            $matches = 0 if $parsed->{occur_bitlen} != $target_params->{occur_bitlen};
        }
        if (defined $target_params->{max_appearance_rate}) {
            $matches = 0 if abs($parsed->{max_appearance_rate} - $target_params->{max_appearance_rate}) >= 0.0001;
        }
        if (defined $target_params->{max_appearance_nrow}) {
            $matches = 0 if $parsed->{max_appearance_nrow} != $target_params->{max_appearance_nrow};
        }
        if (defined $target_params->{preclude_highfreq_kmer}) {
            $matches = 0 if $parsed->{preclude_highfreq_kmer} != $target_params->{preclude_highfreq_kmer};
        }

        if ($matches) {
            push @matching_indexes, {
                index_name => $indexname,
                params => $parsed
            };
        }
    }

    my $match_count = scalar(@matching_indexes);

    if ($match_count == 0) {
        # Build helpful error message listing available indexes
        my @available = ();
        for my $indexname (@$indexes) {
            my $parsed = parse_gin_index_name($indexname);
            if ($parsed) {
                push @available, sprintf("  - %s (kmersize=%d, occurbitlen=%d, maxpappear=%.3f, maxnappear=%d, precludehighfreqkmer=%s)",
                    $indexname,
                    $parsed->{kmer_size},
                    $parsed->{occur_bitlen},
                    $parsed->{max_appearance_rate},
                    $parsed->{max_appearance_nrow},
                    $parsed->{preclude_highfreq_kmer} ? 'true' : 'false'
                );
            }
        }
        die "Error: No matching GIN index found for the specified parameters.\n" .
            "Available indexes:\n" . join("\n", @available) . "\n" .
            "Please specify parameters that match one of the available indexes.\n";
    }

    if ($match_count == 1) {
        return $matching_indexes[0];
    }

    # Multiple matches - need more specific parameters
    my @match_names = map { "  - $_->{index_name}" } @matching_indexes;
    die "Error: Multiple GIN indexes match the specified parameters.\n" .
        "Matching indexes:\n" . join("\n", @match_names) . "\n" .
        "Please specify additional parameters (--kmersize, --occurbitlen, --maxpappear, --maxnappear, --precludehighfreqkmer) to uniquely identify the index.\n";
}