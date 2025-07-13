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
my $default_maxnseq = 1000;
my $default_numthreads = 1;
my $default_mode = 'normal';
my $default_minpsharedkey = 0.9;

# Command line options
my $host = $default_host;
my $port = $default_port;
my $username = $default_user;
my $database = '';
my $partition = '';
my $maxnseq = $default_maxnseq;
my $minscore = undef;
my $minpsharedkey = $default_minpsharedkey;
my $numthreads = $default_numthreads;
my $mode = $default_mode;
my $help = 0;

# Parse command line options
GetOptions(
    'host=s' => \$host,
    'port=i' => \$port,
    'username=s' => \$username,
    'db=s' => \$database,
    'partition=s' => \$partition,
    'maxnseq=i' => \$maxnseq,
    'minscore=i' => \$minscore,
    'minpsharedkey=f' => \$minpsharedkey,
    'numthreads=i' => \$numthreads,
    'mode=s' => \$mode,
    'help|h' => \$help,
) or die "Error in command line arguments\n";

# Show help
if ($help) {
    print_help();
    exit 0;
}

# Check required arguments
if (@ARGV < 2) {
    die "Usage: af_kmersearch [options] input_file(s) output_file\n" .
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
die "maxnseq must be positive integer\n" unless $maxnseq > 0;
die "minscore must be positive integer\n" if defined $minscore && $minscore <= 0;
die "minpsharedkey must be between 0.0 and 1.0\n" unless $minpsharedkey >= 0.0 && $minpsharedkey <= 1.0;
die "numthreads must be positive integer\n" unless $numthreads > 0;

# Validate mode
my $normalized_mode = normalize_mode($mode);
if (!$normalized_mode) {
    die "Invalid mode: $mode. Must be 'minimum', 'normal', or 'maximum'\n";
}
$mode = $normalized_mode;

print "af_kmersearch version $VERSION\n";
print "Input files (" . scalar(@input_files) . "):\n";
for my $i (0..$#input_files) {
    print "  " . ($i + 1) . ". $input_files[$i]\n";
}
print "Output file: $output_file\n";
print "Database: $database\n";
print "Host: $host\n";
print "Port: $port\n";
print "Username: $username\n";
print "Partition: " . ($partition ? $partition : 'all') . "\n";
print "Max sequences: $maxnseq\n";
print "Min score: " . (defined $minscore ? $minscore : 'default') . "\n";
print "Min shared key rate: $minpsharedkey\n";
print "Number of threads: $numthreads\n";
print "Mode: $mode\n";

# Connect to PostgreSQL database
my $password = $ENV{PGPASSWORD} || '';
my $dsn = "DBI:Pg:dbname=$database;host=$host;port=$port";

my $dbh = DBI->connect($dsn, $username, $password, {
    RaiseError => 1,
    AutoCommit => 1,
    pg_enable_utf8 => 1
}) or die "Cannot connect to database '$database': $DBI::errstr\n";

print "Connected to database successfully.\n";

# Verify database structure
verify_database_structure($dbh);

# Get k-mer size from af_kmersearch_meta table
my $kmer_size = get_kmer_size_from_meta($dbh);
print "Retrieved k-mer size: $kmer_size\n";

# Set k-mer size for pg_kmersearch
print "Setting k-mer size to $kmer_size...\n";
eval {
    $dbh->do("SET kmersearch.kmer_size = $kmer_size");
    print "K-mer size set to $kmer_size successfully.\n";
};
if ($@) {
    die "Failed to set k-mer size: $@\n";
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

# Set minimum shared key rate
print "Setting minimum shared key rate to $minpsharedkey...\n";
eval {
    $dbh->do("SET kmersearch.min_shared_ngram_key_rate = $minpsharedkey");
    print "Minimum shared key rate set to $minpsharedkey successfully.\n";
};
if ($@) {
    die "Failed to set minimum shared key rate: $@\n";
}

# Set rawscore cache max entries (maxnseq * 2)
my $rawscore_cache_max_entries = $maxnseq * 2;
print "Setting rawscore cache max entries to $rawscore_cache_max_entries...\n";
eval {
    $dbh->do("SET kmersearch.rawscore_cache_max_entries = $rawscore_cache_max_entries");
    print "Rawscore cache max entries set to $rawscore_cache_max_entries successfully.\n";
};
if ($@) {
    die "Failed to set rawscore cache max entries: $@\n";
}

# Get ovllen from af_kmersearch_meta table for query validation
my $ovllen = get_ovllen_from_meta($dbh);
print "Retrieved ovllen value: $ovllen\n";

# Open output file
my $output_fh = open_output_file($output_file);

# Process FASTA sequences from multiple files
print "Processing FASTA sequences...\n";
my $total_results = 0;
my $total_queries = 0;

for my $i (0..$#input_files) {
    my $input_file = $input_files[$i];
    print "Processing file " . ($i + 1) . "/" . scalar(@input_files) . ": $input_file\n";
    
    # Open current input file
    my $input_fh = open_input_file($input_file);
    
    my $file_results;
    if ($numthreads == 1) {
        # Single-threaded processing
        $file_results = process_sequences_single_threaded($input_fh, $output_fh, $dbh, $total_queries);
    } else {
        # Multi-threaded processing with process pool
        $file_results = process_sequences_parallel_streaming($input_fh, $output_fh, $total_queries);
    }
    
    $total_results += $file_results->{results};
    $total_queries += $file_results->{queries};
    
    close_input_file($input_fh, $input_file);
    
    print "  Processed " . $file_results->{queries} . " queries, " . $file_results->{results} . " results from this file.\n";
}

# Close files and database connection
close_output_file($output_fh, $output_file);
$dbh->disconnect();

print "Processing completed successfully.\n";
print "Total queries processed: $total_queries\n";
print "Total results output: $total_results\n";

exit 0;

#
# Subroutines
#

sub print_help {
    print <<EOF;
af_kmersearch version $VERSION

Usage: af_kmersearch [options] input_file(s) output_file

Search DNA sequences from multiple sources against af_kmersearch database using k-mer similarity.

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
  --partition=NAME  Limit search to specific partition (optional)
  --maxnseq=INT     Maximum number of results per query (default: 1000)
  --minscore=INT    Minimum score threshold (optional, uses kmersearch.min_score GUC variable)
  --minpsharedkey=REAL  Minimum percentage of shared keys (0.0-1.0, default: 0.9)
  --numthreads=INT  Number of parallel threads (default: 1)
  --mode=MODE       Output mode: minimum, normal, maximum (default: normal)
  --help, -h        Show this help message

Environment variables:
  PGHOST           PostgreSQL server host
  PGPORT           PostgreSQL server port
  PGUSER           PostgreSQL username
  PGPASSWORD       PostgreSQL password

Output format:
  Tab-separated values with columns:
  1. Query sequence number (1-based integer)
  2. Query FASTA label
  3. Corrected score from kmersearch_correctedscore function
  4. Comma-separated seqid list from seqid column
  5. Sequence data (only in maximum mode)

Examples:
  # Single file
  af_kmersearch --db=mydb query.fasta results.tsv
  
  # Multiple files
  af_kmersearch --db=mydb file1.fasta file2.fasta results.tsv
  
  # Wildcard pattern (use quotes to prevent shell expansion)
  af_kmersearch --db=mydb 'queries/*.fasta' results.tsv
  
  # Compressed files
  af_kmersearch --db=mydb query.fasta.gz results.tsv
  af_kmersearch --db=mydb 'data/*.fasta.bz2' results.tsv
  
  # BLAST database
  af_kmersearch --db=mydb nr results.tsv
  
  # Mixed sources
  af_kmersearch --db=mydb file1.fasta 'data/*.gz' blastdb results.tsv
  
  # With options
  af_kmersearch --db=mydb --partition=bacteria 'queries/*.fasta' results.tsv
  af_kmersearch --db=mydb --maxnseq=500 --minscore=10 query.fasta results.tsv
  af_kmersearch --db=mydb --numthreads=4 --mode=maximum 'data/*.fasta' results.tsv
  
  # Standard input
  cat query.fasta | af_kmersearch --db=mydb stdin stdout > results.tsv

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
    
    # Check if af_kmersearch table exists
    $sth = $dbh->prepare(<<SQL);
SELECT COUNT(*)
FROM information_schema.tables 
WHERE table_name = 'af_kmersearch'
SQL
    $sth->execute();
    my ($table_count) = $sth->fetchrow_array();
    $sth->finish();
    
    die "Table 'af_kmersearch' does not exist in database '$database'\n" 
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
        # BLAST nucleotide database
        open my $fh, '-|', 'blastdbcmd', '-db', $filename, '-dbtype', 'nucl', '-entry', 'all', '-out', '-', '-outfmt', '>%a\n%s\n', '-line_length', '1000000', '-target_only' or die "Cannot open BLAST database '$filename': $!\n";
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
    close $fh or warn "Warning: Could not close file handle for '$filename': $!\n";
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
    
    # Parse the FASTA record using regex that handles optional leading '>'
    # and captures label (up to first newline) and sequence (rest, may contain newlines)
    if ($line =~ /^>?\s*(\S[^\r\n]*)\r?\n(.*)/s) {
        my $label = $1;
        my $sequence = $2;
        
        # Replace tab characters in label with 4 spaces to prevent TSV format corruption
        $label =~ s/\t/    /g;
        
        # Remove all whitespace (including newlines) from sequence
        $sequence =~ s/\s+//gs;
        
        return {
            label => $label,
            sequence => $sequence
        };
    }
    
    return undef;  # Invalid FASTA format
}

sub process_sequences_single_threaded {
    my ($input_fh, $output_fh, $dbh, $query_offset) = @_;
    $query_offset ||= 0;
    
    my $total_results = 0;
    my $query_count = 0;
    
    while (my $fasta_entry = read_next_fasta_entry($input_fh)) {
        $query_count++;
        my $global_query_number = $query_offset + $query_count;
        my $results = search_sequence_with_validation($fasta_entry, $dbh, $global_query_number, $ovllen, $mode, $kmer_size);
        
        if (@$results > 0) {
            for my $result (@$results) {
                print $output_fh join("\t", @$result) . "\n";
                $total_results++;
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
    my ($input_fh, $output_fh, $query_offset) = @_;
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
            my ($temp_fh, $temp_file) = tempfile("af_kmersearch_$$" . "_$global_query_number" . "_XXXXXX", UNLINK => 0);
            close($temp_fh);
            
            my $pid = fork();
            
            if (!defined $pid) {
                die "Cannot fork: $!\n";
            } elsif ($pid == 0) {
                # Child process
                process_single_sequence($fasta_entry, $global_query_number, $temp_file);
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
                # Stream results directly from temp file to output file (memory efficient)
                open my $temp_fh, '<', $result_info->{file} or die "Cannot open temporary file '$result_info->{file}': $!\n";
                while (my $line = <$temp_fh>) {
                    chomp $line;
                    print $output_fh "$line\n";
                    $total_results++;
                }
                close $temp_fh;
                unlink $result_info->{file};
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
            # Stream results directly from temp file to output file (memory efficient)
            open my $temp_fh, '<', $result_info->{file} or die "Cannot open temporary file '$result_info->{file}': $!\n";
            while (my $line = <$temp_fh>) {
                chomp $line;
                print $output_fh "$line\n";
                $total_results++;
            }
            close $temp_fh;
            unlink $result_info->{file};
        }
        $next_output_query++;
    }
    
    print "All sequences processed.\n";
    return { results => $total_results, queries => $query_count };
}

sub process_single_sequence {
    my ($fasta_entry, $query_number, $temp_file) = @_;
    
    # Create new database connection for child process
    my $password = $ENV{PGPASSWORD} || '';
    my $child_dsn = "DBI:Pg:dbname=$database;host=$host;port=$port";
        
    my $child_dbh = DBI->connect($child_dsn, $username, $password, {
        RaiseError => 1,
        AutoCommit => 1,
        pg_enable_utf8 => 1
    }) or die "Cannot connect to database in child process: $DBI::errstr\n";
    
    # Get k-mer size from af_kmersearch_meta table
    my $child_kmer_size = get_kmer_size_from_meta($child_dbh);
    
    # Set k-mer size for pg_kmersearch
    eval {
        $child_dbh->do("SET kmersearch.kmer_size = $child_kmer_size");
    };
    if ($@) {
        die "Failed to set k-mer size in child process: $@\n";
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
    
    # Set minimum shared key rate
    eval {
        $child_dbh->do("SET kmersearch.min_shared_ngram_key_rate = $minpsharedkey");
    };
    if ($@) {
        die "Failed to set minimum shared key rate in child process: $@\n";
    }
    
    # Set rawscore cache max entries (maxnseq * 2)
    my $child_rawscore_cache_max_entries = $maxnseq * 2;
    eval {
        $child_dbh->do("SET kmersearch.rawscore_cache_max_entries = $child_rawscore_cache_max_entries");
    };
    if ($@) {
        die "Failed to set rawscore cache max entries in child process: $@\n";
    }
    
    # Get ovllen for validation
    my $child_ovllen = get_ovllen_from_meta($child_dbh);
    
    # Search sequence
    my $results = search_sequence_with_validation($fasta_entry, $child_dbh, $query_number, $child_ovllen, $mode, $child_kmer_size);
    
    # Check for no matches and report to STDERR
    if (@$results == 0) {
        print STDERR "No matches found for query $query_number: " . $fasta_entry->{label} . "\n";
    }
    
    # Write results to temporary file
    open my $temp_fh, '>', $temp_file or die "Cannot open temporary file '$temp_file': $!\n";
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
    
    # Build search query with subquery for efficient sorting
    my $inner_sql;
    if ($search_mode eq 'maximum') {
        $inner_sql = <<SQL;
SELECT seq, seqid
FROM af_kmersearch
WHERE seq =% ?
SQL
    } else {
        $inner_sql = <<SQL;
SELECT seq, seqid
FROM af_kmersearch
WHERE seq =% ?
SQL
    }

    my @params = ($sequence);
    
    # Add partition condition if specified
    if ($partition) {
        $inner_sql .= " AND ? = ANY(part)";
        push @params, $partition;
    }
    
    # Add ORDER BY and LIMIT to inner query (use rawscore for performance)
    $inner_sql .= " ORDER BY kmersearch_rawscore(seq, ?) DESC LIMIT ?";
    push @params, $sequence, $maxnseq;
    
    # Build outer query with corrected score sorting
    my $sql;
    if ($search_mode eq 'maximum') {
        $sql = <<SQL;
SELECT 
    kmersearch_correctedscore(seq, ?) AS score,
    seqid,
    seq
FROM ($inner_sql) selected_rows
ORDER BY score DESC
SQL
    } else {
        $sql = <<SQL;
SELECT 
    kmersearch_correctedscore(seq, ?) AS score,
    seqid
FROM ($inner_sql) selected_rows
ORDER BY score DESC
SQL
    }

    # Add parameters for outer query
    unshift @params, $sequence;
    
    my @results = ();
    
    eval {
        my $sth = $dbh->prepare($sql);
        $sth->execute(@params);
        
        if ($search_mode eq 'maximum') {
            while (my ($score, $seqid_array, $seq) = $sth->fetchrow_array()) {
                # Parse PostgreSQL array and extract seqid
                my $seqid_str = extract_seqid_string($seqid_array);
                
                push @results, [$query_number, $label, $score, $seqid_str, $seq];
            }
        } else {
            while (my ($score, $seqid_array) = $sth->fetchrow_array()) {
                # Parse PostgreSQL array and extract seqid
                my $seqid_str = extract_seqid_string($seqid_array);
                
                push @results, [$query_number, $label, $score, $seqid_str];
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

sub get_ovllen_from_meta {
    my ($dbh) = @_;
    
    # Query af_kmersearch_meta table to get ovllen value
    my $sth = $dbh->prepare("SELECT ovllen FROM af_kmersearch_meta LIMIT 1");
    eval {
        $sth->execute();
        my ($ovllen) = $sth->fetchrow_array();
        $sth->finish();
        
        if (defined $ovllen) {
            return $ovllen;
        } else {
            die "No ovllen value found in af_kmersearch_meta table\n";
        }
    };
    
    if ($@) {
        die "Failed to retrieve ovllen from af_kmersearch_meta table: $@\n";
    }
}

sub get_kmer_size_from_meta {
    my ($dbh) = @_;
    
    # Query af_kmersearch_meta table to get kmer_size value
    my $sth = $dbh->prepare("SELECT kmer_size FROM af_kmersearch_meta LIMIT 1");
    eval {
        $sth->execute();
        my ($kmer_size) = $sth->fetchrow_array();
        $sth->finish();
        
        if (defined $kmer_size) {
            return $kmer_size;
        } else {
            die "No k-mer index found. Please run af_kmerindex to create indexes first.\n";
        }
    };
    
    if ($@) {
        die "Failed to retrieve kmer_size from af_kmersearch_meta table: $@\n";
    }
}

sub validate_query_sequence {
    my ($sequence, $ovllen, $kmer_size) = @_;
    
    # Check for invalid characters (allow all degenerate nucleotide codes)
    if ($sequence =~ /[^ACGTUMRWSYKVHDBN]/i) {
        return {
            valid => 0,
            reason => "Query sequence contains invalid characters (only A, C, G, T, U, M, R, W, S, Y, K, V, H, D, B, N are allowed)"
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
        'normal' => 'normal',
        'max' => 'maximum',
        'maximize' => 'maximum',
        'maximum' => 'maximum'
    );
    
    my $normalized = $mode_aliases{lc($mode)};
    return '' unless $normalized;
    
    # All modes are accepted for af_kmersearch
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