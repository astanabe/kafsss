#!/usr/bin/perl

use strict;
use warnings;
use Getopt::Long;
use LWP::UserAgent;
use JSON;
use HTTP::Request::Common qw(POST);
use POSIX qw(strftime WNOHANG);
use Sys::Hostname;
use File::Basename;
use URI;

# Version number
my $VERSION = "1.0.0";

# Default values
my $default_maxnseq = 1000;
my $default_numthreads = 1;
my $default_maxnretry = 0;  # 0 means unlimited
my $default_maxnretry_total = 100;
my $default_retrydelay = 10;
my $default_failedserverexclusion = -1;  # -1 means infinite (never re-enable)
my $default_mode = 'normal';
my $default_minpsharedkey = 0.9;

# Job management settings
my $job_file = '.af_kmersearchclient';
my @polling_intervals = (5, 10, 20, 30);  # 5s, 10s, 20s, 30s, then 60s

# Command line options
my $server = '';
my $serverlist = '';
my $database = '';  # Now optional
my $partition = '';
my $maxnseq = $default_maxnseq;
my $minscore = undef;
my $minpsharedkey = $default_minpsharedkey;
my $numthreads = $default_numthreads;
my $maxnretry = $default_maxnretry;
my $maxnretry_total = $default_maxnretry_total;
my $retrydelay = $default_retrydelay;
my $failedserverexclusion = $default_failedserverexclusion;
my $mode = $default_mode;
my $netrc_file = '';
my $http_user = '';
my $http_password = '';
my $resume_job_id = '';
my $cancel_job_id = '';
my $show_jobs = 0;
my $help = 0;

# Parse command line options
GetOptions(
    'server=s' => \$server,
    'serverlist=s' => \$serverlist,
    'db=s' => \$database,
    'partition=s' => \$partition,
    'maxnseq=i' => \$maxnseq,
    'minscore=i' => \$minscore,
    'minpsharedkey=f' => \$minpsharedkey,
    'numthreads=i' => \$numthreads,
    'maxnretry=i' => \$maxnretry,
    'maxnretry_total=i' => \$maxnretry_total,
    'retrydelay=i' => \$retrydelay,
    'failedserverexclusion=i' => \$failedserverexclusion,
    'mode=s' => \$mode,
    'netrc-file=s' => \$netrc_file,
    'http-user=s' => \$http_user,
    'http-password=s' => \$http_password,
    'resume=s' => \$resume_job_id,
    'cancel=s' => \$cancel_job_id,
    'jobs' => \$show_jobs,
    'help|h' => \$help,
) or die "Error in command line arguments\n";

# Show help
if ($help) {
    print_help();
    exit 0;
}

# Check for mutually exclusive options
my $special_mode_count = 0;
$special_mode_count++ if $resume_job_id;
$special_mode_count++ if $cancel_job_id;
$special_mode_count++ if $show_jobs;

if ($special_mode_count > 1) {
    die "Error: --resume, --cancel, and --jobs options are mutually exclusive\n";
}

# Show jobs if requested
if ($show_jobs) {
    show_active_jobs();
    exit 0;
}

# Resume job if requested
if ($resume_job_id) {
    resume_job($resume_job_id);
    exit 0;
}

# Cancel job if requested
if ($cancel_job_id) {
    cancel_job($cancel_job_id);
    exit 0;
}

# Declare global variables
our $global_output_file = '';
our @global_input_files = ();

# Check required arguments (skip for resume/jobs/cancel mode)
unless ($resume_job_id || $cancel_job_id || $show_jobs) {
    if (@ARGV < 2) {
        die "Usage: perl af_kmersearchclient.pl [options] input_file(s) output_file\n" .
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
    die "Either --server or --serverlist option must be specified\n" unless $server || $serverlist;
    # Database name is now optional - can be omitted if server has default
    die "maxnseq must be positive integer\n" unless $maxnseq > 0;
    die "minscore must be positive integer\n" if defined $minscore && $minscore <= 0;
    die "minpsharedkey must be between 0.0 and 1.0\n" unless $minpsharedkey >= 0.0 && $minpsharedkey <= 1.0;
    die "numthreads must be positive integer\n" unless $numthreads > 0;
    die "maxnretry must be non-negative integer\n" unless $maxnretry >= 0;
    die "maxnretry_total must be non-negative integer\n" unless $maxnretry_total >= 0;
    die "retrydelay must be non-negative integer\n" unless $retrydelay >= 0;
    die "failedserverexclusion must be integer (-1 for infinite)\n" unless $failedserverexclusion >= -1;
    
    # Store variables globally for new job
    $global_output_file = $output_file;
    @global_input_files = @input_files;
}

# Validate mode
my $normalized_mode = normalize_mode($mode);
if (!$normalized_mode) {
    die "Invalid mode: $mode. Must be 'minimum', 'normal', or 'maximum'\n";
}
$mode = $normalized_mode;

# Validate authentication options
if ($http_user && !$http_password) {
    die "--http-user option requires --http-password option\n";
}
if ($http_password && !$http_user) {
    die "--http-password option requires --http-user option\n";
}

# Parse .netrc file if specified
my %netrc_credentials = ();
if ($netrc_file) {
    %netrc_credentials = parse_netrc_file($netrc_file);
}

print "af_kmersearchclient.pl version $VERSION\n";
print "Input files (" . scalar(@global_input_files) . "):\n";
for my $i (0..$#global_input_files) {
    print "  " . ($i + 1) . ". $global_input_files[$i]\n";
}
print "Output file: $global_output_file\n";
print "Server: " . ($server ? $server : 'none') . "\n";
print "Server list file: " . ($serverlist ? $serverlist : 'none') . "\n";
print "Database: $database\n";
print "Partition: " . ($partition ? $partition : 'all') . "\n";
print "Max sequences: $maxnseq\n";
print "Min score: " . (defined $minscore ? $minscore : 'default') . "\n";
print "Min shared key rate: $minpsharedkey\n";
print "Number of threads: $numthreads\n";
print "Mode: $mode\n";
print "Max retries per query: $maxnretry\n";
print "Max total retries: $maxnretry_total\n";
print "Retry delay: $retrydelay seconds\n";
print "Failed server exclusion: " . ($failedserverexclusion == -1 ? 'infinite' : "$failedserverexclusion seconds") . "\n";

# Handle special modes (resume, jobs, cancel) - these don't need server URLs
if ($resume_job_id) {
    resume_job($resume_job_id);
    exit 0;
}

if ($show_jobs) {
    show_active_jobs();
    exit 0;
}

if ($cancel_job_id) {
    cancel_job($cancel_job_id);
    exit 0;
}

# Parse and normalize server URLs from both sources (for normal mode only)
my @server_urls = parse_all_server_urls($server, $serverlist);
print "Server URLs (" . scalar(@server_urls) . "):";
for my $i (0..$#server_urls) {
    print "\n  " . ($i + 1) . ". $server_urls[$i]";
}
print "\n";

# Validate server metadata (get metadata from first available server)
my $server_metadata = validate_server_metadata(@server_urls);
print "Server database: " . ($server_metadata->{database} || 'default') . "\n";
print "Server k-mer size: " . $server_metadata->{kmer_size} . "\n";

# Use server database if none specified
if (!$database && $server_metadata->{database}) {
    $database = $server_metadata->{database};
    print "Using server default database: $database\n";
}

# Global variables for server load balancing and retry management
my $current_server_index = 0;
my %failed_servers = ();  # server_url => failure_time
my $total_retry_count = 0;

# Submit async job and start polling
my $job_id = submit_search_job(@global_input_files);
print "Job submitted with ID: $job_id\n";

# Save job information
save_job_info({
    job_id => $job_id,
    output_file => $global_output_file,
    input_files => \@global_input_files,
    server_urls => \@server_urls,
    database => $database,
    partition => $partition,
    maxnseq => $maxnseq,
    minscore => $minscore,
    minpsharedkey => $minpsharedkey,
    mode => $mode,
    created_time => time(),
    status => 'running'
});

# Poll for results
my $result = poll_for_results($job_id);

if ($result->{success}) {
    print "Job completed successfully.\n";
    print "Total results: " . $result->{total_results} . "\n";
    
    # Clean up job info
    remove_job_info($job_id);
} else {
    print STDERR "Job failed or was interrupted.\n";
    print STDERR "Job ID: $job_id (saved for resume)\n";
    exit 1;
}

exit 0;

#
# Async Job Management Functions
#

sub submit_search_job {
    my (@input_files) = @_;
    
    # Collect all sequences from input files
    my @sequences = ();
    my $sequence_count = 0;
    
    for my $input_file (@input_files) {
        my $input_fh = open_input_file($input_file);
        
        while (my $fasta_entry = read_next_fasta_entry($input_fh)) {
            $sequence_count++;
            push @sequences, {
                sequence_number => $sequence_count,
                label => $fasta_entry->{label},
                sequence => $fasta_entry->{sequence}
            };
        }
        
        close_input_file($input_fh, $input_file);
    }
    
    print "Collected " . scalar(@sequences) . " sequences from " . scalar(@input_files) . " files.\n";
    
    # Submit job to server
    my $job_data = {
        sequences => \@sequences,
        database => $database,
        partition => $partition,
        maxnseq => $maxnseq,
        mode => $mode
    };
    
    # Add minscore if specified
    $job_data->{minscore} = $minscore if defined $minscore;
    
    # Add minpsharedkey
    $job_data->{minpsharedkey} = $minpsharedkey;
    
    my $server_url = get_next_server_url();
    my $job_id = make_job_submission_request($server_url, $job_data);
    
    return $job_id;
}

sub make_job_submission_request {
    my ($server_url, $job_data) = @_;
    
    my $ua = LWP::UserAgent->new(
        timeout => 30,
        agent => "af_kmersearchclient.pl/$VERSION"
    );
    
    # Convert server URL to submission endpoint
    my $submit_url = $server_url;
    $submit_url =~ s/\/search$/\/search/;  # Already correct for async servers
    
    my $json = JSON->new;
    my $json_data = $json->encode($job_data);
    
    my $request = POST $submit_url,
        'Content-Type' => 'application/json',
        Content => $json_data;
    
    # Add authentication if available
    add_authentication($request, $submit_url);
    
    my $response = $ua->request($request);
    
    if ($response->is_success) {
        my $result = $json->decode($response->content);
        
        if ($result->{success} && $result->{job_id}) {
            return $result->{job_id};
        } else {
            die "Server error: " . ($result->{error} || 'Unknown error') . "\n";
        }
    } else {
        die "HTTP error submitting job: " . $response->status_line . "\n";
    }
}

sub poll_for_results {
    my ($job_id) = @_;
    
    my $retry_count = 0;
    my $poll_index = 0;
    
    print "Starting polling for job $job_id...\n";
    
    while (1) {
        # Determine sleep interval
        my $sleep_time;
        if ($poll_index < scalar(@polling_intervals)) {
            $sleep_time = $polling_intervals[$poll_index];
            $poll_index++;
        } else {
            $sleep_time = 60;  # 60 seconds for all subsequent polls
        }
        
        print "Waiting $sleep_time seconds before next poll...\n";
        sleep($sleep_time);
        
        # Get job status
        my $status_result = get_job_status($job_id);
        
        if ($status_result->{success}) {
            my $status = $status_result->{status};
            print "Job status: $status\n";
            
            if ($status eq 'completed') {
                # Get results
                my $results = get_job_results($job_id);
                if ($results->{success}) {
                    # Write results to output file
                    write_results_to_file($results->{data});
                    return { success => 1, total_results => scalar(@{$results->{data}}) };
                } else {
                    print STDERR "Failed to retrieve results: " . $results->{error} . "\n";
                    return { success => 0, error => $results->{error} };
                }
            } elsif ($status eq 'failed') {
                print STDERR "Job failed on server\n";
                return { success => 0, error => 'Job failed on server' };
            } elsif ($status eq 'running') {
                # Continue polling
                next;
            } else {
                print STDERR "Unknown job status: $status\n";
                return { success => 0, error => "Unknown status: $status" };
            }
        } else {
            $retry_count++;
            print STDERR "Failed to get job status (attempt $retry_count): " . $status_result->{error} . "\n";
            
            # Check retry limits
            if ($maxnretry > 0 && $retry_count >= $maxnretry) {
                print STDERR "Maximum retry count reached for job status\n";
                return { success => 0, error => 'Maximum retry count reached' };
            }
            
            if ($total_retry_count >= $maxnretry_total) {
                print STDERR "Maximum total retry count reached\n";
                return { success => 0, error => 'Maximum total retry count reached' };
            }
            
            $total_retry_count++;
        }
    }
}

sub get_job_status {
    my ($job_id) = @_;
    
    my $ua = LWP::UserAgent->new(
        timeout => 30,
        agent => "af_kmersearchclient.pl/$VERSION"
    );
    
    my $server_url = get_next_server_url();
    my $status_url = $server_url;
    $status_url =~ s/\/search$/\/status/;
    
    my $request = POST $status_url,
        'Content-Type' => 'application/json',
        Content => JSON->new->encode({ job_id => $job_id });
    
    add_authentication($request, $status_url);
    
    my $response = $ua->request($request);
    
    if ($response->is_success) {
        my $result = JSON->new->decode($response->content);
        return $result;
    } else {
        return { success => 0, error => $response->status_line };
    }
}

sub get_job_results {
    my ($job_id) = @_;
    
    my $ua = LWP::UserAgent->new(
        timeout => 60,  # Longer timeout for results
        agent => "af_kmersearchclient.pl/$VERSION"
    );
    
    my $server_url = get_next_server_url();
    my $result_url = $server_url;
    $result_url =~ s/\/search$/\/result/;
    
    my $request = POST $result_url,
        'Content-Type' => 'application/json',
        Content => JSON->new->encode({ job_id => $job_id });
    
    add_authentication($request, $result_url);
    
    my $response = $ua->request($request);
    
    if ($response->is_success) {
        my $result = JSON->new->decode($response->content);
        return $result;
    } else {
        return { success => 0, error => $response->status_line };
    }
}

sub write_results_to_file {
    my ($results_data) = @_;
    
    my $output_fh = open_output_file($global_output_file);
    
    for my $result (@$results_data) {
        print $output_fh join("\t", @$result) . "\n";
    }
    
    close_output_file($output_fh, $global_output_file);
    print "Results written to: $global_output_file\n";
}

sub validate_server_metadata {
    my (@server_urls) = @_;
    
    for my $server_url (@server_urls) {
        my $ua = LWP::UserAgent->new(
            timeout => 10,
            agent => "af_kmersearchclient.pl/$VERSION"
        );
        
        # Try to get server metadata
        my $request = HTTP::Request->new(GET => $server_url);
        add_authentication($request, $server_url);
        
        my $response = $ua->request($request);
        
        if ($response->is_success) {
            my $content = $response->content;
            
            # Parse JSON response for metadata
            if ($content =~ /"database"\s*:\s*"([^"]+)"/) {
                my $server_db = $1;
                if ($content =~ /"kmer_size"\s*:\s*(\d+)/) {
                    my $kmer_size = $1;
                    return {
                        database => $server_db,
                        kmer_size => $kmer_size
                    };
                }
            }
        }
    }
    
    # Fallback: try with minimal request
    return { database => undef, kmer_size => 'unknown' };
}

# Job persistence functions
sub save_job_info {
    my ($job_info) = @_;
    
    my $jobs = load_all_jobs();
    $jobs->{$job_info->{job_id}} = $job_info;
    
    my $json = JSON->new->pretty;
    my $json_data = $json->encode($jobs);
    
    open my $fh, '>', $job_file or die "Cannot save job info: $!\n";
    print $fh $json_data;
    close $fh;
}

sub load_all_jobs {
    return {} unless -f $job_file;
    
    open my $fh, '<', $job_file or return {};
    my $json_data = do { local $/; <$fh> };
    close $fh;
    
    return {} unless $json_data;
    
    my $json = JSON->new;
    return eval { $json->decode($json_data) } || {};
}

sub remove_job_info {
    my ($job_id) = @_;
    
    my $jobs = load_all_jobs();
    delete $jobs->{$job_id};
    
    my $json = JSON->new->pretty;
    my $json_data = $json->encode($jobs);
    
    open my $fh, '>', $job_file or die "Cannot update job info: $!\n";
    print $fh $json_data;
    close $fh;
}

sub resume_job {
    my ($job_id) = @_;
    
    my $jobs = load_all_jobs();
    my $job_info = $jobs->{$job_id};
    
    unless ($job_info) {
        die "Job ID $job_id not found in saved jobs\n";
    }
    
    print "Resuming job: $job_id\n";
    print "Output file: " . $job_info->{output_file} . "\n";
    print "Created: " . strftime("%Y-%m-%d %H:%M:%S", localtime($job_info->{created_time})) . "\n";
    
    # Set global variables
    $global_output_file = $job_info->{output_file};
    @global_input_files = @{$job_info->{input_files}};
    @server_urls = @{$job_info->{server_urls}};
    
    # Poll for results
    my $result = poll_for_results($job_id);
    
    if ($result->{success}) {
        print "Job completed successfully.\n";
        print "Total results: " . $result->{total_results} . "\n";
        remove_job_info($job_id);
    } else {
        print STDERR "Job failed or was interrupted.\n";
        exit 1;
    }
}

sub show_active_jobs {
    my $jobs = load_all_jobs();
    
    unless (keys %$jobs) {
        print "No active jobs found.\n";
        return;
    }
    
    print "Active jobs:\n";
    for my $job_id (sort keys %$jobs) {
        my $job = $jobs->{$job_id};
        my $created = strftime("%Y-%m-%d %H:%M:%S", localtime($job->{created_time}));
        my $input_count = scalar(@{$job->{input_files}});
        
        print "  $job_id\n";
        print "    Created: $created\n";
        print "    Status: " . $job->{status} . "\n";
        print "    Input files: $input_count\n";
        print "    Output: " . $job->{output_file} . "\n";
        print "    Database: " . ($job->{database} || 'default') . "\n";
        print "\n";
    }
}

sub add_auth_credentials {
    my ($request, $url) = @_;
    
    # Extract hostname for .netrc lookup
    my $uri = URI->new($url);
    my $hostname = $uri->host;
    
    # Check .netrc credentials first
    if ($netrc_credentials{$hostname}) {
        my $cred = $netrc_credentials{$hostname};
        $request->authorization_basic($cred->{login}, $cred->{password});
        return;
    }
    
    # Use command line credentials as fallback
    if ($http_user && $http_password) {
        $request->authorization_basic($http_user, $http_password);
    }
}

sub cancel_job {
    my $job_id = shift;
    
    # Load job information
    my $jobs = load_all_jobs();
    my $job = $jobs->{$job_id};
    
    unless ($job) {
        print STDERR "Error: Job '$job_id' not found.\n";
        exit 1;
    }
    
    print "Canceling job $job_id...\n";
    
    # Send cancel requests to all servers that were used for this job
    if ($job->{servers}) {
        for my $server_url (@{$job->{servers}}) {
            print "Sending cancel request to $server_url...\n";
            send_cancel_request($server_url, $job_id);
        }
    }
    
    # Remove any partial output files
    if ($job->{output_file} && -f $job->{output_file}) {
        unlink($job->{output_file});
        print "Removed partial output file: $job->{output_file}\n";
    }
    
    # Remove any temporary files
    if ($job->{temp_files}) {
        for my $temp_file (@{$job->{temp_files}}) {
            if (-f $temp_file) {
                unlink($temp_file);
                print "Removed temporary file: $temp_file\n";
            }
        }
    }
    
    # Remove job from persistence
    delete $jobs->{$job_id};
    save_jobs($jobs);
    
    print "Job $job_id has been canceled and removed.\n";
}

sub send_cancel_request {
    my ($server_url, $job_id) = @_;
    
    # Convert search URL to cancel URL
    my $cancel_url = $server_url;
    $cancel_url =~ s/\/search$/\/cancel/;
    
    # Create cancel request
    my $ua = LWP::UserAgent->new();
    $ua->timeout(30);
    
    my $request_data = {
        job_id => $job_id
    };
    
    my $json_data = encode_json($request_data);
    my $request = POST($cancel_url, Content_Type => 'application/json', Content => $json_data);
    
    # Add authentication
    add_auth_credentials($request, $cancel_url);
    
    # Send request
    my $response = $ua->request($request);
    
    if ($response->is_success) {
        print "Cancel request sent successfully to $cancel_url\n";
    } else {
        print STDERR "Warning: Failed to send cancel request to $cancel_url: " . $response->status_line . "\n";
    }
}

#
# Existing Subroutines (with updates for async functionality)
#

sub print_help {
    print <<EOF;
af_kmersearchclient.pl version $VERSION

Usage: perl af_kmersearchclient.pl [options] input_file(s) output_file
       perl af_kmersearchclient.pl --resume=JOB_ID
       perl af_kmersearchclient.pl --cancel=JOB_ID
       perl af_kmersearchclient.pl --jobs

Search DNA sequences from multiple sources against remote af_kmersearch server using k-mer similarity.
This client now supports asynchronous job processing with automatic polling and resume functionality.

Required arguments (for new jobs):
  input_file(s)     Input FASTA file(s), patterns, or databases:
                    - Regular files: file1.fasta file2.fasta
                    - Wildcard patterns: 'data/*.fasta' (use quotes to prevent shell expansion)
                    - Compressed files: file.fasta.gz file.fasta.bz2 file.fasta.xz file.fasta.zst
                    - BLAST databases: mydb (requires mydb.nsq or mydb.nal)
                    - Standard input: '-', 'stdin', or 'STDIN'
  output_file       Output TSV file (use '-', 'stdout', or 'STDOUT' for standard output)

Required options (at least one):
  --server=SERVERS  Server URL(s) - single server or comma-separated list
                    (hostname[:port], IP[:port], http://..., or https://...)
  --serverlist=FILE File containing server URLs (one per line)

Job management options:
  --resume=JOB_ID   Resume a previously submitted job by ID
  --cancel=JOB_ID   Cancel a job and remove all associated data
  --jobs            List all active jobs
  --maxnretry=INT   Maximum retries per status check (default: 0 = unlimited)

Other options:
  --db=DATABASE     PostgreSQL database name (optional if server has default)
  --partition=NAME  Limit search to specific partition (optional)
  --maxnseq=INT     Maximum number of results per query (default: 1000)
  --minscore=INT    Minimum score threshold (optional, uses server default if not set)
  --minpsharedkey=REAL  Minimum percentage of shared keys (0.0-1.0, default: 0.9)
  --numthreads=INT  Number of parallel threads (default: 1, currently unused for async)
  --mode=MODE       Output mode: minimum, normal, maximum (default: normal)
  --maxnretry_total=INT Maximum total retries for all operations (default: 100)
  --retrydelay=INT  Retry delay in seconds (default: 10, currently unused)
  --failedserverexclusion=INT Exclude failed servers for N seconds (default: infinite, -1)
  --netrc-file=FILE Read authentication credentials from .netrc format file
  --http-user=USER  HTTP Basic authentication username (requires --http-password)
  --http-password=PASS HTTP Basic authentication password (requires --http-user)
  --help, -h        Show this help message

Polling behavior:
  After job submission, the client polls for results with these intervals:
  - 1st check: after 5 seconds
  - 2nd check: after additional 10 seconds
  - 3rd check: after additional 20 seconds
  - 4th check: after additional 30 seconds
  - 5th+ checks: every 60 seconds until completion

Job persistence:
  Job information is saved to '.af_kmersearchclient' in the current directory.
  If a job is interrupted, use --resume=JOB_ID to continue polling.
  Use --jobs to see all active jobs.

Note:
  Both --server and --serverlist can be used together.
  Duplicate servers are allowed for weighted load balancing.
  Servers appearing multiple times will be selected more frequently.

Server URL formats:
  hostname          http://hostname:8080/search
  hostname:9090     http://hostname:9090/search
  192.168.1.100     http://192.168.1.100:8080/search
  192.168.1.100:9090 http://192.168.1.100:9090/search
  http://server/api/search   Use as-is
  https://server/search      Use as-is
  
  Multiple servers (load balancing):
  server1,server2,server3
  localhost:8080,localhost:8081,localhost:8082
  http://server1/search,https://server2/api/search

Output format:
  Tab-separated values with columns:
  1. Query sequence number (1-based integer)
  2. Query FASTA label
  3. CORRECTEDSCORE from server
  4. Comma-separated seqid list
  5. Sequence data (only in maximum mode)

Authentication:
  For servers protected by HTTP Basic authentication, use one of these options:
  
  1. .netrc file (recommended for multiple servers):
     --netrc-file=/path/to/netrc
     
     .netrc format:
     machine hostname.example.com
     login myusername
     password mypassword
     
     machine server2.example.com
     login otherusername
     password otherpassword
  
  2. Command line credentials (for all servers):
     --http-user=myusername --http-password=mypassword
  
  3. Both options (fallback behavior):
     Specific hostnames in .netrc are used first, command line credentials
     are used as fallback for servers not found in .netrc file.

Examples:
  # Submit new job
  perl af_kmersearchclient.pl --server=localhost --db=mydb query.fasta results.tsv
  
  # Resume interrupted job
  perl af_kmersearchclient.pl --resume=20250703T143052-abc123def456
  
  # Cancel running job
  perl af_kmersearchclient.pl --cancel=20250703T143052-abc123def456
  
  # List active jobs
  perl af_kmersearchclient.pl --jobs
  
  # Multiple files with authentication
  perl af_kmersearchclient.pl --server=https://server.com --netrc-file=.netrc file1.fasta file2.fasta results.tsv
  
  # Server with default database
  perl af_kmersearchclient.pl --server=localhost:8080 'queries/*.fasta' results.tsv

EOF
}

# Continue with remaining subroutines from original file...
# (The functions below remain the same as the original client)

sub parse_all_server_urls {
    my ($server_string, $serverlist_file) = @_;
    
    my @all_servers = ();
    
    # Parse servers from --server option
    if ($server_string) {
        my @servers_from_option = parse_server_urls($server_string);
        push @all_servers, @servers_from_option;
    }
    
    # Parse servers from --serverlist file
    if ($serverlist_file) {
        my @servers_from_file = parse_serverlist_file($serverlist_file);
        push @all_servers, @servers_from_file;
    }
    
    die "No servers specified. Use --server or --serverlist option.\n" unless @all_servers;
    
    return @all_servers;
}

sub parse_server_urls {
    my ($server_string) = @_;
    
    # Split by comma and normalize each server URL
    my @servers = split /,/, $server_string;
    my @normalized_urls = ();
    
    for my $server (@servers) {
        # Trim whitespace
        $server =~ s/^\s+|\s+$//g;
        
        # Skip empty entries
        next unless $server;
        
        my $normalized_url = normalize_single_server_url($server);
        push @normalized_urls, $normalized_url;
    }
    
    return @normalized_urls;
}

sub parse_serverlist_file {
    my ($filename) = @_;
    
    die "Server list file '$filename' does not exist\n" unless -f $filename;
    
    open my $fh, '<', $filename or die "Cannot open server list file '$filename': $!\n";
    
    my @normalized_urls = ();
    my $line_number = 0;
    
    while (my $line = <$fh>) {
        $line_number++;
        chomp $line;
        
        # Trim whitespace
        $line =~ s/^\s+|\s+$//g;
        
        # Skip empty lines and comments
        next if $line eq '' || $line =~ /^#/;
        
        eval {
            my $normalized_url = normalize_single_server_url($line);
            push @normalized_urls, $normalized_url;
        };
        
        if ($@) {
            die "Error parsing server list file '$filename' at line $line_number: $@\n";
        }
    }
    
    close $fh;
    
    return @normalized_urls;
}

sub normalize_single_server_url {
    my ($server) = @_;
    
    # If already a full URL, use as-is
    if ($server =~ /^https?:\/\//) {
        return $server;
    }
    
    # Parse hostname and port
    my ($hostname, $port) = split /:/, $server, 2;
    $port ||= 8080;  # Default port
    
    return "http://$hostname:$port/search";
}

sub get_next_server_url {
    my ($exclude_failed) = @_;
    $exclude_failed = 1 unless defined $exclude_failed;
    
    my $attempts = 0;
    my $max_attempts = scalar(@server_urls) * 2;  # Prevent infinite loop
    
    while ($attempts < $max_attempts) {
        my $url = $server_urls[$current_server_index];
        $current_server_index = ($current_server_index + 1) % scalar(@server_urls);
        
        if ($exclude_failed && is_server_failed($url)) {
            $attempts++;
            next;
        }
        
        return $url;
    }
    
    # If all servers are failed and excluding failed servers, return any server
    if ($exclude_failed) {
        my $url = $server_urls[$current_server_index];
        $current_server_index = ($current_server_index + 1) % scalar(@server_urls);
        return $url;
    }
    
    die "No available servers found\n";
}

sub is_server_failed {
    my ($server_url) = @_;
    
    return 0 unless exists $failed_servers{$server_url};
    
    my $failure_time = $failed_servers{$server_url};
    my $current_time = time();
    
    # If failedserverexclusion is -1 (infinite), server stays failed
    if ($failedserverexclusion == -1) {
        return 1;
    }
    
    # Check if exclusion period has expired
    if ($current_time - $failure_time >= $failedserverexclusion) {
        delete $failed_servers{$server_url};
        return 0;
    }
    
    return 1;
}

sub mark_server_failed {
    my ($server_url) = @_;
    $failed_servers{$server_url} = time();
    print STDERR "Marked server as failed: $server_url\n";
}

sub get_available_server_count {
    my $available_count = 0;
    for my $url (@server_urls) {
        $available_count++ unless is_server_failed($url);
    }
    return $available_count;
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
    
    # All modes are accepted for af_kmersearch.pl
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

sub parse_netrc_file {
    my ($filename) = @_;
    
    die ".netrc file '$filename' does not exist\n" unless -f $filename;
    
    open my $fh, '<', $filename or die "Cannot open .netrc file '$filename': $!\n";
    
    my %credentials = ();
    my $current_machine = '';
    my $current_login = '';
    my $current_password = '';
    
    while (my $line = <$fh>) {
        chomp $line;
        
        # Remove leading/trailing whitespace
        $line =~ s/^\s+|\s+$//g;
        
        # Skip empty lines and comments
        next if $line eq '' || $line =~ /^#/;
        
        # Parse .netrc tokens
        my @tokens = split /\s+/, $line;
        
        for my $i (0..$#tokens) {
            my $token = $tokens[$i];
            
            if ($token eq 'machine' && defined $tokens[$i+1]) {
                # Save previous machine if complete
                if ($current_machine && $current_login && $current_password) {
                    $credentials{$current_machine} = {
                        login => $current_login,
                        password => $current_password
                    };
                }
                
                # Start new machine
                $current_machine = $tokens[$i+1];
                $current_login = '';
                $current_password = '';
                $i++; # Skip next token
            } elsif ($token eq 'login' && defined $tokens[$i+1]) {
                $current_login = $tokens[$i+1];
                $i++; # Skip next token
            } elsif ($token eq 'password' && defined $tokens[$i+1]) {
                $current_password = $tokens[$i+1];
                $i++; # Skip next token
            }
        }
    }
    
    # Save last machine if complete
    if ($current_machine && $current_login && $current_password) {
        $credentials{$current_machine} = {
            login => $current_login,
            password => $current_password
        };
    }
    
    close $fh;
    
    return %credentials;
}