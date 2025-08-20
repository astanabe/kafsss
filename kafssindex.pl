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
my $default_max_appearance_rate = 0.5;
my $default_max_appearance_nrow = 0;
my $default_occur_bitlen = 8;

# Command line options
my $host = $default_host;
my $port = $default_port;
my $username = $default_user;
my $tablespace = '';
my $mode = '';
my $kmer_size = undef;
my $max_appearance_rate = undef;
my $max_appearance_nrow = undef;
my $occur_bitlen = undef;
my $numthreads = 0;
my $workingmemory = '8GB';
my $maintenanceworkingmemory = '8GB';
my $temporarybuffer = '512MB';
my $verbose = 0;
my $help = 0;

# Parse command line options
GetOptions(
    'host=s' => \$host,
    'port=i' => \$port,
    'username=s' => \$username,
    'tablespace=s' => \$tablespace,
    'mode=s' => \$mode,
    'kmersize=i' => \$kmer_size,
    'maxpappear=f' => \$max_appearance_rate,
    'maxnappear=i' => \$max_appearance_nrow,
    'occurbitlen=i' => \$occur_bitlen,
    'numthreads=i' => \$numthreads,
    'workingmemory=s' => \$workingmemory,
    'maintenanceworkingmemory=s' => \$maintenanceworkingmemory,
    'temporarybuffer=s' => \$temporarybuffer,
    'verbose|v' => \$verbose,
    'help|h' => \$help,
) or die "Error in command line arguments\n";

# Show help
if ($help) {
    print_help();
    exit 0;
}

# Check required arguments
if (@ARGV != 1) {
    die "Usage: kafssindex [options] database_name\n" .
        "Use --help for detailed usage information.\n";
}

my ($database_name) = @ARGV;

# Validate required options
die "Mode must be specified with --mode option (create or drop)\n" unless $mode;
die "Invalid mode '$mode'. Must be 'create' or 'drop'\n" unless $mode eq 'create' || $mode eq 'drop';

# Validate numthreads before connecting to database
die "numthreads must be non-negative\n" unless $numthreads >= 0;

print "kafssindex version $VERSION\n";
print "Database: $database_name\n";
print "Host: $host\n";
print "Port: $port\n";
print "Username: $username\n";
print "Tablespace: " . ($tablespace ? $tablespace : 'default') . "\n";
print "Mode: $mode\n";

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
unless (check_database_exists($server_dbh, $database_name)) {
    $server_dbh->disconnect();
    die "Error: Database '$database_name' does not exist.\n" .
        "Please create it first using kafssstore.\n";
}

$server_dbh->disconnect();

# Connect to target database
my $dsn = "DBI:Pg:dbname=$database_name;host=$host;port=$port";
my $dbh = DBI->connect($dsn, $username, $password, {
    AutoCommit => 1,
    PrintError => 0,
    RaiseError => 1,
    ShowErrorStatement => 1,
    AutoInactiveDestroy => 1,
    pg_enable_utf8 => 1
}) or die "Cannot connect to database '$database_name': $DBI::errstr\n";

print "Connected to database successfully.\n" if $verbose;

# Validate database permissions and schema
validate_database_permissions($dbh, $username, $mode);
validate_database_schema($dbh);

# Validate tablespace if specified and mode is 'create'
if ($tablespace && $mode eq 'create') {
    validate_tablespace_exists($dbh, $tablespace);
}

# Acquire advisory lock for exclusive access to prevent conflicts with other tools
print "Acquiring exclusive lock...\n" if $verbose;
eval {
    $dbh->do("SELECT pg_advisory_xact_lock(999)");
    print "Exclusive lock acquired.\n" if $verbose;
};
if ($@) {
    die "Failed to acquire advisory lock: $@\n";
}

# Verify database structure
verify_database_structure($dbh);

# Detect PostgreSQL version
my $pg_version = get_postgresql_version($dbh);
print "PostgreSQL version: $pg_version\n" if $verbose;

# Load and validate parameters from kmersearch_highfreq_kmer if needed (only for create mode)
if ($mode eq 'create') {
    load_and_validate_parameters($dbh);
    
    # Print parameters after loading/validation
    print "K-mer size: $kmer_size\n";
    print "Max appearance rate: $max_appearance_rate\n";
    print "Max appearance nrow: $max_appearance_nrow\n";
    print "Occur bitlen: $occur_bitlen\n";
    print "Num threads: " . ($numthreads ? $numthreads : 'default') . "\n";
    print "Working memory: $workingmemory\n";
    print "Maintenance working memory: $maintenanceworkingmemory\n";
    print "Temporary buffer: $temporarybuffer\n";
    
    # Set GUC variables
    set_guc_variables($dbh);
    
    # Execute create operation
    create_indexes($dbh, $pg_version);
} elsif ($mode eq 'drop') {
    # Execute drop operation
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
kafssindex version $VERSION

Usage: kafssindex [options] database_name

Create or drop GIN indexes on kafsss_data table.

Required arguments:
  database_name     PostgreSQL database name

Required options:
  --mode=MODE       Operation mode: 'create' or 'drop'

Other options:
  --host=HOST       PostgreSQL server host (default: \$PGHOST or localhost)
  --port=PORT       PostgreSQL server port (default: \$PGPORT or 5432)
  --username=USER   PostgreSQL username (default: \$PGUSER or current user)
  --tablespace=NAME Tablespace name for CREATE INDEX (default: default tablespace)
  --kmersize=INT    K-mer length for index creation (default: 8, range: 4-64)
  --maxpappear=REAL Max k-mer appearance rate (default: 0.5, range: 0.0-1.0)
  --maxnappear=INT  Max rows containing k-mer (default: 0=unlimited)
  --occurbitlen=INT Bits for occurrence count (default: 8, range: 0-16)
  --numthreads=INT  Number of parallel workers (default: 0=auto)
  --workingmemory=SIZE  Work memory for each operation (default: 8GB)
  --maintenanceworkingmemory=SIZE  Maintenance work memory for index creation (default: 8GB)
  --temporarybuffer=SIZE  Temporary buffer size (default: 512MB)
  --verbose, -v     Show detailed processing messages (default: false)
  --help, -h        Show this help message

Environment variables:
  PGHOST           PostgreSQL server host
  PGPORT           PostgreSQL server port
  PGUSER           PostgreSQL username
  PGPASSWORD       PostgreSQL password

Examples:
  kafssindex --mode=create mydb
  kafssindex --mode=drop mydb
  kafssindex --mode=create --tablespace=fast_ssd mydb
  kafssindex --mode=create --kmersize=16 mydb
  kafssindex --mode=create --maintenanceworkingmemory=64GB mydb
  kafssindex --mode=create --kmersize=32 --maintenanceworkingmemory=128GB --temporarybuffer=1GB mydb
  kafssindex --mode=create --workingmemory=32GB --maintenanceworkingmemory=128GB --tablespace=fast_ssd mydb
  kafssindex --mode=create --maxpappear=0.3 --maxnappear=500 mydb
  kafssindex --mode=create --occurbitlen=12 --numthreads=8 mydb

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
    
    # Check if kafsss_data table exists
    $sth = $dbh->prepare(<<SQL);
SELECT COUNT(*)
FROM information_schema.tables 
WHERE table_name = 'kafsss_data'
SQL
    $sth->execute();
    my ($table_count) = $sth->fetchrow_array();
    $sth->finish();
    
    die "Table 'kafsss_data' does not exist in database '$database_name'\n" 
        unless $table_count > 0;
    
    # Check if required columns exist with correct types
    $sth = $dbh->prepare(<<SQL);
SELECT column_name, CASE WHEN data_type = 'USER-DEFINED' THEN udt_name ELSE data_type END AS data_type
FROM information_schema.columns 
WHERE table_name = 'kafsss_data'
AND column_name IN ('seq', 'subset', 'seqid')
ORDER BY column_name
SQL
    $sth->execute();
    
    my %columns = ();
    while (my ($col, $type) = $sth->fetchrow_array()) {
        $columns{$col} = $type;
    }
    $sth->finish();
    
    die "Required columns not found in table 'kafsss_data'\n"
        unless exists $columns{seq} && exists $columns{subset} && exists $columns{seqid};
    
    die "Column 'subset' must be ARRAY type\n" unless $columns{subset} eq 'ARRAY';
    die "Column 'seqid' must be ARRAY type\n" unless $columns{seqid} eq 'ARRAY';
    die "Column 'seq' must be DNA2 or DNA4 type\n" 
        unless lc($columns{seq}) eq 'dna2' || lc($columns{seq}) eq 'dna4';
    
    print "Database structure verified.\n";
}

sub create_indexes {
    my ($dbh, $pg_version) = @_;
    
    print "Creating GIN indexes...\n";
    
    # Check if kafsss_data is a partitioned table
    my $is_partitioned = check_if_partitioned($dbh, 'kafsss_data');
    if (!$is_partitioned) {
        die "Error: Table 'kafsss_data' is not partitioned.\n" .
            "Please partition the table first using kafsspart:\n" .
            "  kafsspart --npart=16 $database_name\n" .
            "Then run kafssindex again.\n";
    }
    print "Table 'kafsss_data' is partitioned.\n";
    
    # Validate and set memory parameters
    validate_and_set_memory_parameters($dbh, $workingmemory, $maintenanceworkingmemory, $temporarybuffer);
    
    # Get data type of seq column for operator class selection
    my $type_check_sql = "SELECT CASE WHEN data_type = 'USER-DEFINED' THEN udt_name ELSE data_type END AS data_type " .
                         "FROM information_schema.columns WHERE table_name = 'kafsss_data' AND column_name = 'seq'";
    my $type_sth = $dbh->prepare($type_check_sql);
    $type_sth->execute();
    my ($seq_data_type) = $type_sth->fetchrow_array();
    $type_sth->finish();
    
    # Calculate total bits for operator class selection
    my $total_bits = $kmer_size * 2 + $occur_bitlen;
    my $op_class;
    
    if (lc($seq_data_type) eq 'dna4') {
        if ($total_bits <= 16) {
            $op_class = "kmersearch_dna4_gin_ops_int2";
        } elsif ($total_bits <= 32) {
            $op_class = "kmersearch_dna4_gin_ops_int4";
        } elsif ($total_bits <= 64) {
            $op_class = "kmersearch_dna4_gin_ops_int8";
        } else {
            die "Total bits ($total_bits) exceeds 64 for DNA4 type\n";
        }
    } elsif (lc($seq_data_type) eq 'dna2') {
        if ($total_bits <= 16) {
            $op_class = "kmersearch_dna2_gin_ops_int2";
        } elsif ($total_bits <= 32) {
            $op_class = "kmersearch_dna2_gin_ops_int4";
        } elsif ($total_bits <= 64) {
            $op_class = "kmersearch_dna2_gin_ops_int8";
        } else {
            die "Total bits ($total_bits) exceeds 64 for DNA2 type\n";
        }
    } else {
        die "Unknown data type for seq column: $seq_data_type\n";
    }
    
    print "Selected operator class: $op_class (data_type=$seq_data_type, kmer_size=$kmer_size, occur_bitlen=$occur_bitlen, total_bits=$total_bits)\n";
    
    # Check if matching high-frequency k-mer data exists in kmersearch_highfreq_kmer_meta
    my $check_highfreq_sql = <<SQL;
SELECT COUNT(*) 
FROM kmersearch_highfreq_kmer_meta 
WHERE table_oid = 'kafsss_data'::regclass 
  AND column_name = 'seq'
  AND kmer_size = ?
  AND occur_bitlen = ?
  AND max_appearance_rate = ?
  AND max_appearance_nrow = ?
SQL
    
    my $check_sth = $dbh->prepare($check_highfreq_sql);
    $check_sth->execute($kmer_size, $occur_bitlen, $max_appearance_rate, $max_appearance_nrow);
    my ($matching_highfreq_count) = $check_sth->fetchrow_array();
    $check_sth->finish();
    
    my $use_highfreq_cache = ($matching_highfreq_count > 0);
    
    if ($use_highfreq_cache) {
        print "Found matching high-frequency k-mer metadata in kmersearch_highfreq_kmer_meta.\n";
        print "High-frequency k-mer exclusion will be enabled for index creation.\n";
    } else {
        print "No matching high-frequency k-mer metadata found in kmersearch_highfreq_kmer_meta.\n";
        print "Creating indexes without high-frequency k-mer exclusion.\n";
    }
    
    # Get list of partitions
    my @partitions = get_partitions($dbh, 'kafsss_data');
    
    if (@partitions == 0) {
        die "Error: No partitions found for kafsss_data table.\n" .
            "Please partition the table first using kafsspart.\n";
    }
    
    print "Found " . scalar(@partitions) . " partitions.\n";
    
    # Determine number of parallel workers
    my $max_workers = $numthreads > 0 ? $numthreads : 4;
    my $num_workers = (@partitions < $max_workers) ? @partitions : $max_workers;
    
    print "Using up to $num_workers parallel worker processes for index creation.\n";
    
    # Create indexes on partitions by column
    # Process subset, then seqid, then seq columns in order
    my @columns = (
        { name => 'subset', op_class => '', type => 'gin' },
        { name => 'seqid', op_class => '', type => 'gin' },
        { name => 'seq', op_class => $op_class, type => 'gin' }
    );
    
    # Disconnect from database before forking
    $dbh->disconnect();
    print "Parent process disconnected from database before forking.\n";
    
    for my $column (@columns) {
        print "\nCreating indexes on column '$column->{name}' for all partitions...\n";
        create_partition_indexes_for_column(\@partitions, $num_workers, $column, 
                                            $tablespace, $pg_version, $use_highfreq_cache);
        print "Completed indexes on column '$column->{name}' for all partitions.\n";
    }
    
    # Reconnect to database after all child processes are done
    print "\nReconnecting to database for parent table index creation...\n";
    my $password = $ENV{PGPASSWORD} || '';
    my $parent_dsn = "DBI:Pg:dbname=$database_name;host=$host;port=$port";
    $dbh = DBI->connect($parent_dsn, $username, $password, {
        AutoCommit => 1,
        PrintError => 0,
        RaiseError => 1,
        ShowErrorStatement => 1,
        AutoInactiveDestroy => 1,
        pg_enable_utf8 => 1
    }) or die "Cannot reconnect to database '$database_name': $DBI::errstr\n";
    
    # Re-set GUC variables after reconnection
    set_guc_variables($dbh);
    
    # Create indexes on parent table
    print "\nCreating indexes on parent table 'kafsss_data'...\n";
    create_parent_table_indexes($dbh, $op_class, $tablespace, $use_highfreq_cache);
    
    # Do not free cache - PostgreSQL will handle it automatically on disconnect
    # This prevents interfering with other processes that may be using the cache
    
    # Update kafsss_meta table
    update_meta_table($dbh);
    
    print "All indexes created successfully.\n";
}

sub create_parent_table_indexes {
    my ($dbh, $op_class, $tablespace, $use_highfreq_cache) = @_;
    
    # Set GUC variables for parent process
    if ($use_highfreq_cache) {
        eval {
            $dbh->do("SET kmersearch.preclude_highfreq_kmer = true");
            $dbh->do("SET kmersearch.force_use_parallel_highfreq_kmer_cache = true");
            print "Enabled high-frequency k-mer exclusion for parent table.\n";
        };
        if ($@) {
            die "Failed to set GUC variables for parent table: $@\n";
        }
        
        # Verify cache is loaded
        eval {
            my $load_sth = $dbh->prepare("SELECT kmersearch_parallel_highfreq_kmer_cache_load('kafsss_data', 'seq')");
            $load_sth->execute();
            my ($load_result) = $load_sth->fetchrow_array();
            $load_sth->finish();
            
            if (!$load_result) {
                die "kmersearch_parallel_highfreq_kmer_cache_load() returned false\n";
            }
            print "Verified high-frequency k-mer cache is loaded.\n";
        };
        if ($@) {
            die "Failed to verify cache load: $@\n";
        }
    } else {
        eval {
            $dbh->do("SET kmersearch.preclude_highfreq_kmer = false");
            $dbh->do("SET kmersearch.force_use_parallel_highfreq_kmer_cache = false");
            print "Disabled high-frequency k-mer exclusion for parent table.\n";
        };
        if ($@) {
            die "Failed to set GUC variables for parent table: $@\n";
        }
    }
    
    # Create subset column GIN index on parent table
    my $subset_index_name = 'idx_kafsss_data_subset_gin';
    print "Creating parent index '$subset_index_name'...\n";
    my $subset_sql = "CREATE INDEX IF NOT EXISTS $subset_index_name ON kafsss_data USING gin(subset)";
    if ($tablespace) {
        $subset_sql .= " TABLESPACE \"$tablespace\"";
    }
    
    eval {
        $dbh->do($subset_sql);
        print "Parent index '$subset_index_name' created successfully.\n";
    };
    if ($@) {
        die "Failed to create parent index '$subset_index_name': $@\n";
    }
    
    # Create seqid column GIN index on parent table
    my $seqid_index_name = 'idx_kafsss_data_seqid_gin';
    print "Creating parent index '$seqid_index_name'...\n";
    my $seqid_sql = "CREATE INDEX IF NOT EXISTS $seqid_index_name ON kafsss_data USING gin(seqid)";
    if ($tablespace) {
        $seqid_sql .= " TABLESPACE \"$tablespace\"";
    }
    
    eval {
        $dbh->do($seqid_sql);
        print "Parent index '$seqid_index_name' created successfully.\n";
    };
    if ($@) {
        die "Failed to create parent index '$seqid_index_name': $@\n";
    }
    
    # Create seq column GIN index on parent table
    my $seq_index_name = 'idx_kafsss_data_seq_gin';
    print "Creating parent index '$seq_index_name' with operator class '$op_class'...\n";
    my $seq_sql = "CREATE INDEX IF NOT EXISTS $seq_index_name ON kafsss_data USING gin(seq $op_class)";
    if ($tablespace) {
        $seq_sql .= " TABLESPACE \"$tablespace\"";
    }
    
    eval {
        $dbh->do($seq_sql);
        print "Parent index '$seq_index_name' created successfully.\n";
    };
    if ($@) {
        die "Failed to create parent index '$seq_index_name': $@\n";
    }
}

sub drop_indexes {
    my ($dbh) = @_;
    
    print "Dropping GIN indexes...\n";
    
    # Get existing indexes
    my $existing_indexes = get_existing_indexes($dbh);
    
    # List of indexes to drop (for seq, subset, and seqid columns)
    my @indexes_to_drop = ();
    
    # Find all indexes on seq, subset, and seqid columns
    for my $index_name (keys %$existing_indexes) {
        my $index_info = $existing_indexes->{$index_name};
        if ($index_info->{columns} =~ /\b(seq|subset|seqid)\b/) {
            push @indexes_to_drop, $index_name;
        }
    }
    
    if (@indexes_to_drop == 0) {
        print "No indexes found on 'seq', 'subset', or 'seqid' columns.\n";
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
    
    # Clear kafsss_meta table
    clear_meta_table($dbh);
    
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
WHERE i.tablename = 'kafsss_data'
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

sub validate_and_set_memory_parameters {
    my ($dbh, $work_mem_value, $maintenance_work_mem_value, $temp_buffers_value) = @_;
    
    print "Setting memory parameters...\n";
    
    # Set work_mem
    eval {
        $dbh->do("SET work_mem = '$work_mem_value'");
        my $sth = $dbh->prepare("SHOW work_mem");
        $sth->execute();
        my ($actual_value) = $sth->fetchrow_array();
        $sth->finish();
        print "work_mem set to '$actual_value' (from '$work_mem_value').\n";
    };
    if ($@) {
        die "Invalid work_mem value '$work_mem_value': $@\n";
    }
    
    # Set maintenance_work_mem
    eval {
        $dbh->do("SET maintenance_work_mem = '$maintenance_work_mem_value'");
        my $sth = $dbh->prepare("SHOW maintenance_work_mem");
        $sth->execute();
        my ($actual_value) = $sth->fetchrow_array();
        $sth->finish();
        print "maintenance_work_mem set to '$actual_value' (from '$maintenance_work_mem_value').\n";
    };
    if ($@) {
        die "Invalid maintenance_work_mem value '$maintenance_work_mem_value': $@\n";
    }
    
    # Set temp_buffers
    eval {
        $dbh->do("SET temp_buffers = '$temp_buffers_value'");
        my $sth = $dbh->prepare("SHOW temp_buffers");
        $sth->execute();
        my ($actual_value) = $sth->fetchrow_array();
        $sth->finish();
        print "temp_buffers set to '$actual_value' (from '$temp_buffers_value').\n";
    };
    if ($@) {
        die "Invalid temp_buffers value '$temp_buffers_value': $@\n";
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

sub get_postgresql_version {
    my ($dbh) = @_;
    
    my $version;
    my $sth = $dbh->prepare("SELECT version()");
    eval {
        $sth->execute();
        my ($version_string) = $sth->fetchrow_array();
        $sth->finish();
        
        # Extract major version number (e.g., "PostgreSQL 16.1" -> 16)
        if ($version_string =~ /PostgreSQL (\d+)\./) {
            $version = $1;
        } elsif ($version_string =~ /PostgreSQL (\d+)/) {
            $version = $1;
        } else {
            die "Could not parse PostgreSQL version: $version_string\n";
        }
    };
    
    if ($@) {
        die "Failed to get PostgreSQL version: $@\n";
    }
    
    return $version;
}

sub set_guc_variables {
    my ($dbh) = @_;
    
    print "Setting GUC variables...\n";
    
    # Set kmer_size
    eval {
        $dbh->do("SET kmersearch.kmer_size = $kmer_size");
        print "Set kmersearch.kmer_size = $kmer_size\n";
    };
    if ($@) {
        die "Failed to set kmersearch.kmer_size: $@\n";
    }
    
    # Set occur_bitlen
    eval {
        $dbh->do("SET kmersearch.occur_bitlen = $occur_bitlen");
        print "Set kmersearch.occur_bitlen = $occur_bitlen\n";
    };
    if ($@) {
        die "Failed to set kmersearch.occur_bitlen: $@\n";
    }
    
    # Set max_appearance_rate
    eval {
        $dbh->do("SET kmersearch.max_appearance_rate = $max_appearance_rate");
        print "Set kmersearch.max_appearance_rate = $max_appearance_rate\n";
    };
    if ($@) {
        die "Failed to set kmersearch.max_appearance_rate: $@\n";
    }
    
    # Set max_appearance_nrow
    eval {
        $dbh->do("SET kmersearch.max_appearance_nrow = $max_appearance_nrow");
        print "Set kmersearch.max_appearance_nrow = $max_appearance_nrow\n";
    };
    if ($@) {
        die "Failed to set kmersearch.max_appearance_nrow: $@\n";
    }
    
    # Set max_parallel_maintenance_workers if numthreads is specified
    if ($numthreads > 0) {
        eval {
            $dbh->do("SET max_parallel_maintenance_workers = $numthreads");
            print "Set max_parallel_maintenance_workers = $numthreads\n";
        };
        if ($@) {
            die "Failed to set max_parallel_maintenance_workers: $@\n";
        }
    }
    
    print "GUC variables set successfully.\n";
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
    my ($dbh, $username, $mode) = @_;
    
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
    
    # Check table permissions based on mode
    if ($mode eq 'create') {
        # Check if user can create indexes
        $sth = $dbh->prepare("SELECT has_table_privilege(?, 'kafsss_data', 'SELECT, INSERT, UPDATE, DELETE')");
        $sth->execute($username);
        my $has_table_perm = $sth->fetchrow_array();
        $sth->finish();
        
        unless ($has_table_perm) {
            die "Error: User '$username' does not have sufficient permissions on kafsss_data table.\n" .
                "Please grant permissions:\n" .
                "  sudo -u postgres psql -d " . $dbh->{pg_db} . "\n" .
                "  GRANT SELECT, INSERT, UPDATE, DELETE ON kafsss_data TO $username;\n" .
                "  \\q\n";
        }
    } elsif ($mode eq 'drop') {
        # Check if user can drop indexes
        $sth = $dbh->prepare("SELECT has_table_privilege(?, 'kafsss_data', 'SELECT')");
        $sth->execute($username);
        my $has_table_perm = $sth->fetchrow_array();
        $sth->finish();
        
        unless ($has_table_perm) {
            die "Error: User '$username' does not have SELECT permission on kafsss_data table.\n" .
                "Please grant permissions:\n" .
                "  sudo -u postgres psql -d " . $dbh->{pg_db} . "\n" .
                "  GRANT SELECT ON kafsss_data TO $username;\n" .
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
    
    print "Database schema validation completed.\n";
}

sub undo_highfreq_analysis {
    my ($dbh) = @_;
    
    print "Undoing high-frequency k-mer analysis...\n";
    
    eval {
        my $sth = $dbh->prepare("SELECT kmersearch_undo_highfreq_analysis('kafsss_data', 'seq')");
        $sth->execute();
        my ($result) = $sth->fetchrow_array();
        $sth->finish();
        
        print "High-frequency k-mer analysis undone: $result\n";
    };
    
    if ($@) {
        die "Failed to undo high-frequency k-mer analysis: $@\n";
    }
}



sub update_meta_table {
    my ($dbh) = @_;
    
    print "Updating kafsss_meta table...\n";
    
    # Check if kafsss_meta table has rows, insert if empty
    eval {
        my ($count) = $dbh->selectrow_array("SELECT COUNT(*) FROM kafsss_meta");
        if ($count == 0) {
            $dbh->do("INSERT INTO kafsss_meta DEFAULT VALUES");
        }
    };
    if ($@) {
        die "Failed to check kafsss_meta table: $@\n";
    }
    
    # Use do() method which works correctly after reconnection
    eval {
        my $sql = sprintf(
            "UPDATE kafsss_meta SET kmer_size = %d, max_appearance_rate = %f, max_appearance_nrow = %d, occur_bitlen = %d",
            $kmer_size, $max_appearance_rate, $max_appearance_nrow, $occur_bitlen
        );
        $dbh->do($sql);
        print "Metadata updated: kmer_size=$kmer_size, max_appearance_rate=$max_appearance_rate, max_appearance_nrow=$max_appearance_nrow, occur_bitlen=$occur_bitlen\n";
    };
    
    if ($@) {
        die "Failed to update kafsss_meta table: $@\n";
    }
}

sub clear_meta_table {
    my ($dbh) = @_;
    
    print "Clearing kafsss_meta table...\n";
    
    eval {
        my $update_sth = $dbh->prepare(<<SQL);
UPDATE kafsss_meta SET 
    kmer_size = NULL, 
    max_appearance_rate = NULL, 
    max_appearance_nrow = NULL, 
    occur_bitlen = NULL
SQL
        $update_sth->execute();
        $update_sth->finish();
        print "Metadata cleared from kafsss_meta table.\n";
    };
    
    if ($@) {
        print STDERR "Warning: Failed to clear kafsss_meta table: $@\n";
    }
}

sub load_and_validate_parameters {
    my ($dbh) = @_;
    
    print "Checking for parameters in kmersearch_highfreq_kmer table...\n";
    
    # Track which parameters were specified on command line
    my $kmer_size_specified = defined($kmer_size);
    my $max_appearance_rate_specified = defined($max_appearance_rate);
    my $max_appearance_nrow_specified = defined($max_appearance_nrow);
    my $occur_bitlen_specified = defined($occur_bitlen);
    
    # Try to get parameters from kmersearch_highfreq_kmer_meta table
    my $sth = $dbh->prepare(<<SQL);
SELECT DISTINCT kmer_size, occur_bitlen, max_appearance_rate, max_appearance_nrow
FROM kmersearch_highfreq_kmer_meta
WHERE table_oid = 'kafsss_data'::regclass
  AND column_name = 'seq'
SQL
    
    eval {
        $sth->execute();
        my @rows = ();
        while (my $row = $sth->fetchrow_hashref()) {
            push @rows, $row;
        }
        $sth->finish();
        
        if (@rows == 0) {
            # No data in kmersearch_highfreq_kmer_meta table
            print "No parameters found in kmersearch_highfreq_kmer_meta table.\n";
            
            # Use defaults if not specified
            $kmer_size = $kmer_size // $default_kmer_size;
            $max_appearance_rate = $max_appearance_rate // $default_max_appearance_rate;
            $max_appearance_nrow = $max_appearance_nrow // $default_max_appearance_nrow;
            $occur_bitlen = $occur_bitlen // $default_occur_bitlen;
            
            print "Using " . ($kmer_size_specified ? "specified" : "default") . " kmer_size: $kmer_size\n";
            print "Using " . ($max_appearance_rate_specified ? "specified" : "default") . " max_appearance_rate: $max_appearance_rate\n";
            print "Using " . ($max_appearance_nrow_specified ? "specified" : "default") . " max_appearance_nrow: $max_appearance_nrow\n";
            print "Using " . ($occur_bitlen_specified ? "specified" : "default") . " occur_bitlen: $occur_bitlen\n";
            print "Note: High-frequency k-mer exclusion will be disabled (no matching data in kmersearch_highfreq_kmer).\n";
        } elsif (@rows == 1) {
            # Found exactly one set of parameters
            my $row = $rows[0];
            my $db_kmer_size = $row->{kmer_size};
            my $db_occur_bitlen = $row->{occur_bitlen};
            my $db_max_appearance_rate = $row->{max_appearance_rate};
            my $db_max_appearance_nrow = $row->{max_appearance_nrow};
            
            print "Found parameters in kmersearch_highfreq_kmer_meta table:\n";
            print "  kmer_size: $db_kmer_size\n";
            print "  occur_bitlen: $db_occur_bitlen\n";
            print "  max_appearance_rate: $db_max_appearance_rate\n";
            print "  max_appearance_nrow: $db_max_appearance_nrow\n";
            
            # Validate or use database values
            if ($kmer_size_specified) {
                if ($kmer_size != $db_kmer_size) {
                    die "Error: Specified kmer_size ($kmer_size) does not match value in kmersearch_highfreq_kmer_meta table ($db_kmer_size).\n" .
                        "Please use --kmersize=$db_kmer_size or run kafssfreq again with --kmersize=$kmer_size.\n";
                }
            } else {
                $kmer_size = $db_kmer_size;
                print "Using kmer_size from database: $kmer_size\n";
            }
            
            if ($occur_bitlen_specified) {
                if ($occur_bitlen != $db_occur_bitlen) {
                    die "Error: Specified occur_bitlen ($occur_bitlen) does not match value in kmersearch_highfreq_kmer_meta table ($db_occur_bitlen).\n" .
                        "Please use --occurbitlen=$db_occur_bitlen or run kafssfreq again with --occurbitlen=$occur_bitlen.\n";
                }
            } else {
                $occur_bitlen = $db_occur_bitlen;
                print "Using occur_bitlen from database: $occur_bitlen\n";
            }
            
            if ($max_appearance_rate_specified) {
                if (abs($max_appearance_rate - $db_max_appearance_rate) > 0.0001) {
                    die "Error: Specified max_appearance_rate ($max_appearance_rate) does not match value in kmersearch_highfreq_kmer_meta table ($db_max_appearance_rate).\n" .
                        "Please use --maxpappear=$db_max_appearance_rate or run kafssfreq again with --maxpappear=$max_appearance_rate.\n";
                }
            } else {
                $max_appearance_rate = $db_max_appearance_rate;
                print "Using max_appearance_rate from database: $max_appearance_rate\n";
            }
            
            if ($max_appearance_nrow_specified) {
                if ($max_appearance_nrow != $db_max_appearance_nrow) {
                    die "Error: Specified max_appearance_nrow ($max_appearance_nrow) does not match value in kmersearch_highfreq_kmer_meta table ($db_max_appearance_nrow).\n" .
                        "Please use --maxnappear=$db_max_appearance_nrow or run kafssfreq again with --maxnappear=$max_appearance_nrow.\n";
                }
            } else {
                $max_appearance_nrow = $db_max_appearance_nrow;
                print "Using max_appearance_nrow from database: $max_appearance_nrow\n";
            }
        } else {
            # Multiple different parameter sets found
            die "Error: Multiple different parameter sets found in kmersearch_highfreq_kmer_meta table.\n" .
                "This indicates inconsistent frequency analysis. Please run kafssfreq again to fix this.\n";
        }
    };
    
    if ($@) {
        # Error accessing table - use defaults
        print "Warning: Could not access kmersearch_highfreq_kmer_meta table: $@";
        print "Using default or specified parameters.\n";
        
        $kmer_size = $kmer_size // $default_kmer_size;
        $max_appearance_rate = $max_appearance_rate // $default_max_appearance_rate;
        $max_appearance_nrow = $max_appearance_nrow // $default_max_appearance_nrow;
        $occur_bitlen = $occur_bitlen // $default_occur_bitlen;
    }
    
    # Final validation of parameters
    die "kmersize must be between 4 and 64\n" unless $kmer_size >= 4 && $kmer_size <= 64;
    die "maxpappear must be between 0.0 and 1.0\n" unless $max_appearance_rate >= 0.0 && $max_appearance_rate <= 1.0;
    die "maxnappear must be non-negative\n" unless $max_appearance_nrow >= 0;
    die "occurbitlen must be between 0 and 16\n" unless $occur_bitlen >= 0 && $occur_bitlen <= 16;
}

sub check_if_partitioned {
    my ($dbh, $table_name) = @_;
    
    my $sth = $dbh->prepare(<<SQL);
SELECT COUNT(*) 
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relname = ? 
AND c.relkind = 'p'
AND n.nspname = 'public'
SQL
    
    $sth->execute($table_name);
    my ($count) = $sth->fetchrow_array();
    $sth->finish();
    
    return $count > 0;
}

sub get_partitions {
    my ($dbh, $parent_table) = @_;
    
    my $sth = $dbh->prepare(<<SQL);
SELECT 
    c.relname AS partition_name
FROM pg_inherits i
JOIN pg_class p ON p.oid = i.inhparent
JOIN pg_class c ON c.oid = i.inhrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE p.relname = ?
AND n.nspname = 'public'
ORDER BY c.relname
SQL
    
    $sth->execute($parent_table);
    
    my @partitions = ();
    while (my ($partition_name) = $sth->fetchrow_array()) {
        push @partitions, $partition_name;
    }
    $sth->finish();
    
    return @partitions;
}


sub create_partition_indexes_for_column {
    my ($partitions_ref, $num_workers, $column, $tablespace, $pg_version, $use_highfreq_cache) = @_;
    my @partitions = @$partitions_ref;
    
    use POSIX ':sys_wait_h';
    
    my %active_workers = ();
    my $partition_index = 0;
    
    while ($partition_index < @partitions || keys %active_workers > 0) {
        # Start new workers if we have capacity and partitions to process
        while (keys %active_workers < $num_workers && $partition_index < @partitions) {
            my $partition = $partitions[$partition_index];
            $partition_index++;
            
            my $pid = fork();
            if (!defined $pid) {
                die "Failed to fork worker process: $!\n";
            } elsif ($pid == 0) {
                # Child process
                eval {
                    create_single_partition_index($partition, $column, $tablespace, 
                                                 $pg_version, $use_highfreq_cache);
                };
                if ($@) {
                    print STDERR "[Worker $$] Error: $@\n";
                    exit 1;
                }
                exit 0;
            } else {
                # Parent process
                $active_workers{$pid} = $partition;
                print "Started worker PID $pid for partition '$partition' column '$column->{name}'\n";
            }
        }
        
        # Wait for any worker to finish (blocking wait if at max workers)
        if (keys %active_workers >= $num_workers || $partition_index >= @partitions) {
            my $finished_pid = waitpid(-1, 0);  # Blocking wait
            if ($finished_pid > 0) {
                my $partition = $active_workers{$finished_pid};
                my $exit_status = $? >> 8;
                
                if ($exit_status == 0) {
                    print "Worker PID $finished_pid completed successfully for partition '$partition'\n";
                } else {
                    # Kill all remaining workers and clean up
                    foreach my $worker_pid (keys %active_workers) {
                        kill 'TERM', $worker_pid if $worker_pid != $finished_pid;
                    }
                    # Wait for all workers to exit
                    while (keys %active_workers > 0) {
                        my $pid = waitpid(-1, 0);
                        delete $active_workers{$pid} if $pid > 0;
                    }
                    die "Worker PID $finished_pid failed for partition '$partition' with exit code $exit_status\n";
                }
                
                delete $active_workers{$finished_pid};
            }
        } else {
            # Non-blocking check when we can start more workers
            my $finished_pid = waitpid(-1, WNOHANG);
            if ($finished_pid > 0) {
                my $partition = $active_workers{$finished_pid};
                my $exit_status = $? >> 8;
                
                if ($exit_status == 0) {
                    print "Worker PID $finished_pid completed successfully for partition '$partition'\n";
                } else {
                    # Kill all remaining workers and clean up
                    foreach my $worker_pid (keys %active_workers) {
                        kill 'TERM', $worker_pid if $worker_pid != $finished_pid;
                    }
                    # Wait for all workers to exit
                    while (keys %active_workers > 0) {
                        my $pid = waitpid(-1, 0);
                        delete $active_workers{$pid} if $pid > 0;
                    }
                    die "Worker PID $finished_pid failed for partition '$partition' with exit code $exit_status\n";
                }
                
                delete $active_workers{$finished_pid};
            }
        }
    }
}

sub create_single_partition_index {
    my ($partition_name, $column, $tablespace, $pg_version, $use_highfreq_cache) = @_;
    
    # Each worker creates its own database connection
    my $password = $ENV{PGPASSWORD} || '';
    my $dsn = "DBI:Pg:dbname=$database_name;host=$host;port=$port";
    
    my $worker_dbh = DBI->connect($dsn, $username, $password, {
        AutoCommit => 1,
        PrintError => 0,
        RaiseError => 1,
        ShowErrorStatement => 1,
        AutoInactiveDestroy => 1,
        pg_enable_utf8 => 1
    }) or die "[Worker $$] Cannot connect to database '$database_name': $DBI::errstr\n";
    
    # Set GUC variables in worker
    eval {
        $worker_dbh->do("SET kmersearch.kmer_size = $kmer_size");
        $worker_dbh->do("SET kmersearch.occur_bitlen = $occur_bitlen");
        $worker_dbh->do("SET kmersearch.max_appearance_rate = $max_appearance_rate");
        $worker_dbh->do("SET kmersearch.max_appearance_nrow = $max_appearance_nrow");
        
        if ($use_highfreq_cache) {
            $worker_dbh->do("SET kmersearch.preclude_highfreq_kmer = true");
            $worker_dbh->do("SET kmersearch.force_use_parallel_highfreq_kmer_cache = true");
        } else {
            $worker_dbh->do("SET kmersearch.preclude_highfreq_kmer = false");
            $worker_dbh->do("SET kmersearch.force_use_parallel_highfreq_kmer_cache = false");
        }
    };
    if ($@) {
        $worker_dbh->disconnect() if $worker_dbh;
        die "[Worker $$] Failed to set GUC variables: $@\n";
    }
    
    # Set memory parameters
    eval {
        $worker_dbh->do("SET work_mem = '$workingmemory'");
        $worker_dbh->do("SET maintenance_work_mem = '$maintenanceworkingmemory'");
        $worker_dbh->do("SET temp_buffers = '$temporarybuffer'");
    };
    if ($@) {
        $worker_dbh->disconnect() if $worker_dbh;
        die "[Worker $$] Failed to set memory parameters: $@\n";
    }
    
    # Note: Cache should be loaded by parent process or kafsspreload before running kafssindex
    # Worker processes should NOT try to load cache themselves
    if ($use_highfreq_cache && $column->{name} eq 'seq') {
        print "[Worker $$] Using pre-loaded high-frequency k-mer cache.\n";
    }
    
    # Create index on partition with proper error handling
    my $index_name = "idx_${partition_name}_$column->{name}_gin";
    my $index_sql = "CREATE INDEX IF NOT EXISTS $index_name ON $partition_name USING gin($column->{name}";
    
    # Add operator class for seq column
    if ($column->{op_class}) {
        $index_sql .= " $column->{op_class}";
    }
    $index_sql .= ")";
    
    if ($tablespace) {
        $index_sql .= " TABLESPACE \"$tablespace\"";
    }
    
    print "[Worker $$] Creating index '$index_name' on partition '$partition_name'...\n";
    
    # Try to create index with error handling
    eval {
        local $SIG{PIPE} = 'IGNORE';  # Ignore SIGPIPE
        $worker_dbh->do($index_sql);
        print "[Worker $$] Index '$index_name' created successfully.\n";
    };
    
    my $error = $@;
    
    # Always disconnect properly
    if ($worker_dbh) {
        eval { $worker_dbh->disconnect(); };
    }
    
    if ($error) {
        die "[Worker $$] Failed to create index '$index_name': $error\n";
    }
    
    print "[Worker $$] Completed indexing column '$column->{name}' on partition '$partition_name'.\n";
}