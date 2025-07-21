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
my $kmer_size = $default_kmer_size;
my $max_appearance_rate = $default_max_appearance_rate;
my $max_appearance_nrow = $default_max_appearance_nrow;
my $occur_bitlen = $default_occur_bitlen;
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
    'kmer_size=i' => \$kmer_size,
    'max_appearance_rate=f' => \$max_appearance_rate,
    'max_appearance_nrow=i' => \$max_appearance_nrow,
    'occur_bitlen=i' => \$occur_bitlen,
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
    die "Usage: af_kmerindex [options] database_name\n" .
        "Use --help for detailed usage information.\n";
}

my ($database_name) = @ARGV;

# Validate required options
die "Mode must be specified with --mode option (create or drop)\n" unless $mode;
die "Invalid mode '$mode'. Must be 'create' or 'drop'\n" unless $mode eq 'create' || $mode eq 'drop';

# Validate parameters
die "kmer_size must be between 4 and 64\n" unless $kmer_size >= 4 && $kmer_size <= 64;
die "max_appearance_rate must be between 0.0 and 1.0\n" unless $max_appearance_rate >= 0.0 && $max_appearance_rate <= 1.0;
die "max_appearance_nrow must be non-negative\n" unless $max_appearance_nrow >= 0;
die "occur_bitlen must be between 0 and 16\n" unless $occur_bitlen >= 0 && $occur_bitlen <= 16;
die "numthreads must be non-negative\n" unless $numthreads >= 0;

print "af_kmerindex version $VERSION\n";
print "Database: $database_name\n";
print "Host: $host\n";
print "Port: $port\n";
print "Username: $username\n";
print "Tablespace: " . ($tablespace ? $tablespace : 'default') . "\n";
print "Mode: $mode\n";
print "K-mer size: $kmer_size\n";
print "Max appearance rate: $max_appearance_rate\n";
print "Max appearance nrow: $max_appearance_nrow\n";
print "Occur bitlen: $occur_bitlen\n";
print "Num threads: " . ($numthreads ? $numthreads : 'default') . "\n";
print "Working memory: $workingmemory\n";
print "Maintenance working memory: $maintenanceworkingmemory\n";
print "Temporary buffer: $temporarybuffer\n";

# Connect to PostgreSQL server first for validation
my $password = $ENV{PGPASSWORD} || '';
my $server_dsn = "DBI:Pg:host=$host;port=$port";

my $server_dbh = DBI->connect($server_dsn, $username, $password, {
    RaiseError => 1,
    AutoCommit => 1,
    pg_enable_utf8 => 1
}) or die "Cannot connect to PostgreSQL server: $DBI::errstr\n";

# Validate user existence and permissions
validate_user_and_permissions($server_dbh, $username);

# Check if database exists
unless (check_database_exists($server_dbh, $database_name)) {
    $server_dbh->disconnect();
    die "Error: Database '$database_name' does not exist.\n" .
        "Please create it first using af_kmerstore.\n";
}

$server_dbh->disconnect();

# Connect to target database
my $dsn = "DBI:Pg:dbname=$database_name;host=$host;port=$port";
my $dbh = DBI->connect($dsn, $username, $password, {
    RaiseError => 1,
    AutoCommit => 1,
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

# Set GUC variables
set_guc_variables($dbh);

# Execute the requested operation
if ($mode eq 'create') {
    create_indexes($dbh, $pg_version);
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
af_kmerindex version $VERSION

Usage: af_kmerindex [options] database_name

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
  --max_appearance_rate=REAL  Max k-mer appearance rate (default: 0.5, range: 0.0-1.0)
  --max_appearance_nrow=INT   Max rows containing k-mer (default: 0=unlimited)
  --occur_bitlen=INT          Bits for occurrence count (default: 8, range: 0-16)
  --numthreads=INT            Number of parallel workers (default: 0=auto)
  --workingmemory=SIZE        Work memory for each operation (default: 8GB)
  --maintenanceworkingmemory=SIZE  Maintenance work memory for index creation (default: 8GB)
  --temporarybuffer=SIZE      Temporary buffer size (default: 512MB)
  --verbose, -v     Show detailed processing messages (default: false)
  --help, -h        Show this help message

Environment variables:
  PGHOST           PostgreSQL server host
  PGPORT           PostgreSQL server port
  PGUSER           PostgreSQL username
  PGPASSWORD       PostgreSQL password

Examples:
  af_kmerindex --mode=create mydb
  af_kmerindex --mode=drop mydb
  af_kmerindex --mode=create --tablespace=fast_ssd mydb
  af_kmerindex --mode=create --kmer_size=16 mydb
  af_kmerindex --mode=create --maintenanceworkingmemory=64GB mydb
  af_kmerindex --mode=create --kmer_size=32 --maintenanceworkingmemory=128GB --temporarybuffer=1GB mydb
  af_kmerindex --mode=create --workingmemory=32GB --maintenanceworkingmemory=128GB --tablespace=fast_ssd mydb
  af_kmerindex --mode=create --max_appearance_rate=0.3 --max_appearance_nrow=500 mydb
  af_kmerindex --mode=create --occur_bitlen=12 --numthreads=8 mydb

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

sub create_indexes {
    my ($dbh, $pg_version) = @_;
    
    print "Creating GIN indexes...\n";
    
    # Validate and set memory parameters
    validate_and_set_memory_parameters($dbh, $workingmemory, $maintenanceworkingmemory, $temporarybuffer);
    
    # Determine if high-frequency analysis should be performed
    my $should_perform_highfreq_analysis = ($max_appearance_rate > 0 || $max_appearance_nrow > 0);
    my $highfreq_kmer_count = 0;
    my $cache_loaded = 0;
    
    if ($should_perform_highfreq_analysis) {
        # Perform high-frequency k-mer analysis
        perform_highfreq_analysis($dbh);
        
        # Check if any high-frequency k-mers were found
        my $count_sth = $dbh->prepare("SELECT COUNT(*) FROM kmersearch_highfreq_kmer WHERE table_oid = 'af_kmersearch'::regclass AND column_name = 'seq'");
        $count_sth->execute();
        ($highfreq_kmer_count) = $count_sth->fetchrow_array();
        $count_sth->finish();
        
        print "High-frequency k-mer analysis found $highfreq_kmer_count high-frequency k-mers.\n";
        
        if ($highfreq_kmer_count > 0) {
            # Load cache before creating indexes
            load_cache($dbh, $pg_version);
            $cache_loaded = 1;
            
            # Enable high-frequency k-mer exclusion
            eval {
                $dbh->do("SET kmersearch.preclude_highfreq_kmer = true");
                print "Enabled high-frequency k-mer exclusion.\n";
            };
            if ($@) {
                print "Warning: Failed to set kmersearch.preclude_highfreq_kmer: $@\n";
            }
            
            # Enable parallel cache if PostgreSQL 18+
            if ($pg_version >= 18) {
                eval {
                    $dbh->do("SET kmersearch.force_use_parallel_highfreq_kmer_cache = true");
                    print "Enabled parallel high-frequency k-mer cache.\n";
                };
                if ($@) {
                    print "Warning: Failed to set kmersearch.force_use_parallel_highfreq_kmer_cache: $@\n";
                }
            }
        }
    } else {
        print "High-frequency k-mer analysis disabled (max_appearance_rate=0 and max_appearance_nrow=0).\n";
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
    
    # Free cache after creating indexes (only if cache was loaded)
    if ($cache_loaded) {
        free_cache($dbh, $pg_version);
    }
    
    # Update af_kmersearch_meta table
    update_meta_table($dbh);
    
    print "All indexes created successfully.\n";
}

sub drop_indexes {
    my ($dbh) = @_;
    
    print "Dropping GIN indexes...\n";
    
    # Undo high-frequency k-mer analysis
    undo_highfreq_analysis($dbh);
    
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
    
    # Clear af_kmersearch_meta table
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

sub perform_highfreq_analysis {
    my ($dbh) = @_;
    
    print "Performing high-frequency k-mer analysis...\n";
    
    eval {
        my $sth = $dbh->prepare("SELECT kmersearch_perform_highfreq_analysis('af_kmersearch', 'seq')");
        $sth->execute();
        my ($result) = $sth->fetchrow_array();
        $sth->finish();
        
        print "High-frequency k-mer analysis completed: $result\n";
    };
    
    if ($@) {
        die "Failed to perform high-frequency k-mer analysis: $@\n";
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
    } elsif ($mode eq 'drop') {
        # Check if user can drop indexes
        $sth = $dbh->prepare("SELECT has_table_privilege(?, 'af_kmersearch', 'SELECT')");
        $sth->execute($username);
        my $has_table_perm = $sth->fetchrow_array();
        $sth->finish();
        
        unless ($has_table_perm) {
            die "Error: User '$username' does not have SELECT permission on af_kmersearch table.\n" .
                "Please grant permissions:\n" .
                "  sudo -u postgres psql -d " . $dbh->{pg_db} . "\n" .
                "  GRANT SELECT ON af_kmersearch TO $username;\n" .
                "  \\q\n";
        }
    }
    
    print "Database permissions validated.\n";
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

sub undo_highfreq_analysis {
    my ($dbh) = @_;
    
    print "Undoing high-frequency k-mer analysis...\n";
    
    eval {
        my $sth = $dbh->prepare("SELECT kmersearch_undo_highfreq_analysis('af_kmersearch', 'seq')");
        $sth->execute();
        my ($result) = $sth->fetchrow_array();
        $sth->finish();
        
        print "High-frequency k-mer analysis undone: $result\n";
    };
    
    if ($@) {
        die "Failed to undo high-frequency k-mer analysis: $@\n";
    }
}

sub load_cache {
    my ($dbh, $pg_version) = @_;
    
    print "Loading k-mer cache...\n";
    
    eval {
        if ($pg_version >= 18) {
            # PostgreSQL 18+: Use parallel cache
            my $sth = $dbh->prepare("SELECT kmersearch_parallel_highfreq_kmer_cache_load('af_kmersearch', 'seq')");
            $sth->execute();
            $sth->finish();
            print "Parallel k-mer cache loaded.\n";
        } else {
            # PostgreSQL 16-17: Use global cache
            my $sth = $dbh->prepare("SELECT kmersearch_highfreq_kmer_cache_load('af_kmersearch', 'seq')");
            $sth->execute();
            $sth->finish();
            print "Global k-mer cache loaded.\n";
        }
    };
    
    if ($@) {
        die "Failed to load k-mer cache: $@\n";
    }
}

sub free_cache {
    my ($dbh, $pg_version) = @_;
    
    print "Freeing k-mer cache...\n";
    
    eval {
        if ($pg_version >= 18) {
            # PostgreSQL 18+: Use parallel cache
            my $sth = $dbh->prepare("SELECT kmersearch_parallel_highfreq_kmer_cache_free('af_kmersearch', 'seq')");
            $sth->execute();
            $sth->finish();
            print "Parallel k-mer cache freed.\n";
        } else {
            # PostgreSQL 16-17: Use global cache
            my $sth = $dbh->prepare("SELECT kmersearch_highfreq_kmer_cache_free('af_kmersearch', 'seq')");
            $sth->execute();
            $sth->finish();
            print "Global k-mer cache freed.\n";
        }
    };
    
    if ($@) {
        die "Failed to free k-mer cache: $@\n";
    }
}

sub update_meta_table {
    my ($dbh) = @_;
    
    print "Updating af_kmersearch_meta table...\n";
    
    eval {
        my $update_sth = $dbh->prepare(<<SQL);
UPDATE af_kmersearch_meta SET 
    kmer_size = ?, 
    max_appearance_rate = ?, 
    max_appearance_nrow = ?, 
    occur_bitlen = ?
SQL
        $update_sth->execute($kmer_size, $max_appearance_rate, $max_appearance_nrow, $occur_bitlen);
        $update_sth->finish();
        print "Metadata updated: kmer_size=$kmer_size, max_appearance_rate=$max_appearance_rate, max_appearance_nrow=$max_appearance_nrow, occur_bitlen=$occur_bitlen\n";
    };
    
    if ($@) {
        die "Failed to update af_kmersearch_meta table: $@\n";
    }
}

sub clear_meta_table {
    my ($dbh) = @_;
    
    print "Clearing af_kmersearch_meta table...\n";
    
    eval {
        my $update_sth = $dbh->prepare(<<SQL);
UPDATE af_kmersearch_meta SET 
    kmer_size = NULL, 
    max_appearance_rate = NULL, 
    max_appearance_nrow = NULL, 
    occur_bitlen = NULL
SQL
        $update_sth->execute();
        $update_sth->finish();
        print "Metadata cleared from af_kmersearch_meta table.\n";
    };
    
    if ($@) {
        print STDERR "Warning: Failed to clear af_kmersearch_meta table: $@\n";
    }
}