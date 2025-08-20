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

# Command line options
my $host = $default_host;
my $port = $default_port;
my $username = $default_user;
my $workingmemory = '8GB';
my $maintenanceworkingmemory = '8GB';
my $temporarybuffer = '512MB';
my $tablespace = '';
my $verbose = 0;
my $help = 0;

# Parse command line options
GetOptions(
    'host=s' => \$host,
    'port=i' => \$port,
    'username=s' => \$username,
    'workingmemory=s' => \$workingmemory,
    'maintenanceworkingmemory=s' => \$maintenanceworkingmemory,
    'temporarybuffer=s' => \$temporarybuffer,
    'tablespace=s' => \$tablespace,
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
    die "Usage: kafssdedup [options] database_name\n" .
        "Use --help for detailed usage information.\n";
}

my ($database_name) = @ARGV;

print "kafssdedup version $VERSION\n";
print "Database: $database_name\n";
print "Host: $host\n";
print "Port: $port\n";
print "Username: $username\n";
print "Working memory: $workingmemory\n";
print "Maintenance working memory: $maintenanceworkingmemory\n";
print "Temporary buffer: $temporarybuffer\n";
print "Tablespace: " . ($tablespace ? $tablespace : "(default)") . "\n" if $verbose;

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

# Check if database exists
my $db_exists = check_database_exists($server_dbh, $database_name);
unless ($db_exists) {
    die "Database '$database_name' does not exist\n";
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

# Check if kafsss_data table exists
ensure_table_exists($dbh, 'kafsss_data');

# Detect existing compression attributes from kafsss_data table
print "Detecting compression attributes from kafsss_data table...\n" if $verbose;
my ($storage_attr, $compression_attr) = detect_table_compression_attributes($dbh);

# Acquire advisory lock for exclusive access
print "Acquiring exclusive lock...\n";
eval {
    $dbh->do("SELECT pg_advisory_xact_lock(999)");
    print "Exclusive lock acquired.\n";
};
if ($@) {
    die "Failed to acquire advisory lock: $@\n";
}

# Configure PostgreSQL for efficient deduplication (non-parallel)
print "Configuring PostgreSQL for non-parallel deduplication...\n" if $verbose;

# Disable parallel processing
$dbh->do("SET max_parallel_workers_per_gather = 0");
print "Parallel processing disabled for deduplication.\n" if $verbose;

# Set memory parameters
validate_and_set_working_memory($dbh, $workingmemory);
validate_and_set_maintenance_memory($dbh, $maintenanceworkingmemory);
validate_and_set_temp_buffers($dbh, $temporarybuffer);

# Set other memory parameters for efficient processing
$dbh->do("SET random_page_cost = 1.1");

print "Memory configuration: work_mem=$workingmemory" . 
      ", temp_buffers=$temporarybuffer" . 
      ", maintenance_work_mem=$maintenanceworkingmemory\n" if $verbose;

# Additional memory control settings
$dbh->do("SET enable_hashagg = on");           # Force hash aggregation for better memory control
$dbh->do("SET enable_mergejoin = off");        # Disable merge joins to save memory
$dbh->do("SET enable_nestloop = off");         # Disable nested loop joins
$dbh->do("SET hash_mem_multiplier = 1.0");     # Limit hash table memory expansion

# Check and clean up any leftover intermediate tables
print "Checking for leftover intermediate tables...\n" if $verbose;
cleanup_intermediate_tables($dbh);

# Create custom aggregate function for array concatenation
print "Creating custom aggregate function array_cat_agg()...\n" if $verbose;
create_custom_aggregate_functions($dbh);

# Run deduplication
print "Starting deduplication process...\n";
my $duplicate_count = deduplicate_sequences_simple($dbh, $tablespace);

# Verify table exists after deduplication
ensure_table_exists($dbh, 'kafsss_data');

# Final verification with record count
my $final_count = $dbh->selectrow_array("SELECT COUNT(*) FROM kafsss_data");
unless (defined $final_count) {
    die "Table verification failed: kafsss_data table missing after deduplication\n";
}
print "Deduplication verified. Final count: $final_count sequences.\n";

# Update kafsss_meta table with new statistics
print "Updating kafsss_meta table with new statistics...\n";
update_meta_statistics($dbh);

print "Deduplication completed successfully.\n";

$dbh->disconnect();

exit 0;

#
# Subroutines
#

sub print_help {
    print <<EOF;
kafssdedup version $VERSION

Usage: kafssdedup [options] database_name

Remove duplicate sequences from kafsss_data table in PostgreSQL database using pg_kmersearch extension.

Required arguments:
  database_name     Target database name containing kafsss_data table

Options:
  --host=HOST                      PostgreSQL server host (default: \$PGHOST or localhost)
  --port=PORT                      PostgreSQL server port (default: \$PGPORT or 5432)
  --username=USER                  PostgreSQL username (default: \$PGUSER or current user)
  --workingmemory=SIZE             Working memory for deduplication (default: 8GB)
  --maintenanceworkingmemory=SIZE  Maintenance working memory (default: 8GB)
  --temporarybuffer=SIZE           Temporary buffer size (default: 512MB)
  --tablespace=NAME                Target tablespace for new tables (default: database default)
  --verbose, -v                    Show detailed processing messages (default: false)
  --help, -h                       Show this help message

Environment variables:
  PGHOST           PostgreSQL server host
  PGPORT           PostgreSQL server port
  PGUSER           PostgreSQL username
  PGPASSWORD       PostgreSQL password

Examples:
  # Basic deduplication
  kafssdedup mydb
  
  # With custom memory settings
  kafssdedup --workingmemory=32GB mydb
  kafssdedup --workingmemory=64GB --maintenanceworkingmemory=16GB mydb
  kafssdedup --workingmemory=32GB --temporarybuffer=1GB mydb
  
  # With tablespace specification
  kafssdedup --tablespace=fast_ssd mydb
  
  # With remote database
  kafssdedup --host=dbserver --username=myuser mydb
  
  # With verbose output
  kafssdedup --verbose --workingmemory=48GB mydb

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

sub ensure_table_exists {
    my ($dbh, $table_name) = @_;
    
    my $exists = $dbh->selectrow_array(
        "SELECT 1 FROM information_schema.tables WHERE table_name = ?",
        undef, $table_name
    );
    
    unless ($exists) {
        die "Critical error: Table '$table_name' does not exist\n";
    }
    
    print "Table '$table_name' verified to exist.\n" if $verbose;
}

sub validate_and_set_working_memory {
    my ($dbh, $memory_value) = @_;
    
    print "Validating and setting working memory to '$memory_value'...\n";
    
    # First, validate that PostgreSQL can recognize the memory value
    eval {
        # Test the memory value by temporarily setting it
        my $sth = $dbh->prepare("SELECT setting FROM pg_settings WHERE name = 'work_mem'");
        $sth->execute();
        my ($original_value) = $sth->fetchrow_array();
        $sth->finish();
        
        # Try to set the new value
        $dbh->do("SET work_mem = '$memory_value'");
        
        # If successful, get the actual value PostgreSQL understood
        $sth = $dbh->prepare("SHOW work_mem");
        $sth->execute();
        my ($actual_value) = $sth->fetchrow_array();
        $sth->finish();
        
        print "Working memory successfully set to: $actual_value\n";
        
        # Also check if the value seems reasonable (at least 1MB)
        $sth = $dbh->prepare("SELECT setting::int FROM pg_settings WHERE name = 'work_mem'");
        $sth->execute();
        my ($value_kb) = $sth->fetchrow_array();
        $sth->finish();
        
        if ($value_kb < 1024) {
            print "Warning: Working memory value seems quite small ($actual_value). Consider using a larger value for better performance.\n";
        }
        
    };
    
    if ($@) {
        die "Error: Invalid working memory value '$memory_value'. Please use a valid PostgreSQL memory size (e.g., '32GB', '2048MB', '2097152kB').\n" .
            "PostgreSQL error: $@\n";
    }
}

sub deduplicate_sequences_simple {
    my ($dbh, $tablespace) = @_;
    
    print "Starting non-parallel deduplication...\n" if $verbose;
    
    eval {
        $dbh->begin_work;
        
        my $original_count = $dbh->selectrow_array("SELECT COUNT(*) FROM kafsss_data");
        print "Original sequence count: $original_count\n";
        
        # Phase 1: Identify duplicate sequences
        print "Phase 1: Identifying duplicate sequences...\n";
        
        # Check if table exists before dropping
        my $dupseq_exists = $dbh->selectrow_array(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'kafsss_data_dupseq')"
        );
        if ($dupseq_exists) {
            $dbh->do("DROP TABLE kafsss_data_dupseq");
        }
        
        my $tablespace_clause = $tablespace ? " TABLESPACE $tablespace" : "";
        $dbh->do(<<SQL);
CREATE TABLE kafsss_data_dupseq$tablespace_clause AS 
SELECT seq 
FROM kafsss_data 
GROUP BY seq 
HAVING COUNT(*) > 1
SQL
        
        # Apply compression attributes to kafsss_data_dupseq table
        apply_table_compression($dbh, 'kafsss_data_dupseq', ['seq'], $storage_attr, $compression_attr);
        
        my $duplicate_count = $dbh->selectrow_array("SELECT COUNT(*) FROM kafsss_data_dupseq");
        print "Found $duplicate_count unique sequences with duplicates.\n";
        
        if ($duplicate_count > 0) {
            # Phase 2: Process only duplicate data
            print "Phase 2: Processing duplicate sequences...\n";
            
            # Check if table exists before dropping
            my $dedup_temp_exists = $dbh->selectrow_array(
                "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'kafsss_data_dedup_temp')"
            );
            if ($dedup_temp_exists) {
                $dbh->do("DROP TABLE kafsss_data_dedup_temp");
            }
            
            my $tablespace_clause = $tablespace ? " TABLESPACE $tablespace" : "";
            $dbh->do(<<SQL);
CREATE TABLE kafsss_data_dedup_temp$tablespace_clause AS
SELECT 
    a.seq,
    array_uniq(array_cat_agg(a.subset)) as subset,
    array_uniq(array_cat_agg(a.seqid)) as seqid
FROM kafsss_data a
JOIN kafsss_data_dupseq d ON d.seq = a.seq
GROUP BY a.seq
SQL
            
            # Apply compression attributes to kafsss_data_dedup_temp table
            apply_table_compression($dbh, 'kafsss_data_dedup_temp', ['seq', 'subset', 'seqid'], $storage_attr, $compression_attr);
            
            my $dedup_count = $dbh->selectrow_array("SELECT COUNT(*) FROM kafsss_data_dedup_temp");
            print "Created $dedup_count deduplicated records from duplicates.\n";
            
            # Phase 3: Remove duplicate rows from original table
            print "Phase 3: Removing duplicate sequences from original table...\n";
            my $deleted_count = $dbh->do(<<SQL);
DELETE FROM kafsss_data 
WHERE seq IN (SELECT seq FROM kafsss_data_dupseq)
SQL
            print "Removed $deleted_count duplicate rows from original table.\n";
            
            # Phase 4: Insert deduplicated data back and cleanup
            print "Phase 4: Inserting deduplicated data and cleanup...\n";
            $dbh->do(<<SQL);
INSERT INTO kafsss_data 
SELECT * FROM kafsss_data_dedup_temp
SQL
            
            $dbh->do("DROP TABLE kafsss_data_dedup_temp");
            print "Deduplication processing completed.\n";
        } else {
            print "No duplicate sequences found. Skipping deduplication processing.\n";
        }
        
        $dbh->do("DROP TABLE kafsss_data_dupseq");
        
        my $new_count = $dbh->selectrow_array("SELECT COUNT(*) FROM kafsss_data");
        
        $dbh->commit;
        
        my $removed = $original_count - $new_count;
        print "Deduplication completed. Removed $removed duplicate entries.\n";
        
        return $removed;
    };
    
    if ($@) {
        print STDERR "Deduplication failed: $@\n";
        eval { $dbh->rollback; };
        die "Deduplication process failed: $@\n";
    }
}

sub cleanup_intermediate_tables {
    my ($dbh) = @_;
    
    print "Checking for intermediate tables from previous runs...\n" if $verbose;
    
    # List of intermediate tables to clean up
    my @cleanup_tables = (
        'kafsss_data_dedup_temp',    # Current processing temp table
        'kafsss_data_dupseq'         # Current processing duplicate seq table
    );
    
    for my $table_name (@cleanup_tables) {
        # Check if table exists
        my $sth = $dbh->prepare(<<SQL);
SELECT 1 FROM information_schema.tables 
WHERE table_name = ? 
AND table_schema = 'public'
SQL
        
        $sth->execute($table_name);
        my $table_exists = $sth->fetchrow_array();
        $sth->finish();
        
        if ($table_exists) {
            print "Found leftover intermediate table '$table_name'. Removing it...\n";
            
            eval {
                $dbh->do("DROP TABLE \"$table_name\"");
                print "Successfully removed intermediate table '$table_name'.\n";
            };
            
            if ($@) {
                die "Error: Failed to remove intermediate table '$table_name': $@\n";
            }
        }
    }
    
    print "Intermediate table cleanup completed.\n" if $verbose;
}

sub create_custom_aggregate_functions {
    my ($dbh) = @_;
    
    print "Creating custom array concatenation functions...\n" if $verbose;
    
    # Create the state function for array concatenation
    eval {
        $dbh->do(<<SQL);
CREATE OR REPLACE FUNCTION array_cat_sfunc(state text[], new_array text[])
RETURNS text[] AS \$\$
BEGIN
    RETURN array_cat(COALESCE(state, '{}'), COALESCE(new_array, '{}'));
END;
\$\$ LANGUAGE plpgsql IMMUTABLE
SQL
    };
    
    if ($@) {
        die "Error: Failed to create array_cat_sfunc function: $@\n";
    }
    
    # Drop and recreate the aggregate function
    eval {
        # Check if aggregate exists before dropping
        my $check_agg = $dbh->prepare(<<SQL);
SELECT COUNT(*) FROM pg_proc p
JOIN pg_namespace n ON p.pronamespace = n.oid
WHERE n.nspname = 'public' AND p.proname = 'array_cat_agg'
SQL
        $check_agg->execute();
        my ($agg_exists) = $check_agg->fetchrow_array();
        $check_agg->finish();
        
        if ($agg_exists) {
            $dbh->do("DROP AGGREGATE array_cat_agg(text[])");
        }
        
        $dbh->do(<<SQL);
CREATE AGGREGATE array_cat_agg(text[]) (
    SFUNC = array_cat_sfunc,
    STYPE = text[],
    INITCOND = '{}'
)
SQL
    };
    
    if ($@) {
        die "Error: Failed to create array_cat_agg aggregate function: $@\n";
    }
    
    # Create array sort function
    eval {
        $dbh->do(<<SQL);
CREATE OR REPLACE FUNCTION array_sort(arr text[])
RETURNS text[] AS \$\$
BEGIN
    RETURN (SELECT array_agg(elem ORDER BY elem) FROM unnest(arr) as elem);
END;
\$\$ LANGUAGE plpgsql IMMUTABLE
SQL
    };
    
    if ($@) {
        die "Error: Failed to create array_sort function: $@\n";
    }
    
    # Create array unique function
    eval {
        $dbh->do(<<SQL);
CREATE OR REPLACE FUNCTION array_uniq(arr text[])
RETURNS text[] AS \$\$
BEGIN
    RETURN (SELECT array_agg(DISTINCT elem ORDER BY elem) FROM unnest(arr) as elem);
END;
\$\$ LANGUAGE plpgsql IMMUTABLE
SQL
    };
    
    if ($@) {
        die "Error: Failed to create array_uniq function: $@\n";
    }
    
    print "Custom functions array_cat_agg(), array_sort(), and array_uniq() created successfully.\n" if $verbose;
}

sub validate_and_set_maintenance_memory {
    my ($dbh, $memory_value) = @_;
    
    print "Validating and setting maintenance working memory to '$memory_value'...\n";
    
    eval {
        $dbh->do("SET maintenance_work_mem = '$memory_value'");
        
        my $sth = $dbh->prepare("SHOW maintenance_work_mem");
        $sth->execute();
        my ($actual_value) = $sth->fetchrow_array();
        $sth->finish();
        
        print "Maintenance working memory successfully set to: $actual_value\n";
    };
    
    if ($@) {
        die "Error: Invalid maintenance working memory value '$memory_value': $@\n";
    }
}

sub validate_and_set_temp_buffers {
    my ($dbh, $buffer_value) = @_;
    
    print "Validating and setting temporary buffer to '$buffer_value'...\n";
    
    eval {
        $dbh->do("SET temp_buffers = '$buffer_value'");
        
        my $sth = $dbh->prepare("SHOW temp_buffers");
        $sth->execute();
        my ($actual_value) = $sth->fetchrow_array();
        $sth->finish();
        
        print "Temporary buffer successfully set to: $actual_value\n";
    };
    
    if ($@) {
        die "Error: Invalid temporary buffer value '$buffer_value': $@\n";
    }
}

sub detect_table_compression_attributes {
    my ($dbh) = @_;
    
    print "Detecting compression attributes from kafsss_data.seq column...\n" if $verbose;
    
    my $sth = $dbh->prepare(<<SQL);
SELECT attstorage, attcompression 
FROM pg_attribute 
JOIN pg_class ON pg_attribute.attrelid = pg_class.oid 
WHERE pg_class.relname = 'kafsss_data' 
AND pg_attribute.attname = 'seq'
SQL
    
    $sth->execute();
    my ($storage, $compression) = $sth->fetchrow_array();
    $sth->finish();
    
    unless (defined $storage) {
        die "Failed to detect storage attribute for kafsss_data.seq column\n";
    }
    
    # Convert PostgreSQL storage codes to readable names
    my %storage_map = (
        'p' => 'PLAIN',
        'e' => 'EXTERNAL', 
        'x' => 'EXTENDED',
        'm' => 'MAIN'
    );
    
    my $storage_name = $storage_map{$storage} || $storage;
    my $compression_name = defined $compression ? $compression : 'none';
    
    print "Detected compression attributes: STORAGE=$storage_name";
    if ($compression_name ne 'none' && $storage_name eq 'EXTENDED') {
        print ", COMPRESSION=$compression_name";
    }
    print "\n";
    
    return ($storage_name, $compression_name);
}

sub apply_table_compression {
    my ($dbh, $table_name, $columns, $storage_attr, $compression_attr) = @_;
    
    print "Applying compression attributes to table '$table_name'...\n" if $verbose;
    
    for my $column (@$columns) {
        eval {
            # Set storage attribute
            $dbh->do("ALTER TABLE $table_name ALTER COLUMN $column SET STORAGE $storage_attr");
            
            # Set compression attribute only if storage is EXTENDED and compression is available
            if ($storage_attr eq 'EXTENDED' && $compression_attr ne 'none') {
                $dbh->do("ALTER TABLE $table_name ALTER COLUMN $column SET COMPRESSION $compression_attr");
                print "Applied STORAGE=$storage_attr, COMPRESSION=$compression_attr to $table_name.$column\n" if $verbose;
            } else {
                print "Applied STORAGE=$storage_attr to $table_name.$column\n" if $verbose;
            }
        };
        if ($@) {
            print STDERR "Warning: Failed to set compression attributes for $table_name.$column: $@\n";
        }
    }
}

sub update_meta_statistics {
    my ($dbh) = @_;
    
    print "Calculating new statistics for kafsss_meta table...\n" if $verbose;
    
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
    
    print "Total sequences after deduplication: $nseq\n" if $verbose;
    print "Total bases after deduplication: $nchar\n" if $verbose;
    
    # Calculate subset-specific statistics with single query
    print "Calculating subset-specific statistics...\n" if $verbose;
    
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
        
        print "  Subset '$subset': $subset_nseq sequences, $subset_nchar bases\n" if $verbose;
    }
    $sth->finish();
    
    # Prepare subset statistics JSON
    my $subset_json = encode_json(\%subset_stats);
    
    # Update kafsss_meta table
    print "Updating kafsss_meta table with statistics...\n" if $verbose;
    
    $sth = $dbh->prepare(<<SQL);
UPDATE kafsss_meta 
SET nseq = ?, nchar = ?, subset = ?
SQL
    
    eval {
        $dbh->begin_work;
        $sth->execute($nseq, $nchar, $subset_json);
        $dbh->commit;
        print "kafsss_meta table updated successfully.\n" if $verbose;
    };
    
    if ($@) {
        print STDERR "Error updating kafsss_meta statistics: $@\n";
        eval { $dbh->rollback; };
        $sth->finish();
        die "Failed to update kafsss_meta table: $@\n";
    }
    
    $sth->finish();
    
    print "Statistics update completed.\n" if $verbose;
}