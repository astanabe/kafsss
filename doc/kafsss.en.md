# kafsss: K-mer based Alignment-Free Splitted Sequence Search

A comprehensive toolkit for storing, managing, and searching DNA sequences using PostgreSQL with the pg_kmersearch extension.

## Overview

The kafsss suite provides a complete solution for DNA sequence analysis using k-mer similarity search. The toolkit consists of Perl scripts that handle different aspects of DNA sequence management, search operations, and server deployment with asynchronous job processing.

## Prerequisites

- PostgreSQL 9.1 or later
- pg_kmersearch extension installed and available
- Perl 5.10 or later
- PostgreSQL user with appropriate permissions (see Setup section)

### Required Perl Modules

#### Core Database Tools (kafssstore, kafssindex, kafsssearch, kafsssubset, kafssdbinfo, kafssdedup, kafssfreq)
- `DBI` - Database access interface
- `DBD::Pg` - PostgreSQL driver
- `Getopt::Long` - Command line argument parsing
- `POSIX` - POSIX system functions
- `File::Basename` - File name manipulation
- `Sys::Hostname` - System hostname retrieval

#### Network Client (kafsssearchclient)
Core modules (above) plus:
- `JSON` - JSON format processing
- `LWP::UserAgent` - HTTP client
- `HTTP::Request::Common` - HTTP request generation
- `URI` - URI parsing and encoding
- `MIME::Base64` - Base64 encoding/decoding
- `Time::HiRes` - High-resolution time functions
- `Fcntl` - File control operations

#### Standalone HTTP Server (kafsssearchserver.pl)
Core modules plus:
- `JSON` - JSON format processing
- `HTTP::Server::Simple::CGI` - Standalone web server
- `MIME::Base64` - Base64 encoding/decoding
- `Time::HiRes` - High-resolution time functions
- `Fcntl` - File control operations
- `DBD::SQLite` - SQLite driver (for job management)
- `Crypt::OpenSSL::Random` - Cryptographically secure random numbers

#### FastCGI Server (kafsssearchserver.fcgi)
Core modules plus:
- `JSON` - JSON format processing
- `CGI::Fast` - FastCGI implementation
- `FCGI::ProcManager` - FastCGI process management
- `MIME::Base64` - Base64 encoding/decoding
- `Time::HiRes` - High-resolution time functions
- `Fcntl` - File control operations
- `DBD::SQLite` - SQLite driver (for job management)
- `Crypt::OpenSSL::Random` - Cryptographically secure random numbers

#### PSGI Server (kafsssearchserver.psgi)
Core modules plus:
- `JSON` - JSON format processing
- `Plack::Request` - PSGI request handling
- `Plack::Response` - PSGI response handling
- `Plack::Builder` - PSGI middleware composition
- `Plack::Handler::Starman` - Starman HTTP server
- `MIME::Base64` - Base64 encoding/decoding
- `Time::HiRes` - High-resolution time functions
- `Fcntl` - File control operations
- `DBD::SQLite` - SQLite driver (for job management)
- `Crypt::OpenSSL::Random` - Cryptographically secure random numbers

### Database Setup

Before using kafsss tools, you must set up PostgreSQL properly:

#### 1. Install PostgreSQL and pg_kmersearch extension
```bash
sudo apt-get install postgresql postgresql-contrib
# Install pg_kmersearch extension package (contact your system administrator)
```

#### 2. Create PostgreSQL User and Database
```bash
sudo -u postgres psql
CREATE USER yourusername CREATEDB;
ALTER USER yourusername PASSWORD 'yourpassword';
\q
```

#### 3. Set Environment Variables
```bash
export PGUSER=yourusername
export PGPASSWORD=yourpassword
export PGHOST=localhost
export PGPORT=5432
```

#### 4. Create Extension in Database
**Option A: Have superuser create extension (recommended)**
```bash
sudo -u postgres psql -d your_database
CREATE EXTENSION IF NOT EXISTS pg_kmersearch;
\q
```

**Option B: Grant temporary superuser permission**
```bash
sudo -u postgres psql
ALTER USER yourusername SUPERUSER;
\q
# Run kafssstore, then revoke:
# ALTER USER yourusername NOSUPERUSER;
```

### Dependency Installation

#### For Core Database Tools Only
**Ubuntu/Debian:**
```bash
sudo apt-get update
sudo apt-get install -y perl libdbi-perl libdbd-pg-perl

# Using cpanminus
sudo apt-get install -y cpanminus
sudo cpanm DBI DBD::Pg Getopt::Long POSIX File::Basename Sys::Hostname
```

**RHEL/CentOS/Fedora:**
```bash
sudo yum install -y perl perl-DBI perl-DBD-Pg
# or: sudo dnf install -y perl perl-DBI perl-DBD-Pg

# Using cpanminus
sudo yum install -y perl-App-cpanminus  # or dnf
sudo cpanm DBI DBD::Pg Getopt::Long POSIX File::Basename Sys::Hostname
```

#### For Network Client (kafsssearchclient)
**Ubuntu/Debian:**
```bash
sudo apt-get install -y \
    perl libdbi-perl libdbd-pg-perl libjson-perl \
    libwww-perl liburi-perl libdbd-sqlite3-perl \
    libcrypt-openssl-random-perl

# Using cpanminus
sudo cpanm DBI DBD::Pg JSON LWP::UserAgent HTTP::Request::Common URI \
           MIME::Base64 Time::HiRes Fcntl DBD::SQLite Crypt::OpenSSL::Random
```

**RHEL/CentOS/Fedora:**
```bash
sudo yum install -y perl perl-DBI perl-DBD-Pg perl-JSON \
                    perl-libwww-perl perl-URI perl-DBD-SQLite
# or use dnf

# Using cpanminus
sudo cpanm DBI DBD::Pg JSON LWP::UserAgent HTTP::Request::Common URI \
           MIME::Base64 Time::HiRes Fcntl DBD::SQLite Crypt::OpenSSL::Random
```

#### For Standalone HTTP Server (kafsssearchserver.pl)
**Ubuntu/Debian:**
```bash
sudo apt-get install -y \
    perl libdbi-perl libdbd-pg-perl libjson-perl \
    libhttp-server-simple-perl libdbd-sqlite3-perl \
    libcrypt-openssl-random-perl

# Using cpanminus
sudo cpanm DBI DBD::Pg JSON HTTP::Server::Simple::CGI \
           MIME::Base64 Time::HiRes Fcntl DBD::SQLite Crypt::OpenSSL::Random
```

**RHEL/CentOS/Fedora:**
```bash
sudo yum install -y perl perl-DBI perl-DBD-Pg perl-JSON \
                    perl-HTTP-Server-Simple perl-DBD-SQLite
# or use dnf

# Using cpanminus
sudo cpanm DBI DBD::Pg JSON HTTP::Server::Simple::CGI \
           MIME::Base64 Time::HiRes Fcntl DBD::SQLite Crypt::OpenSSL::Random
```

#### For FastCGI Server (kafsssearchserver.fcgi)
**Ubuntu/Debian:**
```bash
sudo apt-get install -y \
    perl libdbi-perl libdbd-pg-perl libjson-perl \
    libcgi-fast-perl libfcgi-procmanager-perl \
    libdbd-sqlite3-perl libcrypt-openssl-random-perl

# Using cpanminus
sudo cpanm DBI DBD::Pg JSON CGI::Fast FCGI::ProcManager \
           MIME::Base64 Time::HiRes Fcntl DBD::SQLite Crypt::OpenSSL::Random
```

**RHEL/CentOS/Fedora:**
```bash
sudo yum install -y perl perl-DBI perl-DBD-Pg perl-JSON \
                    perl-CGI-Fast perl-FCGI-ProcManager perl-DBD-SQLite
# or use dnf

# Using cpanminus
sudo cpanm DBI DBD::Pg JSON CGI::Fast FCGI::ProcManager \
           MIME::Base64 Time::HiRes Fcntl DBD::SQLite Crypt::OpenSSL::Random
```

#### For PSGI Server (kafsssearchserver.psgi)
**Ubuntu/Debian:**
```bash
sudo apt-get install -y \
    perl libdbi-perl libdbd-pg-perl libjson-perl \
    libplack-perl starman libdbd-sqlite3-perl \
    libcrypt-openssl-random-perl

# Using cpanminus
sudo cpanm DBI DBD::Pg JSON Plack::Request Plack::Response Plack::Builder \
           Plack::Handler::Starman MIME::Base64 Time::HiRes Fcntl \
           DBD::SQLite Crypt::OpenSSL::Random
```

**RHEL/CentOS/Fedora:**
```bash
sudo yum install -y perl perl-DBI perl-DBD-Pg perl-JSON \
                    perl-Plack perl-DBD-SQLite
# or use dnf

# Using cpanminus
sudo cpanm DBI DBD::Pg JSON Plack::Request Plack::Response Plack::Builder \
           Plack::Handler::Starman MIME::Base64 Time::HiRes Fcntl \
           DBD::SQLite Crypt::OpenSSL::Random
```

#### Manual Installation (using CPAN)

**For Core Database Tools:**
```bash
perl -MCPAN -e 'install DBI, DBD::Pg, Getopt::Long, POSIX, File::Basename, Sys::Hostname'
```

**For Network Client (kafsssearchclient):**
```bash
perl -MCPAN -e 'install DBI, DBD::Pg, JSON, LWP::UserAgent, HTTP::Request::Common, URI, MIME::Base64, Time::HiRes, Fcntl, DBD::SQLite, Crypt::OpenSSL::Random'
```

**For Standalone HTTP Server (kafsssearchserver.pl):**
```bash
perl -MCPAN -e 'install DBI, DBD::Pg, JSON, HTTP::Server::Simple::CGI, MIME::Base64, Time::HiRes, Fcntl, DBD::SQLite, Crypt::OpenSSL::Random'
```

**For FastCGI Server (kafsssearchserver.fcgi):**
```bash
perl -MCPAN -e 'install DBI, DBD::Pg, JSON, CGI::Fast, FCGI::ProcManager, MIME::Base64, Time::HiRes, Fcntl, DBD::SQLite, Crypt::OpenSSL::Random'
```

**For PSGI Server (kafsssearchserver.psgi):**
```bash
perl -MCPAN -e 'install DBI, DBD::Pg, JSON, Plack::Request, Plack::Response, Plack::Builder, Plack::Handler::Starman, MIME::Base64, Time::HiRes, Fcntl, DBD::SQLite, Crypt::OpenSSL::Random'
```

### Dependency Verification

**Using automatic checker script (recommended):**

```bash
# Run the dependency checker script
perl check_dependencies.pl
```

This script will verify the presence of all required modules and provide specific installation instructions if any are missing.

**Manual verification:**

After installation, you can also manually verify dependencies with these commands:

```bash
# Verify core modules
perl -MDBI -e 'print "DBI version: $DBI::VERSION\n"'
perl -MDBD::Pg -e 'print "DBD::Pg version: $DBD::Pg::VERSION\n"'
perl -MJSON -e 'print "JSON version: $JSON::VERSION\n"'

# Verify network modules
perl -MLWP::UserAgent -e 'print "LWP::UserAgent available\n"'
perl -MURI -e 'print "URI available\n"'

# Verify server modules
perl -MHTTP::Server::Simple -e 'print "HTTP::Server::Simple available\n"'
perl -MPlack -e 'print "Plack available\n"'
perl -MStarman -e 'print "Starman available\n"'
```

## Scripts Overview

| Script | Purpose |
|--------|---------|
| `kafssstore` | Store FASTA sequences into PostgreSQL database |
| `kafsssubset` | Add/remove subset information for sequences |
| `kafssindex` | Create/drop GIN indexes on sequence data |
| `kafsssearch` | Search sequences using k-mer similarity |
| `kafssdbinfo` | Display database metadata information |
| `kafssdedup` | Deduplicate sequences in database |
| `kafsspart` | Partition kafsss_data table for improved performance |
| `kafssfreq` | K-mer frequency analysis |
| `kafsssearchclient` | Remote k-mer search client with load balancing |
| `kafsssearchserver.pl` | REST API server for k-mer search (standalone) with asynchronous job processing |
| `kafsssearchserver.fcgi` | FastCGI version for production web servers |
| `kafsssearchserver.psgi` | PSGI version for modern web deployment |
| `calcsegment` | Mathematical utility for sequence segmentation parameter calculation |

## Installation

### Using make (Recommended)

1. Install pg_kmersearch extension in PostgreSQL
2. Install command-line tools:
   ```bash
   make
   sudo make install
   
   # Custom installation prefix
   make PREFIX=/opt/kafsss
   sudo make install PREFIX=/opt/kafsss
   ```

**Note**: Server scripts (`kafsssearchserver.pl`, `.fcgi`, `.psgi`) are not installed by make and should be manually deployed to appropriate web server locations.

### Manual Installation

1. Install pg_kmersearch extension in PostgreSQL
2. Make scripts executable:
   ```bash
   chmod +x kafss*.pl kafsssearch*.pl
   ```

## Database Connection

All scripts support PostgreSQL connection options:

- `--host=HOST` - PostgreSQL server host (default: $PGHOST or localhost)
- `--port=PORT` - PostgreSQL server port (default: $PGPORT or 5432)
- `--username=USER` - PostgreSQL username (default: $PGUSER or current user)

Password is read from the `PGPASSWORD` environment variable.

## Script Documentation

### kafssstore

Store multi-FASTA DNA sequences into PostgreSQL database.

#### Usage
```bash
kafssstore [options] input_file output_database
```

#### Options
- `--datatype=DNA2|DNA4` - Data type (default: DNA4)
- `--minlen=INT` - Minimum sequence length for splitting (default: 50000)
- `--ovllen=INT` - Overlap length between split sequences (default: 500). Must be less than half of `--minsplitlen` to prevent overlap conflicts
- `--numthreads=INT` - Number of parallel threads (default: 1)
- `--subset=NAME` - Subset name (multiple values allowed)
- `--tablespace=NAME` - Tablespace name for CREATE DATABASE
- `--overwrite` - Overwrite existing database

#### Input File
- Multi-FASTA format DNA sequences
- Use `-`, `stdin`, or `STDIN` for standard input

#### Examples
```bash
# Basic usage
kafssstore sequences.fasta mydb

# With subsets and parallel processing
kafssstore --subset=bacteria --numthreads=4 sequences.fasta mydb

# From standard input
cat sequences.fasta | kafssstore stdin mydb

# Custom parameters
kafssstore --datatype=DNA2 --minlen=100000 sequences.fasta mydb
```

### kafssdedup

**Purpose**: Remove duplicate sequences from kafsss_data table.

**Usage**: `kafssdedup [options] database_name`

**Options**:
- `--host=HOST` - PostgreSQL server host
- `--port=PORT` - PostgreSQL server port
- `--username=USER` - PostgreSQL username
- `--workingmemory=SIZE` - Working memory for deduplication (default: 8GB)
- `--maintenanceworkingmemory=SIZE` - Maintenance working memory (default: 8GB)
- `--temporarybuffer=SIZE` - Temporary buffer size (default: 512MB)
- `--verbose` - Show detailed processing messages
- `--help` - Show help message

**Example**:
```bash
# Basic deduplication
kafssdedup mydb

# With custom memory settings
kafssdedup --workingmemory=32GB mydb
```

### kafsspart

**Purpose**: Partition kafsss_data table using pg_kmersearch's partition function for improved performance.

**Usage**: `kafsspart [options] database_name`

**Required Options**:
- `--npart=INT` - Number of partitions (must be 2 or greater)

**Optional Arguments**:
- `--host=HOST` - Database server host
- `--port=PORT` - Database server port
- `--username=USER` - Database user name
- `--tablespace=NAME` - Tablespace name for partitions
- `--verbose` - Enable verbose output
- `--help` - Show help message

**Example**:
```bash
# Partition into 16 partitions
kafsspart --npart=16 mydb

# Partition with specific tablespace
kafsspart --npart=32 --tablespace=fast_ssd mydb
```

### kafssfreq

**Purpose**: Perform high-frequency k-mer analysis on kafsss_data table.

**Usage**: `kafssfreq [options] database_name`

**Required Options**:
- `--mode=MODE` - Operation mode: 'create' or 'drop'

**Optional Arguments**:
- `--host=HOST` - PostgreSQL server host
- `--port=PORT` - PostgreSQL server port
- `--username=USER` - PostgreSQL username
- `--kmersize=INT` - K-mer length for analysis (default: 8, range: 4-64)
- `--maxpappear=REAL` - Max k-mer appearance rate (default: 0.5, range: 0.0-1.0)
- `--maxnappear=INT` - Max rows containing k-mer (default: 0=unlimited)
- `--occurbitlen=INT` - Bits for occurrence count (default: 8, range: 0-16)
- `--numthreads=INT` - Number of parallel workers (default: 0=auto)
- `--workingmemory=SIZE` - Work memory for each operation (default: 8GB)
- `--maintenanceworkingmemory=SIZE` - Maintenance work memory (default: 8GB)
- `--temporarybuffer=SIZE` - Temporary buffer size (default: 512MB)
- `--verbose` - Show detailed processing messages
- `--overwrite` - Overwrite existing analysis (only for --mode=create)
- `--help` - Show help message

**Example**:
```bash
# Create frequency analysis
kafssfreq --mode=create mydb

# With custom parameters
kafssfreq --mode=create --kmersize=16 --numthreads=32 mydb

# Drop frequency analysis
kafssfreq --mode=drop mydb
```

### kafsssubset

Add or remove subset information for sequences based on accession numbers or apply operations to all rows.

#### Usage
```bash
kafsssubset [options] input_file database_name
```

#### Options
- `--mode=MODE` - Operation mode: `add` (default) or `del`
- `--subset=NAME` - Subset name to add/remove (required, multiple values allowed)
  - Use `all` to target all subsets (only in del mode)
- `--numthreads=INT` - Number of parallel threads (default: 1)

#### Input File
- Plain text file with one accession number per line
- Use `-`, `stdin`, or `STDIN` for standard input
- Use `all` to target all rows in the database
- Lines starting with `#` are treated as comments

#### Examples
```bash
# Add subsets to sequences
kafsssubset --subset=bacteria accessions.txt mydb
kafsssubset --subset=bacteria,archaea accessions.txt mydb

# Remove subsets from sequences
kafsssubset --mode=del --subset=bacteria accessions.txt mydb

# Remove all subsets from all rows
kafsssubset --mode=del --subset=all all mydb

# Remove specific subset from all rows
kafsssubset --mode=del --subset=archaea all mydb

# From standard input
echo -e "AB123456\nCD789012" | kafsssubset --subset=bacteria stdin mydb
```

#### Notes
- Subset name `all` is prohibited in add mode
- When input file is `all`, operations target all rows in the database
- When `--subset=all` is used in del mode, all subset information is removed

### kafssindex

Create or drop GIN indexes on sequence data.

#### Usage
```bash
kafssindex [options] database_name
```

#### Options
- `--mode=create|drop` - Operation mode (required)
- `--tablespace=NAME` - Tablespace name for CREATE INDEX

#### Examples
```bash
# Create indexes
kafssindex --mode=create mydb

# Create indexes on specific tablespace
kafssindex --mode=create --tablespace=fast_ssd mydb

# Drop indexes
kafssindex --mode=drop mydb
```

### kafsspreload

**Purpose**: Preload high-frequency k-mer cache into memory for acceleration.

**Usage**: `kafsspreload [options] database_name`

**Options**:
- `--host=HOST` - PostgreSQL server host
- `--port=PORT` - PostgreSQL server port
- `--username=USER` - PostgreSQL username
- `--verbose` - Enable verbose output
- `--help` - Show help message

**Notes**:
- Runs as a daemon process that maintains database connection
- Monitors for changes hourly and exits gracefully when changes detected
- While running, accelerates kafssindex builds and kafsssearch operations
- Requires pg_kmersearch extension and kafssfreq to be run first

**Example**:
```bash
# Preload cache (runs as daemon)
kafsspreload mydb

# With verbose logging
kafsspreload --verbose mydb
```

### kafsssearch

Search DNA sequences using k-mer similarity.

#### Usage
```bash
kafsssearch [options] input_file output_file
```

#### Options
- `--db=DATABASE` - PostgreSQL database name (required)
- `--subset=NAME` - Limit search to specific subset
- `--maxnseq=INT` - Maximum number of results per query (default: 1000, 0=unlimited)
- `--minscore=INT` - Minimum score threshold (default: 1)
- `--minpsharedkmer=REAL` - Minimum percentage of shared k-mers (0.0-1.0, default: 0.5)
- `--mode=MODE` - Output mode: minimum (min), matchscore (score), sequence (seq), maximum (max) (default: matchscore)
- `--numthreads=INT` - Number of parallel threads (default: 1)

#### Input/Output Files
- Input: Multi-FASTA format, use `-`, `stdin`, or `STDIN` for standard input
- Output: TSV format, use `-`, `stdout`, or `STDOUT` for standard output

#### Output Format
Tab-separated values with columns (varies by mode):
1. Query sequence number (1-based)
2. Query FASTA label
3. Comma-separated seqid list from seqid column
4. Match score from kmersearch_matchscore function (only in matchscore and maximum modes)
5. Sequence data (only in sequence and maximum modes)

#### Examples
```bash
# Basic search
kafsssearch --db=mydb query.fasta results.tsv

# Search with subset filter
kafsssearch --db=mydb --subset=bacteria query.fasta results.tsv

# Parallel search with custom parameters
kafsssearch --db=mydb --numthreads=4 --maxnseq=500 query.fasta results.tsv

# Pipeline usage
cat query.fasta | kafsssearch --db=mydb stdin stdout > results.tsv
```

### kafssdbinfo

Display metadata information from kafsssearch database.

#### Usage
```bash
kafssdbinfo [options] database_name
```

#### Options
- `--host=HOST` - PostgreSQL server host (default: $PGHOST or localhost)
- `--port=PORT` - PostgreSQL server port (default: $PGPORT or 5432)
- `--username=USER` - PostgreSQL username (default: $PGUSER or current user)
- `--help, -h` - Show help message

#### Output
- All output is written to STDERR
- Displays version, min length, overlap length
- Shows total sequences and characters
- Lists subset information with sequence and character counts

#### Examples
```bash
# Basic usage
kafssdbinfo mydb

# Remote database
kafssdbinfo --host=remote-server mydb

# Custom connection parameters
kafssdbinfo --host=localhost --port=5433 --username=postgres mydb
```

### kafsssearchclient

Remote k-mer search client with asynchronous job processing, load balancing, and retry logic.

#### Usage
```bash
# New job submission
kafsssearchclient [options] input_file output_file

# Resume existing job
kafsssearchclient --resume=JOB_ID

# Cancel existing job
kafsssearchclient --cancel=JOB_ID

# List active jobs
kafsssearchclient --jobs
```

#### Options
- `--server=SERVERS` - Server URL(s) - single server or comma-separated list
- `--serverlist=FILE` - File containing server URLs (one per line)
- `--db=DATABASE` - PostgreSQL database name (optional if server has default)
- `--subset=NAME` - Limit search to specific subset (optional)
- `--maxnseq=INT` - Maximum number of results per query (default: 1000, 0=unlimited)
- `--minscore=INT` - Minimum score threshold (default: 1)
- `--minpsharedkmer=REAL` - Minimum percentage of shared k-mers (0.0-1.0, default: 0.5)
- `--mode=MODE` - Output mode: minimum (min), matchscore (score), sequence (seq), maximum (max) (default: matchscore)
- `--numthreads=INT` - Number of parallel threads (default: 1)
- `--maxnretry=INT` - Maximum retries per status check (default: 0 = unlimited)
- `--maxnretry_total=INT` - Maximum total retries for all operations (default: 100)
- `--retrydelay=INT` - Retry delay in seconds (default: 10)
- `--failedserverexclusion=INT` - Exclude failed servers for N seconds (default: infinite)
- `--netrc-file=FILE` - Read authentication credentials from .netrc format file
- `--http-user=USER` - HTTP Basic authentication username (requires --http-password)
- `--http-password=PASS` - HTTP Basic authentication password (requires --http-user)
- `--resume=JOB_ID` - Resume a previously submitted job
- `--cancel=JOB_ID` - Cancel a job and remove all associated data
- `--jobs` - List all active jobs
- `--help, -h` - Show help message

#### Input/Output Files
- Input: Multi-FASTA format, use `-`, `stdin`, or `STDIN` for standard input
- Output: TSV format, use `-`, `stdout`, or `STDOUT` for standard output

#### Output Format
Tab-separated values with 4 columns:
1. Query sequence number (1-based)
2. Query FASTA label
3. CORRECTEDSCORE from server
4. Comma-separated seqid list

#### Authentication
For servers protected by HTTP Basic authentication, use one of these options:

**1. .netrc file (recommended for multiple servers):**
```bash
kafsssearchclient --netrc-file=/path/to/netrc --server=https://server.com --db=mydb query.fasta results.tsv
```

.netrc format:
```
machine hostname.example.com
login myusername
password mypassword

machine server2.example.com
login otherusername
password otherpassword
```

**2. Command line credentials (for all servers):**
```bash
kafsssearchclient --http-user=myusername --http-password=mypassword --server=https://server.com --db=mydb query.fasta results.tsv
```

**3. Both options (fallback behavior):**
Specific hostnames in .netrc are used first, command line credentials are used as fallback for servers not found in .netrc file.

#### Server URL Formats
- `hostname` → `http://hostname:8080/search`
- `hostname:9090` → `http://hostname:9090/search`
- `192.168.1.100` → `http://192.168.1.100:8080/search`
- `http://server/api/search` → Use as-is
- `https://server/search` → Use as-is

#### Asynchronous Job Processing

The client now supports asynchronous job processing with automatic polling:

- **Job Persistence**: Jobs are saved to `.kafsssearchclient` file for resume capability
- **Automatic Polling**: Uses adaptive intervals (5s → 10s → 20s → 30s → 60s)
- **Resume Support**: Can resume interrupted jobs using `--resume=JOB_ID`
- **Cancel Support**: Can cancel running jobs using `--cancel=JOB_ID`
- **Job Management**: List active jobs with `--jobs`

#### Examples
```bash
# Basic usage with asynchronous processing
kafsssearchclient --server=localhost --db=mydb query.fasta results.tsv

# Multiple servers with load balancing
kafsssearchclient --server="server1,server2,server3" --db=mydb query.fasta results.tsv

# Server list file
kafsssearchclient --serverlist=servers.txt --db=mydb query.fasta results.tsv

# With authentication (.netrc file)
kafsssearchclient --server=https://server.com --db=mydb --netrc-file=.netrc query.fasta results.tsv

# With authentication (command line)
kafsssearchclient --server=https://server.com --db=mydb --http-user=myuser --http-password=mypass query.fasta results.tsv

# Parallel processing with retries
kafsssearchclient --server=localhost --db=mydb --numthreads=4 --maxnretry=10 query.fasta results.tsv

# Pipeline usage
cat query.fasta | kafsssearchclient --server=localhost --db=mydb stdin stdout > results.tsv

# Job management examples
kafsssearchclient --jobs                                    # List active jobs
kafsssearchclient --resume=20250703T120000-AbCdEf123456     # Resume job
kafsssearchclient --cancel=20250703T120000-AbCdEf123456     # Cancel job
```

### kafsssearchserver.pl

REST API server for k-mer search (standalone HTTP server).

#### Usage
```bash
perl kafsssearchserver.pl [options]
```

#### Options
- `--listen-port=PORT` - HTTP server listen port (default: 8080)
- `--numthreads=INT` - Number of parallel request processing threads (default: 5)

#### Configuration
Edit default values in the script header:
```perl
my $default_database = 'mykmersearch';  # Default database name
my $default_subset = 'bacteria';     # Default subset name
my $default_maxnseq = 1000;             # Default max results (0=unlimited)
my $default_minscore = 1;               # Default min score
my $default_minpsharedkmer = 0.5;       # Default minimum shared k-mer rate
my $default_numthreads = 5;             # Number of parallel threads
```

#### API Endpoints

**POST /search** - Submit asynchronous k-mer sequence search job

Request JSON:
```json
{
  "querylabel": "sequence_name",
  "queryseq": "ATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCG",
  "db": "database_name",
  "subset": "subset_name",
  "maxnseq": 1000,
  "minscore": 10
}
```

Response JSON (job submitted):
```json
{
  "success": true,
  "job_id": "20250703T120000-AbCdEfGhIjKlMnOpQrStUvWxYz012345",
  "status": "running",
  "message": "Job submitted successfully"
}
```

**POST /result** - Get job result (one-time retrieval, removes job after access)

Request JSON:
```json
{
  "job_id": "20250703T120000-AbCdEfGhIjKlMnOpQrStUvWxYz012345"
}
```

Response JSON (completed):
```json
{
  "job_id": "20250703T120000-AbCdEfGhIjKlMnOpQrStUvWxYz012345",
  "status": "completed",
  "querylabel": "sequence_name",
  "queryseq": "ATCGATCG...",
  "results": [
    {
      "correctedscore": 95,
      "seqid": ["AB123:1:100", "CD456:50:150"]
    }
  ]
}
```

**POST /status** - Check job status (non-destructive, for monitoring)

Request JSON:
```json
{
  "job_id": "20250703T120000-AbCdEfGhIjKlMnOpQrStUvWxYz012345"
}
```

Response JSON (still running):
```json
{
  "job_id": "20250703T120000-AbCdEfGhIjKlMnOpQrStUvWxYz012345",
  "status": "running",
  "message": "Job is still processing"
}
```

**POST /cancel** - Cancel running job and remove all associated data

Request JSON:
```json
{
  "job_id": "20250703T120000-AbCdEfGhIjKlMnOpQrStUvWxYz012345"
}
```

Response JSON:
```json
{
  "status": "cancelled",
  "message": "Job has been cancelled and removed"
}
```

**GET /metadata** - Get server configuration and available databases

Response JSON:
```json
{
  "success": true,
  "default_database": "mykmersearch",
  "default_subset": "bacteria",
  "default_maxnseq": 1000,
  "default_minscore": "10",
  "server_version": "1.0",
  "supported_endpoints": ["/search", "/result", "/status", "/cancel", "/metadata"]
}
```

#### Examples
```bash
# Start server
perl kafsssearchserver.pl --listen-port=8080

# Start server with custom thread count
perl kafsssearchserver.pl --listen-port=8080 --numthreads=10

# API call
curl -X POST http://localhost:8080/search \
  -H "Content-Type: application/json" \
  -d '{
    "querylabel": "test_sequence",
    "queryseq": "ATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCG",
    "db": "mydb"
  }'

# Get server metadata
curl http://localhost:8080/metadata
```

### kafsssearchserver.fcgi

FastCGI version for production web servers (NGINX/Apache).

#### Usage
```bash
perl kafsssearchserver.fcgi [options]
```

#### Options
- `--numthreads=NUM` - Number of FastCGI processes (default: 5)

#### Configuration
Same as kafsssearchserver.pl - edit default values in script header.

#### NGINX Setup
```nginx
server {
    listen 80;
    server_name your-domain.com;
    
    location /api/search {
        include fastcgi_params;
        fastcgi_pass unix:/var/run/kafsssearch.sock;
        fastcgi_param SCRIPT_FILENAME $document_root$fastcgi_script_name;
    }
}
```

#### Apache Setup
```apache
<VirtualHost *:80>
    ServerName your-domain.com
    
    ScriptAlias /api/search /path/to/kafsssearchserver.fcgi
    
    <Directory "/path/to/">
        SetHandler fcgid-script
        Options +ExecCGI
        Require all granted
    </Directory>
</VirtualHost>
```

#### Process Management
```bash
# Start FastCGI processes
spawn-fcgi -s /var/run/kafsssearch.sock -U nginx -G nginx \
           -u www-data -g www-data -P /var/run/kafsssearch.pid \
           -- perl kafsssearchserver.fcgi --numthreads=5
```

### kafsssearchserver.psgi

PSGI version for modern web deployment with various PSGI servers.

#### Usage
```bash
perl kafsssearchserver.psgi [options]
```

#### Options
- `--host=HOST` - PostgreSQL server host (default: $PGHOST or localhost)
- `--port=PORT` - PostgreSQL server port (default: $PGPORT or 5432)
- `--username=USER` - PostgreSQL username (default: $PGUSER or current user)
- `--listen-port=PORT` - HTTP server listen port (default: 5000)
- `--workers=NUM` - Number of worker processes (default: 5)
- `--help, -h` - Show help message

#### Configuration
Same as kafsssearchserver.pl - edit default values in script header.

#### Deployment Options
```bash
# Standalone (built-in Starman server)
perl kafsssearchserver.psgi

# With plackup
plackup -p 5000 --workers 10 kafsssearchserver.psgi

# With other PSGI servers
starman --port 5000 --workers 10 kafsssearchserver.psgi
uwsgi --http :5000 --psgi kafsssearchserver.psgi
```

#### Examples
```bash
perl kafsssearchserver.psgi
perl kafsssearchserver.psgi --listen-port=8080 --workers=10
plackup -p 8080 --workers 20 kafsssearchserver.psgi
```

## Workflow Examples

### Complete Database Setup and Search

1. **Create database and store sequences:**
   ```bash
   kafssstore --subset=bacteria sequences.fasta mydb
   ```

2. **Deduplicate sequences (verify partition table compatibility):**
   ```bash
   kafssdedup mydb
   ```

3. **Partition the table for improved performance:**
   ```bash
   kafsspart --npart=16 mydb
   ```

4. **Perform k-mer frequency analysis:**
   ```bash
   kafssfreq mydb
   ```

5. **Create indexes:**
   ```bash
   kafssindex --mode=create mydb
   ```

6. **Add subset information:**
   ```bash
   kafsssubset --subset=pathogenic bacteria_ids.txt mydb
   ```

7. **Check database information:**
   ```bash
   kafssdbinfo mydb
   ```

8. **Search sequences:**
   ```bash
   kafsssearch --db=mydb --subset=pathogenic query.fasta results.tsv
   ```

### Web API Deployment

1. **Configure defaults:**
   ```perl
   # Edit kafsssearchserver.fcgi
   my $default_database = 'mydb';
   my $default_subset = 'bacteria';
   ```

2. **Deploy with NGINX:**
   ```bash
   spawn-fcgi -s /var/run/kafsssearch.sock \
              -- perl kafsssearchserver.fcgi --numthreads=5
   ```

3. **Search via API:**
   ```bash
   curl -X POST http://your-domain.com/api/search \
        -H "Content-Type: application/json" \
        -d '{"querylabel": "test", "queryseq": "ATCG..."}'
   ```

## Performance Tips

- Use appropriate `--numthreads` based on CPU cores
- Create indexes after bulk data loading
- Use subsets for large datasets
- Place indexes on fast storage (SSD) using `--tablespace`
- For web APIs, configure appropriate FastCGI process counts

## Troubleshooting

- Ensure pg_kmersearch extension is installed
- Check PostgreSQL connection parameters
- Verify sequence length (minimum 64 bases for search)
- Check file permissions for FastCGI deployment
- Monitor PostgreSQL logs for connection issues

## License

Open source software. See individual script headers for details.