# af_kmersearch suite

A comprehensive toolkit for storing, managing, and searching DNA sequences using PostgreSQL with the pg_kmersearch extension.

## Overview

The af_kmersearch suite provides a complete solution for DNA sequence analysis using k-mer similarity search. The toolkit consists of 10 Perl scripts that handle different aspects of DNA sequence management, search operations, and server deployment with asynchronous job processing.

## Prerequisites

- PostgreSQL 9.1 or later
- pg_kmersearch extension installed
- Perl 5.10 or later

### Required Perl Modules

#### Core Database Tools (af_kmerstore, af_kmerindex, af_kmersearch, af_kmerpart, af_kmerdbinfo)
- `DBI` - Database access interface
- `DBD::Pg` - PostgreSQL driver
- `Getopt::Long` - Command line argument parsing
- `POSIX` - POSIX system functions
- `File::Basename` - File name manipulation
- `Sys::Hostname` - System hostname retrieval

#### Network Client (af_kmersearchclient)
Core modules (above) plus:
- `JSON` - JSON format processing
- `LWP::UserAgent` - HTTP client
- `HTTP::Request::Common` - HTTP request generation
- `URI` - URI parsing and encoding
- `MIME::Base64` - Base64 encoding/decoding
- `Time::HiRes` - High-resolution time functions
- `Fcntl` - File control operations

#### Standalone HTTP Server (af_kmersearchserver.pl)
Core modules plus:
- `JSON` - JSON format processing
- `HTTP::Server::Simple::CGI` - Standalone web server
- `MIME::Base64` - Base64 encoding/decoding
- `Time::HiRes` - High-resolution time functions
- `Fcntl` - File control operations
- `DBD::SQLite` - SQLite driver (for job management)
- `Crypt::OpenSSL::Random` - Cryptographically secure random numbers

#### FastCGI Server (af_kmersearchserver.fcgi)
Core modules plus:
- `JSON` - JSON format processing
- `CGI::Fast` - FastCGI implementation
- `FCGI::ProcManager` - FastCGI process management
- `MIME::Base64` - Base64 encoding/decoding
- `Time::HiRes` - High-resolution time functions
- `Fcntl` - File control operations
- `DBD::SQLite` - SQLite driver (for job management)
- `Crypt::OpenSSL::Random` - Cryptographically secure random numbers

#### PSGI Server (af_kmersearchserver.psgi)
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

#### For Network Client (af_kmersearchclient)
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

#### For Standalone HTTP Server (af_kmersearchserver.pl)
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

#### For FastCGI Server (af_kmersearchserver.fcgi)
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

#### For PSGI Server (af_kmersearchserver.psgi)
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

**For Network Client (af_kmersearchclient):**
```bash
perl -MCPAN -e 'install DBI, DBD::Pg, JSON, LWP::UserAgent, HTTP::Request::Common, URI, MIME::Base64, Time::HiRes, Fcntl, DBD::SQLite, Crypt::OpenSSL::Random'
```

**For Standalone HTTP Server (af_kmersearchserver.pl):**
```bash
perl -MCPAN -e 'install DBI, DBD::Pg, JSON, HTTP::Server::Simple::CGI, MIME::Base64, Time::HiRes, Fcntl, DBD::SQLite, Crypt::OpenSSL::Random'
```

**For FastCGI Server (af_kmersearchserver.fcgi):**
```bash
perl -MCPAN -e 'install DBI, DBD::Pg, JSON, CGI::Fast, FCGI::ProcManager, MIME::Base64, Time::HiRes, Fcntl, DBD::SQLite, Crypt::OpenSSL::Random'
```

**For PSGI Server (af_kmersearchserver.psgi):**
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
| `af_kmerstore` | Store FASTA sequences into PostgreSQL database |
| `af_kmerpart` | Update partition information for sequences |
| `af_kmerindex` | Create/drop GIN indexes on sequence data |
| `af_kmersearch` | Search sequences using k-mer similarity |
| `af_kmerdbinfo` | Display database metadata information |
| `af_kmersearchclient` | Remote k-mer search client with load balancing |
| `af_kmersearchserver.pl` | REST API server for k-mer search (standalone) with asynchronous job processing |
| `af_kmersearchserver.fcgi` | FastCGI version for production web servers |
| `af_kmersearchserver.psgi` | PSGI version for modern web deployment |
| `calcsegment` | Mathematical utility for sequence segmentation parameter calculation |

## Installation

### Using make (Recommended)

1. Install pg_kmersearch extension in PostgreSQL
2. Install command-line tools:
   ```bash
   make
   sudo make install
   
   # Custom installation prefix
   make PREFIX=/opt/af_kmersearch
   sudo make install PREFIX=/opt/af_kmersearch
   ```

**Note**: Server scripts (`af_kmersearchserver.pl`, `.fcgi`, `.psgi`) are not installed by make and should be manually deployed to appropriate web server locations.

### Manual Installation

1. Install pg_kmersearch extension in PostgreSQL
2. Make scripts executable:
   ```bash
   chmod +x af_kmer*.pl
   ```

## Database Connection

All scripts support PostgreSQL connection options:

- `--host=HOST` - PostgreSQL server host (default: $PGHOST or localhost)
- `--port=PORT` - PostgreSQL server port (default: $PGPORT or 5432)
- `--username=USER` - PostgreSQL username (default: $PGUSER or current user)

Password is read from the `PGPASSWORD` environment variable.

## Script Documentation

### af_kmerstore

Store multi-FASTA DNA sequences into PostgreSQL database.

#### Usage
```bash
af_kmerstore [options] input_file output_database
```

#### Options
- `--datatype=DNA2|DNA4` - Data type (default: DNA4)
- `--minlen=INT` - Minimum sequence length for splitting (default: 50000)
- `--ovllen=INT` - Overlap length between split sequences (default: 500). Must be less than half of `--minsplitlen` to prevent overlap conflicts
- `--numthreads=INT` - Number of parallel threads (default: 1)
- `--partition=NAME` - Partition name (multiple values allowed)
- `--tablespace=NAME` - Tablespace name for CREATE DATABASE
- `--overwrite` - Overwrite existing database

#### Input File
- Multi-FASTA format DNA sequences
- Use `-`, `stdin`, or `STDIN` for standard input

#### Examples
```bash
# Basic usage
af_kmerstore sequences.fasta mydb

# With partitions and parallel processing
af_kmerstore --partition=bacteria --numthreads=4 sequences.fasta mydb

# From standard input
cat sequences.fasta | af_kmerstore stdin mydb

# Custom parameters
af_kmerstore --datatype=DNA2 --minlen=100000 sequences.fasta mydb
```

### af_kmerpart

Update partition information for sequences based on accession numbers.

#### Usage
```bash
af_kmerpart [options] input_file database_name
```

#### Options
- `--partition=NAME` - Partition name to add (required, multiple values allowed)
- `--numthreads=INT` - Number of parallel threads (default: 1)

#### Input File
- Plain text file with one accession number per line
- Use `-`, `stdin`, or `STDIN` for standard input
- Lines starting with `#` are treated as comments

#### Examples
```bash
# Add partition to sequences
af_kmerpart --partition=bacteria accessions.txt mydb

# Multiple partitions
af_kmerpart --partition=bacteria,archaea accessions.txt mydb

# From standard input
echo -e "AB123456\nCD789012" | af_kmerpart --partition=bacteria stdin mydb
```

### af_kmerindex

Create or drop GIN indexes on sequence data.

#### Usage
```bash
af_kmerindex [options] database_name
```

#### Options
- `--mode=create|drop` - Operation mode (required)
- `--tablespace=NAME` - Tablespace name for CREATE INDEX

#### Examples
```bash
# Create indexes
af_kmerindex --mode=create mydb

# Create indexes on specific tablespace
af_kmerindex --mode=create --tablespace=fast_ssd mydb

# Drop indexes
af_kmerindex --mode=drop mydb
```

### af_kmersearch

Search DNA sequences using k-mer similarity.

#### Usage
```bash
af_kmersearch [options] input_file output_file
```

#### Options
- `--db=DATABASE` - PostgreSQL database name (required)
- `--partition=NAME` - Limit search to specific partition
- `--maxnseq=INT` - Maximum number of results per query (default: 1000)
- `--minscore=INT` - Minimum score threshold
- `--numthreads=INT` - Number of parallel threads (default: 1)

#### Input/Output Files
- Input: Multi-FASTA format, use `-`, `stdin`, or `STDIN` for standard input
- Output: TSV format, use `-`, `stdout`, or `STDOUT` for standard output

#### Output Format
Tab-separated values with 4 columns:
1. Query sequence number (1-based)
2. Query FASTA label
3. CORRECTEDSCORE from pg_kmersearch
4. Comma-separated seqid list

#### Examples
```bash
# Basic search
af_kmersearch --db=mydb query.fasta results.tsv

# Search with partition filter
af_kmersearch --db=mydb --partition=bacteria query.fasta results.tsv

# Parallel search with custom parameters
af_kmersearch --db=mydb --numthreads=4 --maxnseq=500 query.fasta results.tsv

# Pipeline usage
cat query.fasta | af_kmersearch --db=mydb stdin stdout > results.tsv
```

### af_kmerdbinfo

Display metadata information from af_kmersearch database.

#### Usage
```bash
af_kmerdbinfo [options] database_name
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
- Lists partition information with sequence and character counts

#### Examples
```bash
# Basic usage
af_kmerdbinfo mydb

# Remote database
af_kmerdbinfo --host=remote-server mydb

# Custom connection parameters
af_kmerdbinfo --host=localhost --port=5433 --username=postgres mydb
```

### af_kmersearchclient

Remote k-mer search client with asynchronous job processing, load balancing, and retry logic.

#### Usage
```bash
# New job submission
af_kmersearchclient [options] input_file output_file

# Resume existing job
af_kmersearchclient --resume=JOB_ID

# Cancel existing job
af_kmersearchclient --cancel=JOB_ID

# List active jobs
af_kmersearchclient --jobs
```

#### Options
- `--server=SERVERS` - Server URL(s) - single server or comma-separated list
- `--serverlist=FILE` - File containing server URLs (one per line)
- `--db=DATABASE` - PostgreSQL database name (optional if server has default)
- `--partition=NAME` - Limit search to specific partition (optional)
- `--maxnseq=INT` - Maximum number of results per query (default: 1000)
- `--minscore=INT` - Minimum score threshold (optional)
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
af_kmersearchclient --netrc-file=/path/to/netrc --server=https://server.com --db=mydb query.fasta results.tsv
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
af_kmersearchclient --http-user=myusername --http-password=mypassword --server=https://server.com --db=mydb query.fasta results.tsv
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

- **Job Persistence**: Jobs are saved to `.af_kmersearchclient` file for resume capability
- **Automatic Polling**: Uses adaptive intervals (5s → 10s → 20s → 30s → 60s)
- **Resume Support**: Can resume interrupted jobs using `--resume=JOB_ID`
- **Cancel Support**: Can cancel running jobs using `--cancel=JOB_ID`
- **Job Management**: List active jobs with `--jobs`

#### Examples
```bash
# Basic usage with asynchronous processing
af_kmersearchclient --server=localhost --db=mydb query.fasta results.tsv

# Multiple servers with load balancing
af_kmersearchclient --server="server1,server2,server3" --db=mydb query.fasta results.tsv

# Server list file
af_kmersearchclient --serverlist=servers.txt --db=mydb query.fasta results.tsv

# With authentication (.netrc file)
af_kmersearchclient --server=https://server.com --db=mydb --netrc-file=.netrc query.fasta results.tsv

# With authentication (command line)
af_kmersearchclient --server=https://server.com --db=mydb --http-user=myuser --http-password=mypass query.fasta results.tsv

# Parallel processing with retries
af_kmersearchclient --server=localhost --db=mydb --numthreads=4 --maxnretry=10 query.fasta results.tsv

# Pipeline usage
cat query.fasta | af_kmersearchclient --server=localhost --db=mydb stdin stdout > results.tsv

# Job management examples
af_kmersearchclient --jobs                                    # List active jobs
af_kmersearchclient --resume=20250703T120000-AbCdEf123456     # Resume job
af_kmersearchclient --cancel=20250703T120000-AbCdEf123456     # Cancel job
```

### af_kmersearchserver.pl

REST API server for k-mer search (standalone HTTP server).

#### Usage
```bash
perl af_kmersearchserver.pl [options]
```

#### Options
- `--listen-port=PORT` - HTTP server listen port (default: 8080)
- `--numthreads=INT` - Number of parallel request processing threads (default: 5)

#### Configuration
Edit default values in the script header:
```perl
my $default_database = 'mykmersearch';  # Default database name
my $default_partition = 'bacteria';     # Default partition name
my $default_maxnseq = 1000;             # Default max results
my $default_minscore = '10';            # Default min score
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
  "partition": "partition_name",
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
  "default_partition": "bacteria",
  "default_maxnseq": 1000,
  "default_minscore": "10",
  "server_version": "1.0",
  "supported_endpoints": ["/search", "/result", "/status", "/cancel", "/metadata"]
}
```

#### Examples
```bash
# Start server
perl af_kmersearchserver.pl --listen-port=8080

# Start server with custom thread count
perl af_kmersearchserver.pl --listen-port=8080 --numthreads=10

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

### af_kmersearchserver.fcgi

FastCGI version for production web servers (NGINX/Apache).

#### Usage
```bash
perl af_kmersearchserver.fcgi [options]
```

#### Options
- `--numthreads=NUM` - Number of FastCGI processes (default: 5)

#### Configuration
Same as af_kmersearchserver.pl - edit default values in script header.

#### NGINX Setup
```nginx
server {
    listen 80;
    server_name your-domain.com;
    
    location /api/search {
        include fastcgi_params;
        fastcgi_pass unix:/var/run/af_kmersearch.sock;
        fastcgi_param SCRIPT_FILENAME $document_root$fastcgi_script_name;
    }
}
```

#### Apache Setup
```apache
<VirtualHost *:80>
    ServerName your-domain.com
    
    ScriptAlias /api/search /path/to/af_kmersearchserver.fcgi
    
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
spawn-fcgi -s /var/run/af_kmersearch.sock -U nginx -G nginx \
           -u www-data -g www-data -P /var/run/af_kmersearch.pid \
           -- perl af_kmersearchserver.fcgi --numthreads=5
```

### af_kmersearchserver.psgi

PSGI version for modern web deployment with various PSGI servers.

#### Usage
```bash
perl af_kmersearchserver.psgi [options]
```

#### Options
- `--host=HOST` - PostgreSQL server host (default: $PGHOST or localhost)
- `--port=PORT` - PostgreSQL server port (default: $PGPORT or 5432)
- `--username=USER` - PostgreSQL username (default: $PGUSER or current user)
- `--listen-port=PORT` - HTTP server listen port (default: 5000)
- `--workers=NUM` - Number of worker processes (default: 5)
- `--help, -h` - Show help message

#### Configuration
Same as af_kmersearchserver.pl - edit default values in script header.

#### Deployment Options
```bash
# Standalone (built-in Starman server)
perl af_kmersearchserver.psgi

# With plackup
plackup -p 5000 --workers 10 af_kmersearchserver.psgi

# With other PSGI servers
starman --port 5000 --workers 10 af_kmersearchserver.psgi
uwsgi --http :5000 --psgi af_kmersearchserver.psgi
```

#### Examples
```bash
perl af_kmersearchserver.psgi
perl af_kmersearchserver.psgi --listen-port=8080 --workers=10
plackup -p 8080 --workers 20 af_kmersearchserver.psgi
```

## Workflow Examples

### Complete Database Setup and Search

1. **Create database and store sequences:**
   ```bash
   af_kmerstore --partition=bacteria sequences.fasta mydb
   ```

2. **Add partition information:**
   ```bash
   af_kmerpart --partition=pathogenic bacteria_ids.txt mydb
   ```

3. **Create indexes:**
   ```bash
   af_kmerindex --mode=create mydb
   ```

4. **Check database information:**
   ```bash
   af_kmerdbinfo mydb
   ```

5. **Search sequences:**
   ```bash
   af_kmersearch --db=mydb --partition=pathogenic query.fasta results.tsv
   ```

### Web API Deployment

1. **Configure defaults:**
   ```perl
   # Edit af_kmersearchserver.fcgi
   my $default_database = 'mydb';
   my $default_partition = 'bacteria';
   ```

2. **Deploy with NGINX:**
   ```bash
   spawn-fcgi -s /var/run/af_kmersearch.sock \
              -- perl af_kmersearchserver.fcgi --numthreads=5
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
- Use partitions for large datasets
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