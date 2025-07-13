# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

The af_kmersearch suite is a comprehensive Perl-based toolkit for DNA sequence analysis using k-mer similarity search with PostgreSQL and the pg_kmersearch extension. The system consists of command-line tools for sequence storage, indexing, searching, and web servers for remote API access with asynchronous job processing.

## Core Components

### Command-Line Tools
- `af_kmerstore` - Store multi-FASTA sequences in PostgreSQL database
- `af_kmerindex` - Create and manage k-mer search indexes 
- `af_kmersearch` - Local sequence similarity search
- `af_kmersearchclient` - Remote API client with job management
- `af_kmerpart` - Database partitioning utilities
- `af_kmerdbinfo` - Database information and statistics
- `calcsegment` - Sequence segmentation calculations

### Server Components
- `af_kmersearchserver.pl` - Standalone HTTP server
- `af_kmersearchserver.fcgi` - FastCGI server implementation
- `af_kmersearchserver.psgi` - PSGI/Plack server implementation

### Architecture
- **Database Layer**: PostgreSQL with pg_kmersearch extension for k-mer indexing
- **Job Management**: SQLite-based asynchronous job processing with result caching
- **Client-Server**: HTTP API with Base64-encoded sequence data and JSON responses
- **Multi-server Support**: Load balancing with failover and retry logic

## Common Development Commands

### Dependency Management
```bash
# Check all required Perl modules and external tools
perl check_dependencies.pl

# Install missing dependencies on Ubuntu/Debian
sudo apt-get install libdbi-perl libdbd-pg-perl libdbd-sqlite3-perl \
                     libjson-perl libwww-perl liburi-perl \
                     libhttp-server-simple-perl libcgi-fast-perl \
                     libfcgi-procmanager-perl libplack-perl starman

# Install via CPAN
cpanm DBI DBD::Pg DBD::SQLite JSON LWP::UserAgent HTTP::Request::Common \
      URI HTTP::Server::Simple::CGI CGI::Fast FCGI::ProcManager \
      Plack::Request Plack::Response Plack::Builder Plack::Handler::Starman
```

### Testing the Tools
```bash
# Basic workflow test
af_kmerstore sampledata.fasta testdb
af_kmerindex --mode=create testdb
af_kmersearch --db=testdb sampledata.fasta results.tsv

# Test server functionality (asynchronous job processing)
perl af_kmersearchserver.pl --listen-port=8080 &
af_kmersearchclient --server=localhost --db=testdb sampledata.fasta results.tsv

# Test multi-server load balancing
af_kmersearchclient --server="server1,server2,server3" --db=testdb sampledata.fasta results.tsv
```

### Code Analysis
```bash
# Check Perl syntax
perl -c script_name.pl

# Find specific functions or database operations
grep -n "sub " *.pl
grep -n "DBI" *.pl
grep -n "pg_kmersearch" *.pl
```

## Key Configuration Points

### Database Connection
- Environment variables: `PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD`
- Default values defined in each script's header section
- PostgreSQL must have pg_kmersearch extension installed

### Server Configuration  
- Default ports: 8080 (HTTP), various for FastCGI/PSGI
- SQLite job database: `./af_kmersearchserver.sqlite`
- Configurable limits: max jobs, timeouts, result retention

### Performance Parameters
- K-mer size: Default 8, configurable via `--kmer_size` in af_kmerindex
- Search modes: minimum, normal, maximum (affects sensitivity/speed) in af_kmersearch
- Thread counts: Configurable for parallel processing (`--numthreads`)
- Memory settings: Working memory for index operations (`--workingmemory`)
- Compression: Database compression options (lz4, pglz, disable) in af_kmerstore
- Input formats: FASTA, compressed files (.gz, .bz2, .xz, .zst), BLAST databases, wildcards

## Important Notes

- All tools require PostgreSQL with pg_kmersearch extension
- Supports multiple input formats: FASTA, compressed files, BLAST databases, stdin
- Results are typically tab-separated values (TSV)
- Server components support job-based asynchronous processing with SQLite job management
- Authentication via .netrc files or HTTP Basic Auth is supported for remote servers
- Load balancing and failover support for multiple servers
- Job persistence with resume/cancel capabilities
- Comprehensive error handling, logging, and transaction management throughout the codebase
- Sequence validation and automatic splitting for large sequences
- Memory-efficient streaming processing for large datasets