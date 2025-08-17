# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Language Usage Guidelines

- **Dialogue/Conversation**: Use Japanese
- **Source Code**: Use English only
- **Comments**: Use English only (in source code)
- **Commit Messages**: Use English only
- **Documentation**: 
  - `CLAUDE.md`: Use English only
  - `README.md`: Use English only
  - Other documentation: English preferred

## Important Development Principles

- **No speculation or assumptions**: Never make changes or write documentation based on speculation or assumptions. Always verify information before making any changes or documentation updates.
- **Fact-checking**: When uncertain about technical details, check the actual source code, documentation, or ask for clarification rather than guessing.
- **Verify possibilities**: Never leave "possibilities" or "might be" statements unresolved. Always confirm and verify to reach definitive conclusions. Do not make decisions or leave issues unaddressed based on uncertain possibilities.
- **Error recovery**: When mistakes are made, do not attempt to patch or cover up errors. Instead, restore the previous state from backups or git repository. Use `git checkout`, `git reset`, or restore from backup files rather than making additional changes to fix mistakes.
- **Git commit restrictions**: 
  - **Never commit without explicit instruction** from the user
  - **Never use `git add -A` or `git add .`** especially when temporary files exist
  - **Always add files selectively** using specific file paths
  - **Check git status** before any add operation to understand what will be staged

## Project Overview

The kafsss (K-mer based Alignment-Free Splitted Sequence Search) suite is a comprehensive Perl-based toolkit for DNA sequence analysis using k-mer similarity search with PostgreSQL and the pg_kmersearch extension. The system consists of command-line tools for sequence storage, indexing, searching, and web servers for remote API access with asynchronous job processing.

## Important Notes on pg_kmersearch Compatibility

**This project requires the latest version of pg_kmersearch extension. Compatibility with older versions is not maintained.**

The codebase assumes the following pg_kmersearch features from the latest version:
- Function name: `kmersearch_matchscore()` (not rawscore/correctedscore)
- GUC variables: `kmersearch.min_shared_kmer_rate` (not min_shared_ngram_key_rate)
- Rawscore cache has been completely removed (kmersearch.rawscore_cache_max_entries no longer exists)
- GIN index creation requires explicit operator class specification (e.g., `kmersearch_dna4_gin_ops_int4`)

### Required GUC Variable Settings

The following GUC variables must be set every time a PostgreSQL database connection is established:

**kafssfreq**:
- `kmersearch.kmer_size`
- `kmersearch.occur_bitlen`
- `kmersearch.max_appearance_rate`
- `kmersearch.max_appearance_nrow`

**kafssindex**:
- `kmersearch.kmer_size`
- `kmersearch.occur_bitlen`
- `kmersearch.max_appearance_rate`
- `kmersearch.max_appearance_nrow`
- `kmersearch.preclude_highfreq_kmer`
- `kmersearch.force_use_parallel_highfreq_kmer_cache`

**kafsssearch**:
- `kmersearch.kmer_size`
- `kmersearch.occur_bitlen`
- `kmersearch.max_appearance_rate`
- `kmersearch.max_appearance_nrow`
- `kmersearch.preclude_highfreq_kmer`
- `kmersearch.force_use_parallel_highfreq_kmer_cache`
- `kmersearch.min_score`
- `kmersearch.min_shared_kmer_rate`

**kafsssearchserver.\*** (all server variants):
- `kmersearch.kmer_size`
- `kmersearch.occur_bitlen`
- `kmersearch.max_appearance_rate`
- `kmersearch.max_appearance_nrow`
- `kmersearch.preclude_highfreq_kmer`
- `kmersearch.force_use_parallel_highfreq_kmer_cache`
- `kmersearch.min_score`
- `kmersearch.min_shared_kmer_rate`

## Workflow

The typical kafsss workflow is as follows:

1. **kafssstore**: Create database and register sequence data
2. **kafssdedup**: Merge identical sequences to reduce row count and storage (verify partition table compatibility)
3. **kafsspart**: Partition kafsss_data table for improved performance
4. **kafssfreq**: Perform k-mer frequency analysis (stores high-frequency k-mers in system tables)
5. **kafssindex**: Build GIN index with k-mers as keys (excludes high-frequency k-mers)
6. **kafsssubset**: Register subset names to enable subset-specific search results
7. **kafsssearch** or **kafsssearchserver.\***: Retrieve IDs (and optionally sequences) of sequences similar to query sequence from database

## Core Components

### Command-Line Tools
- `kafssstore` - Store multi-FASTA sequences in PostgreSQL database
- `kafssdedup` - Sequence deduplication tool (merge identical sequences)
- `kafsspart` - Partition kafsss_data table using pg_kmersearch's partition function
- `kafssfreq` - K-mer frequency analysis tool (stores high-frequency k-mers in system tables)
- `kafssindex` - Create and manage k-mer search indexes (GIN index with k-mers as keys)
- `kafsspreload` - Preload high-frequency k-mer cache into memory for acceleration
- `kafsssubset` - Database subset management utilities (add/remove subset labels)
- `kafssdbinfo` - Database information and statistics
- `kafsssearch` - Local sequence similarity search
- `kafsssearchclient` - Remote API client with asynchronous job management

### Server Components
- `kafsssearchserver.pl` - Standalone HTTP server
- `kafsssearchserver.fcgi` - FastCGI server implementation
- `kafsssearchserver.psgi` - PSGI/Plack server implementation

### Architecture
- **Database Layer**: PostgreSQL with pg_kmersearch extension for k-mer indexing
- **Job Management**: SQLite-based asynchronous job processing with result caching
- **Client-Server**: HTTP API with Base64-encoded sequence data and JSON responses
- **Multi-server Support**: Load balancing with failover and retry logic

## Common Development Commands

### Installation
```bash
# Install command-line tools via make
make
sudo make install

# Custom installation prefix
make PREFIX=/opt/kafsss
sudo make install PREFIX=/opt/kafsss

# Note: Server scripts (kafsssearchserver.pl, .fcgi, .psgi) are not installed by make
# They should be manually deployed to appropriate web server locations
```

### Database Setup (Required)
```bash
# 1. Install PostgreSQL and pg_kmersearch extension
sudo apt-get install postgresql postgresql-contrib
# Install pg_kmersearch extension package (contact your system administrator)

# 2. Create PostgreSQL user and database
sudo -u postgres psql
CREATE USER yourusername CREATEDB;
ALTER USER yourusername PASSWORD 'yourpassword';
\q

# 3. Set environment variables
export PGUSER=yourusername
export PGPASSWORD=yourpassword
export PGHOST=localhost
export PGPORT=5432

# 4. Create extension in target database (run once per database)
# Option A: Have superuser create extension
sudo -u postgres psql -d your_database
CREATE EXTENSION IF NOT EXISTS pg_kmersearch;
\q

# Option B: Grant temporary superuser permission
sudo -u postgres psql
ALTER USER yourusername SUPERUSER;
\q
# Run kafssstore, then revoke:
# ALTER USER yourusername NOSUPERUSER;
```

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
# Basic workflow test (ensure database setup is complete first)
kafssstore sampledata.fasta testdb
kafssdedup testdb  # Optional: deduplicate sequences
kafsspart --npart=16 testdb  # Optional: partition table for performance
kafssfreq --mode=create testdb
kafssindex --mode=create testdb
kafsssearch --db=testdb sampledata.fasta results.tsv

# Test server functionality (asynchronous job processing)
perl kafsssearchserver.pl --listen-port=8080 &
kafsssearchclient --server=localhost --db=testdb sampledata.fasta results.tsv

# Test multi-server load balancing
kafsssearchclient --server="server1,server2,server3" --db=testdb sampledata.fasta results.tsv

# Test server metadata endpoint
curl http://localhost:8080/metadata

# Test subset management
kafsssubset --subset=bacteria accessions.txt testdb
kafsssubset --mode=del --subset=bacteria accessions.txt testdb
kafsssubset --mode=del --subset=all all testdb
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
- SQLite job database: `./kafsssearchserver.sqlite`
- Configurable limits: max jobs, timeouts, result retention
- API endpoints: `/search`, `/result`, `/status`, `/cancel`, `/metadata` (GET)

### Performance Parameters
- K-mer size: Default 8, configurable via `--kmersize` in kafssfreq and kafssindex
- Search modes in kafsssearch:
  - `minimum` (alias: `min`) - No score calculation, fastest
  - `matchscore` (alias: `score`) - Calculate match scores (default)
  - `sequence` (alias: `seq`) - Include sequence data, no score
  - `maximum` (alias: `max`) - All columns including score and sequence
- Search parameters:
  - `--maxnseq`: Maximum results per query (default: 1000, 0=unlimited)
  - `--minscore`: Minimum score threshold (default: 1)
  - `--minpsharedkmer`: Minimum shared k-mer rate (default: 0.5, range: 0.0-1.0)
- Thread counts: Configurable for parallel processing (`--numthreads`)
- Memory settings: Working memory for index operations (`--workingmemory`)
- Compression: Database compression options (lz4, pglz, disable) in kafssstore
- Input formats: FASTA, compressed files (.gz, .bz2, .xz, .zst), BLAST databases, wildcards
- Sequence splitting: `--ovllen` must be less than half of `--minsplitlen` to prevent overlap conflicts
- **Database Setup**: Users must have CREATEDB permission and pg_kmersearch extension must be available
- **Extension Creation**: Either have superuser create the extension or grant temporary superuser permission

## Troubleshooting

### Common Error Messages
- `PostgreSQL user 'username' does not exist`: Create user with `CREATE USER username CREATEDB;`
- `User 'username' does not have CREATE DATABASE permission`: Grant with `ALTER USER username CREATEDB;`
- `Extension 'pg_kmersearch' is not available`: Install pg_kmersearch extension package
- `Current user does not have permission to create extensions`: Have superuser create extension or grant temporary superuser permission

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