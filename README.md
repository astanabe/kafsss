# kafsss: K-mer based Alignment-Free Splitted Sequence Search

A comprehensive toolkit for DNA sequence analysis using k-mer similarity search with PostgreSQL and the pg_kmersearch extension.

## Features

- **Database Management**: Store and index multi-FASTA sequences in PostgreSQL with compression and subsetting
- **High-Performance Search**: Fast k-mer similarity search with configurable parameters and parallel processing
- **Multiple GIN Index Support**: Create multiple indexes with different parameters; automatic or manual selection at search time
- **Multi-Database Support**: Server components can serve multiple databases with configured subsets
- **Asynchronous Processing**: Job-based search with SQLite job management, automatic polling, and resume capability
- **Load Balancing**: Multi-server support with failover, retry logic, and round-robin distribution
- **Multiple Input Formats**: FASTA, compressed files (.gz, .bz2, .xz, .zst), BLAST databases, wildcard patterns
- **Multiple Output Formats**: TSV, multi-file TSV, FASTA, BLASTDB (creates BLAST databases directly)
- **Multiple Deployment Options**: PSGI server supporting standalone, FastCGI, and various deployment configurations
- **Flexible Authentication**: Support for .netrc files and HTTP Basic authentication
- **Advanced Job Management**: Resume, cancel, and monitor jobs with persistent storage

## Installation

```bash
# Install command-line tools
make
sudo make install

# Custom installation prefix
make PREFIX=/opt/kafsss
sudo make install PREFIX=/opt/kafsss
```

**Note**: Server script (`kafsssearchserver.psgi`) is installed separately:
```bash
make installserver DESTDIR=/var/www/kafsss
```

## Quick Start

```bash
# Check dependencies
perl check_dependencies.pl

# Store sequences in database
kafssstore sequences.fasta mydb

# Optional: deduplicate sequences
kafssdedup mydb

# Optional: partition table for performance
kafsspart --npart=16 mydb

# Perform k-mer frequency analysis
kafssfreq --mode=create mydb

# Create search indexes  
kafssindex --mode=create mydb

# Search sequences locally
kafsssearch --db=mydb query.fasta results.tsv

# Or use remote server with asynchronous job management
kafsssearchclient --server=localhost --db=mydb query.fasta results.tsv

# Multiple servers with load balancing
kafsssearchclient --server="server1,server2,server3" --db=mydb query.fasta results.tsv

# Check server metadata and configuration
curl http://localhost:8080/metadata
```

## Documentation

See detailed documentation in the `doc/` directory:
- **English**: [doc/kafsss.en.md](doc/kafsss.en.md)
- **日本語**: [doc/kafsss.ja.md](doc/kafsss.ja.md)