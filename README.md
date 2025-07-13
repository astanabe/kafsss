# af_kmersearch suite

A comprehensive toolkit for DNA sequence analysis using k-mer similarity search with PostgreSQL and the pg_kmersearch extension.

## Features

- **Database Management**: Store and index multi-FASTA sequences in PostgreSQL with compression and partitioning
- **High-Performance Search**: Fast k-mer similarity search with configurable parameters and parallel processing
- **Asynchronous Processing**: Job-based search with SQLite job management, automatic polling, and resume capability
- **Load Balancing**: Multi-server support with failover, retry logic, and round-robin distribution
- **Multiple Input Formats**: FASTA, compressed files (.gz, .bz2, .xz, .zst), BLAST databases, wildcard patterns
- **Multiple Deployment Options**: Standalone HTTP, FastCGI, and PSGI servers with production-ready scaling
- **Flexible Authentication**: Support for .netrc files and HTTP Basic authentication
- **Advanced Job Management**: Resume, cancel, and monitor jobs with persistent storage

## Quick Start

```bash
# Check dependencies
perl check_dependencies.pl

# Store sequences in database
af_kmerstore sequences.fasta mydb

# Create search indexes  
af_kmerindex --mode=create mydb

# Search sequences locally
af_kmersearch --db=mydb query.fasta results.tsv

# Or use remote server with asynchronous job management
af_kmersearchclient --server=localhost --db=mydb query.fasta results.tsv

# Multiple servers with load balancing
af_kmersearchclient --server="server1,server2,server3" --db=mydb query.fasta results.tsv
```

## Documentation

See detailed documentation in the `doc/` directory:
- **English**: [doc/af_kmersearch.en.md](doc/af_kmersearch.en.md)
- **日本語**: [doc/af_kmersearch.ja.md](doc/af_kmersearch.ja.md)