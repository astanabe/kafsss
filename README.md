# AF KmerSearch Tools

A comprehensive toolkit for DNA sequence analysis using k-mer similarity search with PostgreSQL and the pg_kmersearch extension.

## Features

- **Database Management**: Store and index multi-FASTA sequences in PostgreSQL
- **High-Performance Search**: Fast k-mer similarity search with configurable parameters  
- **Asynchronous Processing**: Job-based search with automatic polling and resume capability
- **Load Balancing**: Multi-server support with failover and retry logic
- **Multiple Deployment Options**: Standalone HTTP, FastCGI, and PSGI servers
- **Flexible Authentication**: Support for .netrc files and HTTP Basic authentication

## Quick Start

```bash
# Check dependencies
perl check_dependencies.pl

# Store sequences in database
perl af_kmerstore.pl sequences.fasta mydb

# Create search indexes  
perl af_kmerindex.pl --mode=create mydb

# Search sequences locally
perl af_kmersearch.pl --db=mydb query.fasta results.tsv

# Or use remote server with job management
perl af_kmersearchclient.pl --server=localhost --db=mydb query.fasta results.tsv
```

## Documentation

See detailed documentation in the `doc/` directory:
- **English**: [doc/af_kmersearch.en.md](doc/af_kmersearch.en.md)
- **日本語**: [doc/af_kmersearch.ja.md](doc/af_kmersearch.ja.md)