#!/usr/bin/perl

use strict;
use warnings;

# kafsss Tools - Dependency Checker
# このスクリプトは、kafsssツールキットの実行に必要なPerlモジュールが
# すべてインストールされているかを確認します。

my $VERSION = "__VERSION__";

print "kafsss Tools - Dependency Checker v$VERSION\n";
print "=" x 60 . "\n\n";

# 必須モジュールの定義
my @core_modules = (
    ['DBI', 'Database access (PostgreSQL/SQLite)'],
    ['DBD::Pg', 'PostgreSQL driver'],
    ['JSON', 'JSON format processing'],
    ['Getopt::Long', 'Command line argument parsing'],
    ['POSIX', 'POSIX system functions'],
    ['Sys::Hostname', 'System hostname retrieval'],
    ['File::Basename', 'File name manipulation'],
    ['MIME::Base64', 'Base64 encoding/decoding (for servers)'],
    ['Time::HiRes', 'High-resolution time functions (for servers)'],
    ['Fcntl', 'File control operations (for servers)'],
);

my @network_modules = (
    ['LWP::UserAgent', 'HTTP client (for kafsssearchclient)'],
    ['HTTP::Request::Common', 'HTTP request generation (for kafsssearchclient)'],
    ['URI', 'URI parsing and encoding (for kafsssearchclient)'],
);

my @server_modules = (
    ['HTTP::Server::Simple::CGI', 'Standalone web server (for kafsssearchserver.pl)'],
    ['CGI::Fast', 'FastCGI implementation (for kafsssearchserver.fcgi)'],
    ['FCGI::ProcManager', 'FastCGI process manager (for kafsssearchserver.fcgi)'],
    ['Plack::Request', 'PSGI/Plack framework (for kafsssearchserver.psgi)'],
    ['Plack::Response', 'PSGI/Plack framework (for kafsssearchserver.psgi)'],
    ['Plack::Builder', 'PSGI/Plack framework (for kafsssearchserver.psgi)'],
    ['Plack::Handler::Starman', 'Starman HTTP server (for kafsssearchserver.psgi)'],
);

my @optional_modules = (
    ['Crypt::OpenSSL::Random', 'Cryptographically secure random numbers (fallback)'],
    ['DBD::SQLite', 'SQLite driver (for job management)'],
);

my $all_ok = 1;

# モジュール確認関数
sub check_module {
    my ($module, $description) = @_;
    
    eval "use $module";
    if ($@) {
        print "✗ MISSING: $module - $description\n";
        return 0;
    } else {
        # バージョン情報を取得（可能な場合）
        my $version = '';
        {
            no strict 'refs';
            my $version_var = "${module}::VERSION";
            if (defined ${$version_var}) {
                $version = " (v${$version_var})";
            }
        }
        print "✓ OK: $module$version - $description\n";
        return 1;
    }
}

# コアモジュールの確認
print "Core modules (required for all tools):\n";
print "-" x 40 . "\n";
for my $module_info (@core_modules) {
    my ($module, $description) = @$module_info;
    $all_ok = 0 unless check_module($module, $description);
}

print "\n";

# ネットワークモジュールの確認
print "Network client modules (for kafsssearchclient):\n";
print "-" x 50 . "\n";
for my $module_info (@network_modules) {
    my ($module, $description) = @$module_info;
    unless (check_module($module, $description)) {
        print "  Note: This module is only required for kafsssearchclient\n";
    }
}

print "\n";

# サーバーモジュールの確認
print "Server modules (for web API servers):\n";
print "-" x 35 . "\n";
for my $module_info (@server_modules) {
    my ($module, $description) = @$module_info;
    unless (check_module($module, $description)) {
        print "  Note: This module is only required for server functionality\n";
    }
}

print "\n";

# オプショナルモジュールの確認
print "Optional modules (recommended but not required):\n";
print "-" x 45 . "\n";
for my $module_info (@optional_modules) {
    my ($module, $description) = @$module_info;
    unless (check_module($module, $description)) {
        print "  Note: This module provides enhanced functionality but is not required\n";
    }
}

print "\n";

# 外部コマンドの確認
print "External commands (recommended for full functionality):\n";
print "-" x 50 . "\n";
my @external_commands = (
    ['pigz', 'Parallel gzip compression (fallback: gzip)'],
    ['pbzip2', 'Parallel bzip2 compression (fallback: bzip2)'],
    ['xz', 'XZ compression support'],
    ['zstd', 'Zstd compression support'],
    ['blastdbcmd', 'BLAST database access'],
    ['openssl', 'Cryptographically secure random numbers (fallback)'],
);

for my $cmd_info (@external_commands) {
    my ($cmd, $description) = @$cmd_info;
    if (system("which $cmd > /dev/null 2>&1") == 0) {
        print "✓ OK: $cmd - $description\n";
    } else {
        print "✗ MISSING: $cmd - $description\n";
    }
}

print "\n";
print "=" x 60 . "\n";

if ($all_ok) {
    print "✓ All core dependencies are satisfied!\n";
    print "You can run all kafsss tools.\n";
    exit 0;
} else {
    print "✗ Some core dependencies are missing.\n";
    print "Please install the missing modules using one of these methods:\n\n";
    
    print "Ubuntu/Debian:\n";
    print "  sudo apt-get install libdbi-perl libdbd-pg-perl libdbd-sqlite3-perl \\\n";
    print "                       libjson-perl libwww-perl liburi-perl \\\n";
    print "                       libhttp-server-simple-perl libcgi-fast-perl \\\n";
    print "                       libfcgi-procmanager-perl libplack-perl starman \\\n";
    print "                       libcrypt-openssl-random-perl\n\n";
    
    print "  # Optional external tools:\n";
    print "  sudo apt-get install pigz pbzip2 xz-utils zstd ncbi-blast+ openssl\n\n";
    
    print "RHEL/CentOS/Fedora:\n";
    print "  sudo yum install perl-DBI perl-DBD-Pg perl-DBD-SQLite perl-JSON \\\n";
    print "                   perl-libwww-perl perl-URI perl-HTTP-Server-Simple \\\n";
    print "                   perl-CGI-Fast perl-FCGI-ProcManager perl-Plack \\\n";
    print "                   perl-Crypt-OpenSSL-Random\n\n";
    
    print "  # Optional external tools:\n";
    print "  sudo yum install pigz pbzip2 xz zstd ncbi-blast+ openssl\n\n";
    
    print "CPAN:\n";
    print "  cpanm DBI DBD::Pg DBD::SQLite JSON LWP::UserAgent HTTP::Request::Common \\\n";
    print "        URI HTTP::Server::Simple::CGI CGI::Fast FCGI::ProcManager \\\n";
    print "        Plack::Request Plack::Response Plack::Builder Plack::Handler::Starman \\\n";
    print "        Crypt::OpenSSL::Random\n\n";
    
    exit 1;
}