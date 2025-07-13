# af_kmersearch suite

pg_kmersearch拡張を使用してPostgreSQLでDNA配列の保存、管理、検索を行う包括的なツールキット。

## 概要

af_kmersearch suiteは、k-mer類似性検索を使用したDNA配列解析の完全なソリューションを提供します。ツールキットは、DNA配列管理、検索操作、非同期ジョブ処理によるサーバーデプロイメントの異なる側面を処理する10のPerlスクリプトで構成されています。

## 前提条件

- PostgreSQL 9.1以降
- pg_kmersearch拡張がインストール済み
- Perl 5.10以降

### 必要なPerlモジュール

すべてのコアツールで必要：
- `DBI` - データベースアクセス（PostgreSQL/SQLite）
- `DBD::Pg` - PostgreSQLドライバ
- `JSON` - JSON形式の処理
- `Getopt::Long` - コマンドライン引数解析
- `POSIX` - POSIXシステム機能
- `Sys::Hostname` - システムホスト名取得
- `File::Basename` - ファイル名操作
- `MIME::Base64` - Base64エンコード/デコード（サーバー用）
- `Time::HiRes` - 高解像度時間関数（サーバー用）
- `Fcntl` - ファイル制御操作（サーバー用）

ネットワーククライアント用（af_kmersearchclient）：
- `LWP::UserAgent` - HTTPクライアント
- `HTTP::Request::Common` - HTTPリクエスト生成
- `URI` - URI解析とエンコーディング

サーバー用モジュール：
- `HTTP::Server::Simple::CGI` - スタンドアローンWebサーバ（af_kmersearchserver.pl用）
- `CGI::Fast` および `FCGI::ProcManager` - FastCGI実装（af_kmersearchserver.fcgi用）
- `Plack::Request`、`Plack::Response`、`Plack::Builder` - PSGI/Plackフレームワーク（af_kmersearchserver.psgi用）
- `Plack::Handler::Starman` - Starman HTTP サーバー（af_kmersearchserver.psgi用）

オプション（推奨）：
- `Crypt::OpenSSL::Random` - 暗号学的に安全な乱数
- `DBD::SQLite` - SQLiteドライバ（ジョブ管理用）

### 依存関係のインストール

#### Ubuntu/Debian

```bash
# システムパッケージマネージャーでインストール
sudo apt-get update
sudo apt-get install -y \
    perl \
    libdbi-perl \
    libdbd-pg-perl \
    libjson-perl \
    libwww-perl \
    liburi-perl \
    libhttp-server-simple-perl \
    libcgi-fast-perl \
    libfcgi-procmanager-perl \
    libplack-perl \
    starman

# または、cpanminusを使用
sudo apt-get install -y cpanminus
sudo cpanm DBI DBD::Pg JSON LWP::UserAgent HTTP::Request::Common URI \
           HTTP::Server::Simple::CGI CGI::Fast FCGI::ProcManager \
           Plack::Request Plack::Response Plack::Builder Plack::Handler::Starman
```

#### RHEL/CentOS/Fedora

```bash
# EPELリポジトリを有効化（RHEL/CentOS用）
sudo yum install -y epel-release  # CentOS 7
# または
sudo dnf install -y epel-release  # CentOS 8/Fedora

# システムパッケージマネージャーでインストール
sudo yum install -y perl perl-DBI perl-DBD-Pg perl-JSON perl-libwww-perl \
                    perl-URI perl-HTTP-Server-Simple perl-CGI-Fast \
                    perl-FCGI-ProcManager perl-Plack
# または dnf を使用
sudo dnf install -y perl perl-DBI perl-DBD-Pg perl-JSON perl-libwww-perl \
                     perl-URI perl-HTTP-Server-Simple perl-CGI-Fast \
                     perl-FCGI-ProcManager perl-Plack

# cpanminusを使用（一部のモジュールがパッケージで利用できない場合）
sudo yum install -y perl-App-cpanminus  # または dnf
sudo cpanm DBI DBD::Pg JSON LWP::UserAgent HTTP::Request::Common URI \
           HTTP::Server::Simple::CGI CGI::Fast FCGI::ProcManager \
           Plack::Request Plack::Response Plack::Builder Plack::Handler::Starman
```

#### 手動インストール（CPAN使用）

```bash
# CPANシェルで個別インストール
perl -MCPAN -e 'install DBI'
perl -MCPAN -e 'install DBD::Pg'
perl -MCPAN -e 'install JSON'
perl -MCPAN -e 'install LWP::UserAgent'
perl -MCPAN -e 'install HTTP::Request::Common'
perl -MCPAN -e 'install URI'
perl -MCPAN -e 'install HTTP::Server::Simple::CGI'
perl -MCPAN -e 'install CGI::Fast'
perl -MCPAN -e 'install FCGI::ProcManager'
perl -MCPAN -e 'install Plack'
perl -MCPAN -e 'install Starman'

# または一括インストール
perl -MCPAN -e 'install DBI, DBD::Pg, JSON, LWP::UserAgent, HTTP::Request::Common, URI, HTTP::Server::Simple::CGI, CGI::Fast, FCGI::ProcManager, Plack, Starman'
```

### 依存関係確認

**自動確認スクリプトを使用（推奨）：**

```bash
# 依存関係確認スクリプトを実行
perl check_dependencies.pl
```

このスクリプトは、すべての必要なモジュールの存在を確認し、不足している場合は具体的なインストール手順を表示します。

**手動確認：**

インストール後、以下のコマンドで依存関係を個別に確認することもできます：

```bash
# 基本モジュールの確認
perl -MDBI -e 'print "DBI version: $DBI::VERSION\n"'
perl -MDBD::Pg -e 'print "DBD::Pg version: $DBD::Pg::VERSION\n"'
perl -MJSON -e 'print "JSON version: $JSON::VERSION\n"'

# ネットワークモジュールの確認
perl -MLWP::UserAgent -e 'print "LWP::UserAgent available\n"'
perl -MURI -e 'print "URI available\n"'

# サーバーモジュールの確認
perl -MHTTP::Server::Simple -e 'print "HTTP::Server::Simple available\n"'
perl -MPlack -e 'print "Plack available\n"'
perl -MStarman -e 'print "Starman available\n"'
```

## スクリプト概要

| スクリプト | 用途 |
|-----------|------|
| `af_kmerstore` | FASTA配列をPostgreSQLデータベースに格納 |
| `af_kmerpart` | 配列のパーティション情報を更新 |
| `af_kmerindex` | 配列データのGINインデックスを作成/削除 |
| `af_kmersearch` | k-mer類似性を使用した配列検索 |
| `af_kmerdbinfo` | データベースメタデータ情報を表示 |
| `af_kmersearchclient` | 負荷分散機能付きリモートk-mer検索クライアント |
| `af_kmersearchserver.pl` | 非同期ジョブ処理機能付きk-mer検索用REST APIサーバ（スタンドアローン） |
| `af_kmersearchserver.fcgi` | 本番Webサーバ用FastCGI版 |
| `af_kmersearchserver.psgi` | モダンなWebデプロイ用PSGI版 |
| `calcsegment` | 配列分割パラメータ計算用数学ユーティリティ |

## インストール

1. PostgreSQLにpg_kmersearch拡張をインストール
2. スクリプトを実行可能にする：
   ```bash
   chmod +x af_kmer*.pl
   ```

## データベース接続

すべてのスクリプトはPostgreSQL接続オプションをサポート：

- `--host=HOST` - PostgreSQLサーバホスト（デフォルト: $PGHOST または localhost）
- `--port=PORT` - PostgreSQLサーバポート（デフォルト: $PGPORT または 5432）
- `--username=USER` - PostgreSQLユーザー名（デフォルト: $PGUSER または現在のユーザー）

パスワードは`PGPASSWORD`環境変数から読み取られます。

## スクリプトドキュメント

### af_kmerstore

マルチFASTA DNA配列をPostgreSQLデータベースに格納します。

#### 使用方法
```bash
af_kmerstore [オプション] 入力ファイル名 出力データベース名
```

#### オプション
- `--datatype=DNA2|DNA4` - データ型（デフォルト: DNA4）
- `--minlen=INT` - 分割用最小配列長（デフォルト: 50000）
- `--ovllen=INT` - 分割配列間の重複長（デフォルト: 500）
- `--numthreads=INT` - 並列スレッド数（デフォルト: 1）
- `--partition=NAME` - パーティション名（複数指定可能）
- `--tablespace=NAME` - CREATE DATABASE用テーブルスペース名
- `--overwrite` - 既存データベースを上書き

#### 入力ファイル
- マルチFASTA形式のDNA配列
- 標準入力の場合は `-`、`stdin`、または `STDIN` を使用

#### 使用例
```bash
# 基本的な使用方法
af_kmerstore sequences.fasta mydb

# パーティションと並列処理を使用
af_kmerstore --partition=bacteria --numthreads=4 sequences.fasta mydb

# 標準入力から
cat sequences.fasta | af_kmerstore stdin mydb

# カスタムパラメータ
af_kmerstore --datatype=DNA2 --minlen=100000 sequences.fasta mydb
```

### af_kmerpart

アクセッション番号に基づいて配列のパーティション情報を更新します。

#### 使用方法
```bash
af_kmerpart [オプション] 入力ファイル名 データベース名
```

#### オプション
- `--partition=NAME` - 追加するパーティション名（必須、複数指定可能）
- `--numthreads=INT` - 並列スレッド数（デフォルト: 1）

#### 入力ファイル
- 1行に1つのアクセッション番号を記載したプレーンテキストファイル
- 標準入力の場合は `-`、`stdin`、または `STDIN` を使用
- `#`で始まる行はコメントとして扱われます

#### 使用例
```bash
# 配列にパーティションを追加
af_kmerpart --partition=bacteria accessions.txt mydb

# 複数のパーティション
af_kmerpart --partition=bacteria,archaea accessions.txt mydb

# 標準入力から
echo -e "AB123456\nCD789012" | af_kmerpart --partition=bacteria stdin mydb
```

### af_kmerindex

配列データのGINインデックスを作成または削除します。

#### 使用方法
```bash
af_kmerindex [オプション] データベース名
```

#### オプション
- `--mode=create|drop` - 操作モード（必須）
- `--tablespace=NAME` - CREATE INDEX用テーブルスペース名

#### 使用例
```bash
# インデックス作成
af_kmerindex --mode=create mydb

# 特定のテーブルスペースにインデックス作成
af_kmerindex --mode=create --tablespace=fast_ssd mydb

# インデックス削除
af_kmerindex --mode=drop mydb
```

### af_kmersearch

k-mer類似性を使用してDNA配列を検索します。

#### 使用方法
```bash
af_kmersearch [オプション] 入力ファイル名 出力ファイル名
```

#### オプション
- `--db=DATABASE` - PostgreSQLデータベース名（必須）
- `--partition=NAME` - 特定のパーティションに検索を限定
- `--maxnseq=INT` - クエリあたりの最大結果数（デフォルト: 1000）
- `--minscore=INT` - 最小スコア閾値
- `--numthreads=INT` - 並列スレッド数（デフォルト: 1）

#### 入出力ファイル
- 入力: マルチFASTA形式、標準入力の場合は `-`、`stdin`、または `STDIN`
- 出力: TSV形式、標準出力の場合は `-`、`stdout`、または `STDOUT`

#### 出力形式
4つのカラムを持つタブ区切り値：
1. クエリ配列番号（1ベース）
2. クエリFASTAラベル
3. pg_kmersearchのCORRECTEDSCORE
4. カンマ区切りのseqidリスト

#### 使用例
```bash
# 基本的な検索
af_kmersearch --db=mydb query.fasta results.tsv

# パーティションフィルタを使用した検索
af_kmersearch --db=mydb --partition=bacteria query.fasta results.tsv

# カスタムパラメータを使用した並列検索
af_kmersearch --db=mydb --numthreads=4 --maxnseq=500 query.fasta results.tsv

# パイプライン使用
cat query.fasta | af_kmersearch --db=mydb stdin stdout > results.tsv
```

### af_kmerdbinfo

af_kmersearchデータベースのメタデータ情報を表示します。

#### 使用方法
```bash
af_kmerdbinfo [オプション] データベース名
```

#### オプション
- `--host=HOST` - PostgreSQLサーバホスト（デフォルト: $PGHOST または localhost）
- `--port=PORT` - PostgreSQLサーバポート（デフォルト: $PGPORT または 5432）
- `--username=USER` - PostgreSQLユーザー名（デフォルト: $PGUSER または現在のユーザー）
- `--help, -h` - ヘルプメッセージを表示

#### 出力
- 全ての出力はSTDERRに書き込まれます
- バージョン、最小長、重複長を表示
- 総配列数と総文字数を表示
- パーティション情報と配列数・文字数を一覧表示

#### 使用例
```bash
# 基本的な使用方法
af_kmerdbinfo mydb

# リモートデータベース
af_kmerdbinfo --host=remote-server mydb

# カスタム接続パラメータ
af_kmerdbinfo --host=localhost --port=5433 --username=postgres mydb
```

### af_kmersearchclient

非同期ジョブ処理、負荷分散機能、リトライロジック付きリモートk-mer検索クライアント。

#### 使用方法
```bash
# 新しいジョブ実行
af_kmersearchclient [オプション] 入力ファイル名 出力ファイル名

# 既存ジョブの再開
af_kmersearchclient --resume=ジョブID

# 既存ジョブのキャンセル
af_kmersearchclient --cancel=ジョブID

# アクティブジョブ一覧
af_kmersearchclient --jobs
```

#### オプション
- `--server=SERVERS` - サーバURL（単一サーバまたはカンマ区切りリスト）
- `--serverlist=FILE` - サーバURLを記載したファイル（1行に1つ）
- `--db=DATABASE` - PostgreSQLデータベース名（サーバーにデフォルト設定があればオプション）
- `--partition=NAME` - 特定のパーティションに検索を限定（オプション）
- `--maxnseq=INT` - クエリあたりの最大結果数（デフォルト: 1000）
- `--minscore=INT` - 最小スコア閾値（オプション）
- `--numthreads=INT` - 並列スレッド数（デフォルト: 1）
- `--maxnretry=INT` - ステータス確認の最大リトライ数（デフォルト: 0 = 無制限）
- `--maxnretry_total=INT` - 全操作の最大総リトライ数（デフォルト: 100）
- `--retrydelay=INT` - リトライ遅延秒数（デフォルト: 10）
- `--failedserverexclusion=INT` - 失敗サーバの除外時間（秒、デフォルト: 無限）
- `--netrc-file=FILE` - .netrc形式認証情報ファイルを読み込み
- `--http-user=USER` - HTTP Basic認証ユーザー名（--http-passwordが必須）
- `--http-password=PASS` - HTTP Basic認証パスワード（--http-userが必須）
- `--resume=ジョブID` - 以前に投入したジョブを再開
- `--cancel=ジョブID` - ジョブをキャンセルし関連データを全て削除
- `--jobs` - アクティブジョブを全て表示
- `--help, -h` - ヘルプメッセージを表示

#### 入出力ファイル
- 入力: マルチFASTA形式、標準入力の場合は `-`、`stdin`、または `STDIN` を使用
- 出力: TSV形式、標準出力の場合は `-`、`stdout`、または `STDOUT` を使用

#### 出力形式
4つのカラムを持つタブ区切り値：
1. クエリ配列番号（1ベース）
2. クエリFASTAラベル
3. サーバからのCORRECTEDSCORE
4. カンマ区切りのseqidリスト

#### 認証
HTTP Basic認証で保護されたサーバに対しては、以下のオプションのいずれかを使用：

**1. .netrcファイル（複数サーバの場合推奨）:**
```bash
af_kmersearchclient --netrc-file=/path/to/netrc --server=https://server.com --db=mydb query.fasta results.tsv
```

.netrc形式:
```
machine hostname.example.com
login myusername
password mypassword

machine server2.example.com
login otherusername
password otherpassword
```

**2. コマンドライン認証情報（全サーバ共通）:**
```bash
af_kmersearchclient --http-user=myusername --http-password=mypassword --server=https://server.com --db=mydb query.fasta results.tsv
```

**3. 両オプション併用（フォールバック動作）:**
.netrcファイルの特定ホスト設定を優先し、.netrcに記載されていないサーバにはコマンドライン認証情報をフォールバックとして使用。

#### サーバURL形式
- `hostname` → `http://hostname:8080/search`
- `hostname:9090` → `http://hostname:9090/search`
- `192.168.1.100` → `http://192.168.1.100:8080/search`
- `http://server/api/search` → そのまま使用
- `https://server/search` → そのまま使用

#### 非同期ジョブ処理

クライアントは自動ポーリング機能付きの非同期ジョブ処理をサポートします：

- **ジョブ永続化**: ジョブは`.af_kmersearchclient`ファイルに保存され、再開機能を提供
- **自動ポーリング**: 適応的間隔を使用（5秒 → 10秒 → 20秒 → 30秒 → 60秒）
- **再開サポート**: `--resume=ジョブID`で中断されたジョブを再開可能
- **キャンセルサポート**: `--cancel=ジョブID`で実行中のジョブをキャンセル可能
- **ジョブ管理**: `--jobs`でアクティブジョブを一覧表示

#### 使用例
```bash
# 非同期処理を使用した基本的な使用方法
af_kmersearchclient --server=localhost --db=mydb query.fasta results.tsv

# 負荷分散を使用した複数サーバ
af_kmersearchclient --server="server1,server2,server3" --db=mydb query.fasta results.tsv

# サーバリストファイル
af_kmersearchclient --serverlist=servers.txt --db=mydb query.fasta results.tsv

# 認証を使用（.netrcファイル）
af_kmersearchclient --server=https://server.com --db=mydb --netrc-file=.netrc query.fasta results.tsv

# 認証を使用（コマンドライン）
af_kmersearchclient --server=https://server.com --db=mydb --http-user=myuser --http-password=mypass query.fasta results.tsv

# 並列処理とリトライ
af_kmersearchclient --server=localhost --db=mydb --numthreads=4 --maxnretry=10 query.fasta results.tsv

# パイプライン使用
cat query.fasta | af_kmersearchclient --server=localhost --db=mydb stdin stdout > results.tsv

# ジョブ管理例
af_kmersearchclient --jobs                                    # アクティブジョブ一覧
af_kmersearchclient --resume=20250703T120000-AbCdEf123456     # ジョブ再開
af_kmersearchclient --cancel=20250703T120000-AbCdEf123456     # ジョブキャンセル
```

### af_kmersearchserver.pl

k-mer検索用REST APIサーバ（スタンドアロンHTTPサーバ）。

#### 使用方法
```bash
perl af_kmersearchserver.pl [オプション]
```

#### オプション
- `--listen-port=PORT` - HTTPサーバリスンポート（デフォルト: 8080）
- `--numthreads=INT` - 並列リクエスト処理スレッド数（デフォルト: 5）

#### 設定
スクリプトヘッダーでデフォルト値を編集：
```perl
my $default_database = 'mykmersearch';  # デフォルトデータベース名
my $default_partition = 'bacteria';     # デフォルトパーティション名
my $default_maxnseq = 1000;             # デフォルト最大結果数
my $default_minscore = '10';            # デフォルト最小スコア
my $default_numthreads = 5;             # 並列スレッド数
```

#### APIエンドポイント

**POST /search** - 非同期k-mer配列検索ジョブ投入

リクエストJSON:
```json
{
  "querylabel": "配列名",
  "queryseq": "ATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCG",
  "db": "データベース名",
  "partition": "パーティション名",
  "maxnseq": 1000,
  "minscore": 10
}
```

レスポンスJSON（ジョブ投入成功）:
```json
{
  "job_id": "20250703T120000-AbCdEfGhIjKlMnOpQrStUvWxYz012345",
  "status": "running",
  "message": "Job submitted successfully"
}
```

**POST /result** - ジョブ結果取得（一回限り、取得後にジョブ削除）

リクエストJSON:
```json
{
  "job_id": "20250703T120000-AbCdEfGhIjKlMnOpQrStUvWxYz012345"
}
```

レスポンスJSON（完了時）:
```json
{
  "job_id": "20250703T120000-AbCdEfGhIjKlMnOpQrStUvWxYz012345",
  "status": "completed",
  "querylabel": "配列名",
  "queryseq": "ATCGATCG...",
  "results": [
    {
      "correctedscore": 95,
      "seqid": ["AB123:1:100", "CD456:50:150"]
    }
  ]
}
```

**POST /status** - ジョブ状態確認（非破壊的、監視用）

リクエストJSON:
```json
{
  "job_id": "20250703T120000-AbCdEfGhIjKlMnOpQrStUvWxYz012345"
}
```

レスポンスJSON（実行中）:
```json
{
  "job_id": "20250703T120000-AbCdEfGhIjKlMnOpQrStUvWxYz012345",
  "status": "running",
  "message": "Job is still processing"
}
```

**POST /cancel** - 実行中ジョブのキャンセルと関連データ削除

リクエストJSON:
```json
{
  "job_id": "20250703T120000-AbCdEfGhIjKlMnOpQrStUvWxYz012345"
}
```

レスポンスJSON:
```json
{
  "status": "cancelled",
  "message": "Job has been cancelled and removed"
}
```

#### 使用例
```bash
# サーバ起動
perl af_kmersearchserver.pl --listen-port=8080

# カスタムスレッド数でサーバ起動
perl af_kmersearchserver.pl --listen-port=8080 --numthreads=10

# API呼び出し
curl -X POST http://localhost:8080/search \
  -H "Content-Type: application/json" \
  -d '{
    "querylabel": "test_sequence",
    "queryseq": "ATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCG",
    "db": "mydb"
  }'
```

### af_kmersearchserver.fcgi

本番Webサーバ（NGINX/Apache）用FastCGI版。

#### 使用方法
```bash
perl af_kmersearchserver.fcgi [オプション]
```

#### オプション
- `--numthreads=NUM` - FastCGIプロセス数（デフォルト: 5）

#### 設定
af_kmersearchserver.plと同様 - スクリプトヘッダーでデフォルト値を編集。

#### NGINX設定
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

#### Apache設定
```apache
<VirtualHost *:80>
    ServerName your-domain.com
    
    ScriptAlias /api/search /path/to/af_kmersearchweb_fcgi.pl
    
    <Directory "/path/to/">
        SetHandler fcgid-script
        Options +ExecCGI
        Require all granted
    </Directory>
</VirtualHost>
```

#### プロセス管理
```bash
# FastCGIプロセス起動
spawn-fcgi -s /var/run/af_kmersearch.sock -U nginx -G nginx \
           -u www-data -g www-data -P /var/run/af_kmersearch.pid \
           -- perl af_kmersearchserver.fcgi --numthreads=5
```

### af_kmersearchserver.psgi

様々なPSGIサーバでのモダンなWebデプロイ用PSGI版。

#### 使用方法
```bash
perl af_kmersearchserver.psgi [オプション]
```

#### オプション
- `--host=HOST` - PostgreSQLサーバホスト（デフォルト: $PGHOST または localhost）
- `--port=PORT` - PostgreSQLサーバポート（デフォルト: $PGPORT または 5432）
- `--username=USER` - PostgreSQLユーザー名（デフォルト: $PGUSER または現在のユーザー）
- `--listen-port=PORT` - HTTPサーバリスンポート（デフォルト: 5000）
- `--workers=NUM` - ワーカープロセス数（デフォルト: 5）
- `--help, -h` - ヘルプメッセージを表示

#### 設定
af_kmersearchserver.plと同様 - スクリプトヘッダーでデフォルト値を編集。

#### デプロイオプション
```bash
# スタンドアローン（内蔵Starmanサーバ）
perl af_kmersearchserver.psgi

# plackupでの起動
plackup -p 5000 --workers 10 af_kmersearchserver.psgi

# その他のPSGIサーバ
starman --port 5000 --workers 10 af_kmersearchserver.psgi
uwsgi --http :5000 --psgi af_kmersearchserver.psgi
```

#### 使用例
```bash
perl af_kmersearchserver.psgi
perl af_kmersearchserver.psgi --listen-port=8080 --workers=10
plackup -p 8080 --workers 20 af_kmersearchserver.psgi
```

## ワークフロー例

### 完全なデータベースセットアップと検索

1. **データベース作成と配列格納:**
   ```bash
   af_kmerstore --partition=bacteria sequences.fasta mydb
   ```

2. **パーティション情報追加:**
   ```bash
   af_kmerpart --partition=pathogenic bacteria_ids.txt mydb
   ```

3. **インデックス作成:**
   ```bash
   af_kmerindex --mode=create mydb
   ```

4. **データベース情報確認:**
   ```bash
   af_kmerdbinfo mydb
   ```

5. **配列検索:**
   ```bash
   af_kmersearch --db=mydb --partition=pathogenic query.fasta results.tsv
   ```

### Web APIデプロイメント

1. **デフォルト値設定:**
   ```perl
   # af_kmersearchserver.fcgiを編集
   my $default_database = 'mydb';
   my $default_partition = 'bacteria';
   ```

2. **NGINXでのデプロイ:**
   ```bash
   spawn-fcgi -s /var/run/af_kmersearch.sock \
              -- perl af_kmersearchserver.fcgi --numthreads=5
   ```

3. **API経由での検索:**
   ```bash
   curl -X POST http://your-domain.com/api/search \
        -H "Content-Type: application/json" \
        -d '{"querylabel": "test", "queryseq": "ATCG..."}'
   ```

## パフォーマンスのヒント

- CPUコア数に基づいて適切な`--numthreads`を使用
- 大量データロード後にインデックスを作成
- 大規模データセットにはパーティションを使用
- `--tablespace`を使用してインデックスを高速ストレージ（SSD）に配置
- Web APIでは適切なFastCGIプロセス数を設定

## トラブルシューティング

- pg_kmersearch拡張がインストールされていることを確認
- PostgreSQL接続パラメータを確認
- 配列長を確認（検索には最低64塩基が必要）
- FastCGIデプロイメント時のファイル権限を確認
- PostgreSQLログで接続問題を監視

## ライセンス

オープンソースソフトウェア。詳細は各スクリプトのヘッダーを参照。