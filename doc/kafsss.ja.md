# kafsss: K-mer based Alignment-Free Splitted Sequence Search

pg_kmersearch拡張を使用してPostgreSQLでDNA配列の保存、管理、検索を行う包括的なツールキット。

## 概要

kafsss suiteは、k-mer類似性検索を使用したDNA配列解析の完全なソリューションを提供します。ツールキットは、DNA配列管理、検索操作、非同期ジョブ処理によるサーバーデプロイメントの異なる側面を処理するPerlスクリプトで構成されています。

## 前提条件

- PostgreSQL 9.1以降
- pg_kmersearch拡張がインストール済みで利用可能
- Perl 5.10以降
- 適切な権限を持つPostgreSQLユーザー（セットアップ部分参照）

### 必要なPerlモジュール

#### コアデータベースツール（kafssstore, kafssindex, kafsssearch, kafsssubset, kafssdbinfo, kafssdedup, kafssfreq）
- `DBI` - データベースアクセスインターフェース
- `DBD::Pg` - PostgreSQLドライバ
- `Getopt::Long` - コマンドライン引数解析
- `POSIX` - POSIXシステム機能
- `File::Basename` - ファイル名操作
- `Sys::Hostname` - システムホスト名取得

#### ネットワーククライアント（kafsssearchclient）
コアモジュール（上記）に加えて：
- `JSON` - JSON形式の処理
- `LWP::UserAgent` - HTTPクライアント
- `HTTP::Request::Common` - HTTPリクエスト生成
- `URI` - URI解析とエンコーディング
- `MIME::Base64` - Base64エンコード/デコード
- `Time::HiRes` - 高解像度時間関数
- `Fcntl` - ファイル制御操作

#### PSGIサーバ（kafsssearchserver.psgi）
コアモジュールに加えて：
- `JSON` - JSON形式の処理
- `Plack::Request` - PSGIリクエスト処理
- `Plack::Response` - PSGIレスポンス処理
- `Plack::Builder` - PSGIミドルウェア構成
- `Plack::Handler::Starman` - Starman HTTPサーバ
- `MIME::Base64` - Base64エンコード/デコード
- `Time::HiRes` - 高解像度時間関数
- `Fcntl` - ファイル制御操作
- `DBD::SQLite` - SQLiteドライバ（ジョブ管理用）
- `Crypt::OpenSSL::Random` - 暗号学的に安全な乱数

### データベースセットアップ

kafsssツールを使用する前に、PostgreSQLを適切に設定する必要があります：

#### 1. PostgreSQLとpg_kmersearch拡張のインストール
```bash
sudo apt-get install postgresql postgresql-contrib
# pg_kmersearch拡張パッケージをインストール（システム管理者にお問い合わせください）
```

#### 2. PostgreSQLユーザーとデータベースの作成
```bash
sudo -u postgres psql
CREATE USER yourusername CREATEDB;
ALTER USER yourusername PASSWORD 'yourpassword';
\q
```

#### 3. 環境変数の設定
```bash
export PGUSER=yourusername
export PGPASSWORD=yourpassword
export PGHOST=localhost
export PGPORT=5432
```

#### 4. データベースに拡張を作成
**オプションA: スーパーユーザーが拡張を作成（推奨）**
```bash
sudo -u postgres psql -d your_database
CREATE EXTENSION IF NOT EXISTS pg_kmersearch;
\q
```

**オプションB: 一時的にスーパーユーザー権限を付与**
```bash
sudo -u postgres psql
ALTER USER yourusername SUPERUSER;
\q
# kafssstoreを実行後、権限を取り消す：
# ALTER USER yourusername NOSUPERUSER;
```

### 依存関係のインストール

#### コアデータベースツール用のみ
**Ubuntu/Debian:**
```bash
sudo apt-get update
sudo apt-get install -y perl libdbi-perl libdbd-pg-perl

# cpanminusを使用
sudo apt-get install -y cpanminus
sudo cpanm DBI DBD::Pg Getopt::Long POSIX File::Basename Sys::Hostname
```

**RHEL/CentOS/Fedora:**
```bash
sudo yum install -y perl perl-DBI perl-DBD-Pg
# または: sudo dnf install -y perl perl-DBI perl-DBD-Pg

# cpanminusを使用
sudo yum install -y perl-App-cpanminus  # または dnf
sudo cpanm DBI DBD::Pg Getopt::Long POSIX File::Basename Sys::Hostname
```

#### ネットワーククライアント用（kafsssearchclient）
**Ubuntu/Debian:**
```bash
sudo apt-get install -y \
    perl libdbi-perl libdbd-pg-perl libjson-perl \
    libwww-perl liburi-perl libdbd-sqlite3-perl \
    libcrypt-openssl-random-perl

# cpanminusを使用
sudo cpanm DBI DBD::Pg JSON LWP::UserAgent HTTP::Request::Common URI \
           MIME::Base64 Time::HiRes Fcntl DBD::SQLite Crypt::OpenSSL::Random
```

**RHEL/CentOS/Fedora:**
```bash
sudo yum install -y perl perl-DBI perl-DBD-Pg perl-JSON \
                    perl-libwww-perl perl-URI perl-DBD-SQLite
# または dnf を使用

# cpanminusを使用
sudo cpanm DBI DBD::Pg JSON LWP::UserAgent HTTP::Request::Common URI \
           MIME::Base64 Time::HiRes Fcntl DBD::SQLite Crypt::OpenSSL::Random
```

#### PSGIサーバ用（kafsssearchserver.psgi）
**Ubuntu/Debian:**
```bash
sudo apt-get install -y \
    perl libdbi-perl libdbd-pg-perl libjson-perl \
    libplack-perl starman libdbd-sqlite3-perl \
    libcrypt-openssl-random-perl

# cpanminusを使用
sudo cpanm DBI DBD::Pg JSON Plack::Request Plack::Response Plack::Builder \
           Plack::Handler::Starman MIME::Base64 Time::HiRes Fcntl \
           DBD::SQLite Crypt::OpenSSL::Random
```

**RHEL/CentOS/Fedora:**
```bash
sudo yum install -y perl perl-DBI perl-DBD-Pg perl-JSON \
                    perl-Plack perl-DBD-SQLite
# または dnf を使用

# cpanminusを使用
sudo cpanm DBI DBD::Pg JSON Plack::Request Plack::Response Plack::Builder \
           Plack::Handler::Starman MIME::Base64 Time::HiRes Fcntl \
           DBD::SQLite Crypt::OpenSSL::Random
```

#### 手動インストール（CPAN使用）

**コアデータベースツール用:**
```bash
perl -MCPAN -e 'install DBI, DBD::Pg, Getopt::Long, POSIX, File::Basename, Sys::Hostname'
```

**ネットワーククライアント用（kafsssearchclient）:**
```bash
perl -MCPAN -e 'install DBI, DBD::Pg, JSON, LWP::UserAgent, HTTP::Request::Common, URI, MIME::Base64, Time::HiRes, Fcntl, DBD::SQLite, Crypt::OpenSSL::Random'
```

**PSGIサーバ用（kafsssearchserver.psgi）:**
```bash
perl -MCPAN -e 'install DBI, DBD::Pg, JSON, Plack::Request, Plack::Response, Plack::Builder, Plack::Handler::Starman, MIME::Base64, Time::HiRes, Fcntl, DBD::SQLite, Crypt::OpenSSL::Random'
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
perl -MPlack -e 'print "Plack available\n"'
perl -MStarman -e 'print "Starman available\n"'
```

## スクリプト概要

| スクリプト | 用途 |
|-----------|------|
| `kafssstore` | FASTA配列をPostgreSQLデータベースに格納 |
| `kafsssubset` | 配列のサブセット情報を追加・削除 |
| `kafssindex` | 配列データのGINインデックスを作成/削除 |
| `kafsssearch` | k-mer類似性を使用した配列検索 |
| `kafssdbinfo` | データベースメタデータ情報を表示 |
| `kafssdedup` | データベース内の配列の重複除去 |
| `kafsspart` | パフォーマンス向上のためのkafsss_dataテーブルのパーティション化 |
| `kafssfreq` | K-mer頻度分析 |
| `kafsssearchclient` | 負荷分散機能付きリモートk-mer検索クライアント |
| `kafsssearchserver.psgi` | k-mer検索用PSGIサーバ（スタンドアローン、FastCGI、各種デプロイに対応） |
| `calcsegment` | 配列分割パラメータ計算用数学ユーティリティ |

## インストール

### makeを使用する方法（推奨）

1. PostgreSQLにpg_kmersearch拡張をインストール
2. コマンドラインツールをインストール：
   ```bash
   make
   sudo make install
   
   # カスタムインストールプレフィックス
   make PREFIX=/opt/kafsssearch
   sudo make install PREFIX=/opt/kafsssearch
   ```

**注意**: サーバスクリプトは`installserver`ターゲットで別途インストールします：
```bash
make installserver DESTDIR=/var/www/kafsss
```

### 手動インストール

1. PostgreSQLにpg_kmersearch拡張をインストール
2. スクリプトを実行可能にする：
   ```bash
   chmod +x kafss*.pl
   ```

## データベース接続

すべてのスクリプトはPostgreSQL接続オプションをサポート：

- `--host=HOST` - PostgreSQLサーバホスト（デフォルト: $PGHOST または localhost）
- `--port=PORT` - PostgreSQLサーバポート（デフォルト: $PGPORT または 5432）
- `--username=USER` - PostgreSQLユーザー名（デフォルト: $PGUSER または現在のユーザー）

パスワードは`PGPASSWORD`環境変数から読み取られます。

## スクリプトドキュメント

### kafssstore

マルチFASTA DNA配列をPostgreSQLデータベースに格納します。

#### 使用方法
```bash
kafssstore [オプション] 入力ファイル名 出力データベース名
```

#### オプション
- `--datatype=DNA2|DNA4` - データ型（デフォルト: DNA4）
- `--minlen=INT` - 分割用最小配列長（デフォルト: 50000）
- `--ovllen=INT` - 分割配列間の重複長（デフォルト: 500）。重複の競合を防ぐため`--minsplitlen`の半分未満である必要があります
- `--numthreads=INT` - 並列スレッド数（デフォルト: 1）
- `--subset=NAME` - サブセット名（複数指定可能）
- `--tablespace=NAME` - CREATE DATABASE用テーブルスペース名
- `--overwrite` - 既存データベースを上書き

#### 入力ファイル
- マルチFASTA形式のDNA配列
- 標準入力の場合は `-`、`stdin`、または `STDIN` を使用

#### 使用例
```bash
# 基本的な使用方法
kafssstore sequences.fasta mydb

# サブセットと並列処理を使用
kafssstore --subset=bacteria --numthreads=4 sequences.fasta mydb

# 標準入力から
cat sequences.fasta | kafssstore stdin mydb

# カスタムパラメータ
kafssstore --datatype=DNA2 --minlen=100000 sequences.fasta mydb
```

### kafssdedup

**目的**: kafsss_dataテーブルから重複配列を削除します。

**使用方法**: `kafssdedup [オプション] データベース名`

**オプション**:
- `--host=HOST` - PostgreSQLサーバーホスト
- `--port=PORT` - PostgreSQLサーバーポート
- `--username=USER` - PostgreSQLユーザー名
- `--workingmemory=SIZE` - 重複除去用作業メモリ（デフォルト: 8GB）
- `--maintenanceworkingmemory=SIZE` - メンテナンス作業メモリ（デフォルト: 8GB）
- `--temporarybuffer=SIZE` - 一時バッファサイズ（デフォルト: 512MB）
- `--verbose` - 詳細処理メッセージを表示
- `--help` - ヘルプメッセージを表示

**例**:
```bash
# 基本的な重複除去
kafssdedup mydb

# カスタムメモリ設定
kafssdedup --workingmemory=32GB mydb
```

### kafsspart

**目的**: pg_kmersearchのパーティション関数を使用してkafsss_dataテーブルのパーティション化またはアンパーティション化を行い、パフォーマンスを向上させます。

**使用方法**: `kafsspart [オプション] データベース名`

**必須オプション**:
- `--npart=INT` - パーティション数（1以上）
  - `--npart=1`: アンパーティション（パーティションテーブルを通常テーブルに戻す）
  - `--npart=2`以上: 指定された数のパーティションに分割

**任意オプション**:
- `--host=HOST` - データベースサーバーホスト
- `--port=PORT` - データベースサーバーポート
- `--username=USER` - データベースユーザー名
- `--tablespace=NAME` - パーティション用テーブルスペース名
- `--verbose` - 詳細出力を有効化
- `--help` - ヘルプメッセージを表示

**例**:
```bash
# 16パーティションに分割
kafsspart --npart=16 mydb

# 特定のテーブルスペースを使用
kafsspart --npart=32 --tablespace=fast_ssd mydb

# アンパーティション（通常テーブルに戻す）
kafsspart --npart=1 mydb
```

**注意事項**:
- パーティション化/アンパーティション化の前にseqカラムのGINインデックスを削除する必要があります
- アンパーティション後、kafssindexでインデックスを再作成してください

### kafssfreq

**目的**: kafsss_dataテーブルの高頻度k-mer解析を実行します。

**使用方法**: `kafssfreq [オプション] データベース名`

**必須オプション**:
- `--mode=MODE` - 操作モード: 'create' または 'drop'

**任意オプション**:
- `--host=HOST` - PostgreSQLサーバーホスト
- `--port=PORT` - PostgreSQLサーバーポート
- `--username=USER` - PostgreSQLユーザー名
- `--kmersize=INT` - 解析用k-mer長（デフォルト: 8、範囲: 4-64）
- `--maxpappear=REAL` - 最大k-mer出現率（デフォルト: 0.5、範囲: 0.0-1.0、小数点以下3桁まで）
- `--maxnappear=INT` - k-merを含む最大行数（デフォルト: 0=無制限）
- `--occurbitlen=INT` - 出現カウント用ビット数（デフォルト: 8、範囲: 0-16）
- `--numthreads=INT` - 並列ワーカー数（デフォルト: 0=自動）
- `--workingmemory=SIZE` - 各操作の作業メモリ（デフォルト: 8GB）
- `--maintenanceworkingmemory=SIZE` - メンテナンス作業メモリ（デフォルト: 8GB）
- `--temporarybuffer=SIZE` - 一時バッファサイズ（デフォルト: 512MB）
- `--verbose` - 詳細処理メッセージを表示
- `--overwrite` - 既存の解析を上書き（--mode=createの場合のみ）
- `--help` - ヘルプメッセージを表示

**例**:
```bash
# 頻度解析を作成
kafssfreq --mode=create mydb

# カスタムパラメータで作成
kafssfreq --mode=create --kmersize=16 --numthreads=32 mydb

# 頻度解析を削除
kafssfreq --mode=drop mydb
```

### kafsssubset

アクセッション番号に基づく配列またはデータベース全体に対してサブセット情報の追加・削除を行います。

#### 使用方法
```bash
kafsssubset [オプション] 入力ファイル名 データベース名
```

#### オプション
- `--mode=MODE` - 動作モード: `add`（デフォルト）または `del`
- `--subset=NAME` - 追加/削除するサブセット名（必須、複数指定可能）
  - delモード時のみ `all` を指定して全サブセットを対象にできます
- `--numthreads=INT` - 並列スレッド数（デフォルト: 1）

#### 入力ファイル
- 1行に1つのアクセッション番号を記載したプレーンテキストファイル
- 標準入力の場合は `-`、`stdin`、または `STDIN` を使用
- データベースの全行を対象にする場合は `all` を使用
- `#`で始まる行はコメントとして扱われます

#### 使用例
```bash
# 配列にサブセットを追加
kafsssubset --subset=bacteria accessions.txt mydb
kafsssubset --subset=bacteria,archaea accessions.txt mydb

# 配列からサブセットを削除
kafsssubset --mode=del --subset=bacteria accessions.txt mydb

# 全行から全サブセットを削除
kafsssubset --mode=del --subset=all all mydb

# 全行から特定サブセットを削除
kafsssubset --mode=del --subset=archaea all mydb

# 標準入力から
echo -e "AB123456\nCD789012" | kafsssubset --subset=bacteria stdin mydb
```

#### 注意事項
- addモード時にサブセット名 `all` の使用は禁止されています
- 入力ファイル名に `all` を指定すると、データベースの全行が対象になります
- `--subset=all` をdelモード時に使用すると、全サブセット情報が削除されます

### kafssindex

配列データのGINインデックスを作成または削除します。

#### 使用方法
```bash
kafssindex [オプション] データベース名
```

#### オプション
- `--mode=create|drop` - 操作モード（必須）
- `--tablespace=NAME` - CREATE INDEX用テーブルスペース名

#### 使用例
```bash
# インデックス作成
kafssindex --mode=create mydb

# 特定のテーブルスペースにインデックス作成
kafssindex --mode=create --tablespace=fast_ssd mydb

# インデックス削除
kafssindex --mode=drop mydb
```

### kafsspreload

**目的**: 高頻度k-merキャッシュをメモリにプリロードして高速化します。

**使用方法**: `kafsspreload [オプション] データベース名`

**オプション**:
- `--host=HOST` - PostgreSQLサーバーホスト
- `--port=PORT` - PostgreSQLサーバーポート
- `--username=USER` - PostgreSQLユーザー名
- `--verbose` - 詳細出力を有効化
- `--help` - ヘルプメッセージを表示

**注意**:
- データベース接続を維持するデーモンプロセスとして動作
- 1時間ごとに変更を監視し、変更を検出すると正常終了
- 動作中はkafssindexの構築やkafsssearchの操作が高速化
- pg_kmersearch拡張とkafssfreqの実行が必要

**例**:
```bash
# キャッシュをプリロード（デーモンとして動作）
kafsspreload mydb

# 詳細ログ付き
kafsspreload --verbose mydb
```

### kafsssearch

k-mer類似性を使用してDNA配列を検索します。

#### 使用方法
```bash
kafsssearch [オプション] 入力ファイル名 出力ファイル名
```

#### オプション
- `--db=DATABASE` - PostgreSQLデータベース名（必須）
- `--subset=NAME` - 特定のサブセットに検索を限定
- `--maxnseq=INT` - クエリあたりの最大結果数（デフォルト: 1000、0=無制限）
- `--minscore=INT` - 最小スコア閾値（デフォルト: 1）
- `--minpsharedkmer=REAL` - 共有k-merの最小割合（0.0-1.0、デフォルト: 0.5）
- `--mode=MODE` - 出力モード: minimum (min)、matchscore (score)、sequence (seq)、maximum (max)（デフォルト: matchscore）
- `--outfmt=FORMAT` - 出力形式: TSV（デフォルト）、multiTSV、FASTA、multiFASTA、BLASTDB。圧縮接尾辞（.gz、.bz2、.xz、.zst）対応
- `--numthreads=INT` - 並列スレッド数（デフォルト: 1）

**GINインデックス選択**（複数インデックスがあるデータベースの場合）：
- `--kmersize=INT` - 一致するkmer_sizeのインデックスを選択
- `--occurbitlen=INT` - 一致するoccur_bitlenのインデックスを選択
- `--maxpappear=REAL` - 一致するmax_appearance_rateのインデックスを選択（最大3桁の小数）
- `--maxnappear=INT` - 一致するmax_appearance_nrowのインデックスを選択
- `--precludehighfreqkmer` - preclude_highfreq_kmer=trueのインデックスを選択

データベースにGINインデックスが1つしかない場合は自動的に選択されます。複数存在する場合は、パラメータを指定して一意に識別してください。

#### 入出力ファイル
- 入力: マルチFASTA形式、標準入力の場合は `-`、`stdin`、または `STDIN`
- 出力: TSV形式、標準出力の場合は `-`、`stdout`、または `STDOUT`

#### 出力形式
タブ区切り値（モードにより可変）：
1. クエリ配列番号（1ベース）
2. クエリFASTAラベル
3. seqidカラムからのカンマ区切りseqidリスト
4. kmersearch_matchscore関数からのマッチスコア（matchscoreおよびmaximumモードのみ）
5. 配列データ（sequenceおよびmaximumモードのみ）

#### 使用例
```bash
# 基本的な検索
kafsssearch --db=mydb query.fasta results.tsv

# サブセットフィルタを使用した検索
kafsssearch --db=mydb --subset=bacteria query.fasta results.tsv

# カスタムパラメータを使用した並列検索
kafsssearch --db=mydb --numthreads=4 --maxnseq=500 query.fasta results.tsv

# パイプライン使用
cat query.fasta | kafsssearch --db=mydb stdin stdout > results.tsv

# 検索結果からBLASTデータベースを作成
kafsssearch --db=mydb --outfmt=BLASTDB query.fasta results

# 圧縮出力
kafsssearch --db=mydb --outfmt=TSV.gz query.fasta results.tsv.gz

# 複数GINインデックス - パラメータを指定してインデックスを選択
kafsssearch --db=mydb --kmersize=8 query.fasta results.tsv
kafsssearch --db=mydb --kmersize=8 --precludehighfreqkmer query.fasta results.tsv
```

#### 圧縮ツール
出力圧縮には以下の外部ツールを使用します（事前にインストールが必要）：
- `.gz` - `pigz`を使用（並列gzip）
- `.bz2` - `pbzip2`を使用（並列bzip2）
- `.xz` - `xz`を使用
- `.zst` - `zstd`を使用

### kafssdbinfo

kafsssearchデータベースのメタデータ情報を表示します。

#### 使用方法
```bash
kafssdbinfo [オプション] データベース名
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
- サブセット情報と配列数・文字数を一覧表示

#### 使用例
```bash
# 基本的な使用方法
kafssdbinfo mydb

# リモートデータベース
kafssdbinfo --host=remote-server mydb

# カスタム接続パラメータ
kafssdbinfo --host=localhost --port=5433 --username=postgres mydb
```

### kafsssearchclient

非同期ジョブ処理、負荷分散機能、リトライロジック付きリモートk-mer検索クライアント。

#### 使用方法
```bash
# 新しいジョブ実行
kafsssearchclient [オプション] 入力ファイル名 出力ファイル名

# 既存ジョブの再開
kafsssearchclient --resume=ジョブID

# 既存ジョブのキャンセル
kafsssearchclient --cancel=ジョブID

# アクティブジョブ一覧
kafsssearchclient --jobs
```

#### オプション
- `--server=SERVERS` - サーバURL（単一サーバまたはカンマ区切りリスト）
- `--serverlist=FILE` - サーバURLを記載したファイル（1行に1つ）
- `--db=DATABASE` - PostgreSQLデータベース名（サーバーにデフォルト設定があればオプション）
- `--subset=NAME` - 特定のサブセットに検索を限定（オプション）
- `--maxnseq=INT` - クエリあたりの最大結果数（デフォルト: 1000、0=無制限）
- `--minscore=INT` - 最小スコア閾値（デフォルト: 1）
- `--minpsharedkmer=REAL` - 共有k-merの最小割合（0.0-1.0、デフォルト: 0.5）
- `--mode=MODE` - 出力モード: minimum (min)、matchscore (score)、sequence (seq)、maximum (max)（デフォルト: matchscore）
- `--outfmt=FORMAT` - 出力形式: TSV（デフォルト）、multiTSV、FASTA、multiFASTA、BLASTDB。圧縮接尾辞（.gz、.bz2、.xz、.zst）対応
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
kafsssearchclient --netrc-file=/path/to/netrc --server=https://server.com --db=mydb query.fasta results.tsv
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
kafsssearchclient --http-user=myusername --http-password=mypassword --server=https://server.com --db=mydb query.fasta results.tsv
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

- **ジョブ永続化**: ジョブは`.kafsssearchclient`ファイルに保存され、再開機能を提供
- **自動ポーリング**: 適応的間隔を使用（5秒 → 10秒 → 20秒 → 30秒 → 60秒）
- **再開サポート**: `--resume=ジョブID`で中断されたジョブを再開可能
- **キャンセルサポート**: `--cancel=ジョブID`で実行中のジョブをキャンセル可能
- **ジョブ管理**: `--jobs`でアクティブジョブを一覧表示

#### 使用例
```bash
# 非同期処理を使用した基本的な使用方法
kafsssearchclient --server=localhost --db=mydb query.fasta results.tsv

# 負荷分散を使用した複数サーバ
kafsssearchclient --server="server1,server2,server3" --db=mydb query.fasta results.tsv

# サーバリストファイル
kafsssearchclient --serverlist=servers.txt --db=mydb query.fasta results.tsv

# 認証を使用（.netrcファイル）
kafsssearchclient --server=https://server.com --db=mydb --netrc-file=.netrc query.fasta results.tsv

# 認証を使用（コマンドライン）
kafsssearchclient --server=https://server.com --db=mydb --http-user=myuser --http-password=mypass query.fasta results.tsv

# 並列処理とリトライ
kafsssearchclient --server=localhost --db=mydb --numthreads=4 --maxnretry=10 query.fasta results.tsv

# パイプライン使用
cat query.fasta | kafsssearchclient --server=localhost --db=mydb stdin stdout > results.tsv

# ジョブ管理例
kafsssearchclient --jobs                                    # アクティブジョブ一覧
kafsssearchclient --resume=20250703T120000-AbCdEf123456     # ジョブ再開
kafsssearchclient --cancel=20250703T120000-AbCdEf123456     # ジョブキャンセル
```

### kafsssearchserver.psgi

非同期ジョブ処理機能付きk-mer検索用PSGIサーバ。スタンドアローン、FastCGI、各種デプロイ設定に対応。

#### 使用方法
```bash
perl kafsssearchserver.psgi [オプション]
```

#### オプション
- `--host=HOST` - PostgreSQLサーバホスト（デフォルト: $PGHOST または localhost）
- `--port=PORT` - PostgreSQLサーバポート（デフォルト: $PGPORT または 5432）
- `--username=USER` - PostgreSQLユーザー名（デフォルト: $PGUSER または現在のユーザー）
- `--listenport=PORT` - HTTPサーバリスンポート（デフォルト: 5000）
- `--numthreads=NUM` - ワーカープロセス数（デフォルト: 5）
- `--sqlitepath=PATH` - SQLiteデータベースファイルパス（デフォルト: ./kafsssearchserver.sqlite）
- `--cleanlimit=INT` - 結果保持期間（秒、デフォルト: 86400）
- `--jobtimeout=INT` - ジョブタイムアウト（秒、デフォルト: 1800）
- `--maxnjob=INT` - 最大同時ジョブ数（デフォルト: 10）
- `--cleaninterval=INT` - クリーンアップ間隔（秒、デフォルト: 300）
- `--help, -h` - ヘルプメッセージを表示

#### 設定
スクリプトヘッダーでデフォルト値を編集：
```perl
my $default_database = 'mykmersearch';  # デフォルトデータベース名
my $default_subset = 'bacteria';     # デフォルトサブセット名
my $default_maxnseq = 1000;             # デフォルト最大結果数（0=無制限）
my $default_minscore = 1;               # デフォルト最小スコア
my $default_minpsharedkmer = 0.5;       # デフォルト最小共有k-mer率
my $default_numthreads = 5;             # 並列スレッド数

# データベース設定 - 複数データベースサポート
my @available_databases = ('mykmersearch', 'otherdb');  # 利用可能なデータベース名の配列

# サブセット設定（形式："データベース名:サブセット名"）
my @available_subsets = ('mykmersearch:bacteria', 'mykmersearch:archaea');

# デフォルトGINインデックスパラメータ（すべて任意、インデックス選択に使用）
my $default_kmersize = '';           # デフォルトkmer_size値（空=未指定）
my $default_occurbitlen = '';        # デフォルトoccur_bitlen値
my $default_maxpappear = '';         # デフォルトmax_appearance_rate（小数点以下3桁まで）
my $default_maxnappear = '';         # デフォルトmax_appearance_nrow値
my $default_precludehighfreqkmer = '';  # デフォルトpreclude_highfreq_kmer（1、0、または空）
```

サーバー起動時に以下を検証：
- `@available_databases`内のすべてのデータベースに接続可能
- 各データベースに必要なテーブル（kafsss_data, kafsss_meta）が存在
- 各データベースに少なくとも1つのGINインデックスが存在
- `@available_subsets`で設定されたすべてのサブセットがそれぞれのデータベースに存在
- デフォルトデータベースとGINインデックスパラメータが一意にインデックスを識別

#### APIエンドポイント

**POST /search** - 非同期k-mer配列検索ジョブ投入

リクエストJSON:
```json
{
  "querylabel": "配列名",
  "queryseq": "ATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCG",
  "database": "データベース名",
  "subset": "サブセット名",
  "maxnseq": 1000,
  "minscore": 10,
  "minpsharedkmer": 0.5,
  "mode": "matchscore",
  "kmersize": 8,
  "occurbitlen": 8,
  "maxpappear": 0.050,
  "maxnappear": 0,
  "precludehighfreqkmer": 1
}
```

リクエストパラメータ：
- `queryseq`（必須）：クエリ配列（A/C/G/T/Uおよび縮重コードのみ）
- `querylabel`（任意）：クエリのラベル（デフォルト："queryseq"）
- `database`または`db`（任意）：データベース名（デフォルト：設定されたデフォルト値）
- `subset`（任意）：結果フィルタリング用のサブセット名
- `maxnseq`（任意）：最大結果数（デフォルト：設定されたデフォルト値、0=無制限）
- `minscore`（任意）：最小スコア閾値（デフォルト：設定されたデフォルト値）
- `minpsharedkmer`（任意）：最小共有k-mer率（デフォルト：0.5、範囲：0.0-1.0）
- `mode`（任意）：検索モード - `minimum`/`min`、`matchscore`/`score`、`sequence`/`seq`、`maximum`/`max`

**GINインデックス選択**（複数インデックスがあるデータベースの場合）：
- `index`（任意）：完全なGINインデックス名（例：`idx_kafsss_data_seq_gin_km8_ob8_mar0500_man0_phkT`）
- または個別パラメータを指定してマッチング：
  - `kmersize`：kmer_size値にマッチ
  - `occurbitlen`：occur_bitlen値にマッチ
  - `maxpappear`：max_appearance_rate値にマッチ（小数点以下3桁まで）
  - `maxnappear`：max_appearance_nrow値にマッチ
  - `precludehighfreqkmer`：preclude_highfreq_kmerにマッチ（1=true、0=false）

注意：`index`と個別パラメータを同時に指定することはできません。データベースにGINインデックスが1つしかない場合は自動的に選択されます。

レスポンスJSON（ジョブ投入成功）:
```json
{
  "success": true,
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

**GET /metadata** - サーバ設定および利用可能データベース情報取得

レスポンスJSON:
```json
{
  "success": true,
  "server_version": "0.1.2026.01.18",
  "default_database": "mykmersearch",
  "default_subset": "bacteria",
  "default_maxnseq": 1000,
  "default_minscore": 10,
  "default_kmersize": 8,
  "default_occurbitlen": 8,
  "default_maxpappear": 0.050,
  "default_maxnappear": 0,
  "default_precludehighfreqkmer": true,
  "available_databases": ["mykmersearch", "otherdb"],
  "available_subsets": ["mykmersearch:bacteria", "mykmersearch:archaea"],
  "available_indices": [
    "mykmersearch:idx_kafsss_data_seq_gin_km8_ob8_mar0500_man0_phkT",
    "otherdb:idx_kafsss_data_seq_gin_km8_ob8_mar0500_man0_phkT"
  ],
  "accept_gzip_request": true,
  "supported_endpoints": ["/search", "/result", "/status", "/cancel", "/metadata"]
}
```

注意：`default_kmersize`、`default_occurbitlen`、`default_maxpappear`、`default_maxnappear`、`default_precludehighfreqkmer`、`default_subset`は設定されている場合のみ含まれます。

#### デプロイオプション
```bash
# スタンドアローン（内蔵Starmanサーバ）
perl kafsssearchserver.psgi

# plackupでの起動（HTTP）
plackup -p 5000 --workers 10 kafsssearchserver.psgi

# plackupでの起動（FastCGI、Unixソケット経由）
plackup -s FCGI --listen /var/run/kafsss.sock --nproc 10 kafsssearchserver.psgi

# plackupでの起動（FastCGI、TCPポート経由）
plackup -s FCGI --listen :9000 --nproc 10 kafsssearchserver.psgi

# spawn-fcgiでの起動（FastCGI）
spawn-fcgi -s /var/run/kafsss.sock -n -- plackup -s FCGI kafsssearchserver.psgi

# その他のPSGIサーバ
starman --port 5000 --workers 10 kafsssearchserver.psgi
uwsgi --http :5000 --psgi kafsssearchserver.psgi
```

#### NGINX設定（FastCGI）
```nginx
server {
    listen 80;
    server_name your-domain.com;

    location /api/ {
        include fastcgi_params;
        fastcgi_pass unix:/var/run/kafsss.sock;
        fastcgi_param SCRIPT_FILENAME $document_root$fastcgi_script_name;
    }
}
```

#### NGINX設定（リバースプロキシ）
```nginx
server {
    listen 80;
    server_name your-domain.com;

    location /api/ {
        proxy_pass http://127.0.0.1:5000/;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
}
```

#### 使用例
```bash
# スタンドアローンサーバ起動
perl kafsssearchserver.psgi

# カスタムポートとスレッド数で起動
perl kafsssearchserver.psgi --listenport=8080 --numthreads=10

# plackupで起動
plackup -p 8080 --workers 20 kafsssearchserver.psgi

# API呼び出し
curl -X POST http://localhost:5000/search \
  -H "Content-Type: application/json" \
  -d '{
    "querylabel": "test_sequence",
    "queryseq": "ATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCGATCG",
    "db": "mydb"
  }'

# サーバメタデータ取得
curl http://localhost:5000/metadata
```

## ワークフロー例

### 完全なデータベースセットアップと検索

1. **データベース作成と配列格納:**
   ```bash
   kafssstore --subset=bacteria sequences.fasta mydb
   ```

2. **配列の重複除去（パーティションテーブルの互換性確認）:**
   ```bash
   kafssdedup mydb
   ```

3. **パフォーマンス向上のためのテーブルパーティション化:**
   ```bash
   kafsspart --npart=16 mydb
   ```

4. **k-mer頻度分析の実行:**
   ```bash
   kafssfreq mydb
   ```

5. **インデックス作成:**
   ```bash
   kafssindex --mode=create mydb
   ```

6. **サブセット情報追加:**
   ```bash
   kafsssubset --subset=pathogenic bacteria_ids.txt mydb
   ```

7. **データベース情報確認:**
   ```bash
   kafssdbinfo mydb
   ```

8. **配列検索:**
   ```bash
   kafsssearch --db=mydb --subset=pathogenic query.fasta results.tsv
   ```

### Web APIデプロイメント

1. **デフォルト値設定:**
   ```perl
   # kafsssearchserver.psgiを編集
   my $default_database = 'mydb';
   my $default_subset = 'bacteria';
   ```

2. **NGINXでのデプロイ（FastCGI）:**
   ```bash
   spawn-fcgi -s /var/run/kafsss.sock \
              -- plackup -s FCGI kafsssearchserver.psgi
   ```

3. **またはNGINXでのデプロイ（リバースプロキシ）:**
   ```bash
   perl kafsssearchserver.psgi --listenport=5000 --numthreads=10
   ```

4. **API経由での検索:**
   ```bash
   curl -X POST http://your-domain.com/api/search \
        -H "Content-Type: application/json" \
        -d '{"querylabel": "test", "queryseq": "ATCG..."}'
   ```

## パフォーマンスのヒント

- CPUコア数に基づいて適切な`--numthreads`を使用
- 大量データロード後にインデックスを作成
- 大規模データセットにはサブセットを使用
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