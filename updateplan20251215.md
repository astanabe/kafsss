# 複数GINインデックス選択ロジックの実装計画

## 背景・現状

`kafsssearch.pl`および`kafsssearchserver.*`において、塩基配列データ用の複数のGINインデックスが存在する場合に適切なGINインデックスを選択するロジックが**実装されていない**。

### 現在の問題点

1. **GINインデックスの存在確認**が「少なくとも1つ存在するか」のみ
2. **複数のGINインデックスがある場合の選択ロジック**がない
3. **GUC変数の設定**が`kafsss_meta`テーブルの値をそのまま使用しており、インデックスとの一致確認がない
4. **サーバーコンポーネント**が単一データベースのみ対応

## 要件

### 共通要件

1. GINインデックスが1つしかない場合は自動的にそれを使用する
2. 複数のGINインデックスがある場合は設定(kmer_size, occur_bitlen, max_appearance_rate, max_appearance_nrow, preclude_highfreq_kmer)の値が一致するものを使用する
3. 設定の一部を「未指定」にすることを許容する
4. 指定された設定に一致するGINインデックスが「1件だけ」マッチするなら、そのインデックスを使用し、未指定だった設定はインデックスから取得してGUC変数に使用する
5. 指定された設定に一致するGINインデックスが複数マッチした場合は、GINインデックスが1つに特定できないというエラーを吐いて終了
6. 設定の値が一致するGINインデックスが存在しない場合もエラーを吐いて終了
7. 設定の値が一致するGINインデックスが存在していればそれがプランナーに選択されるようにGUC変数を設定して検索を実行

### kafsssearchserver.*固有の要件

8. **複数データベース対応**: クライアントが検索ジョブ投入時にデータベースを指定可能
9. **複数サブセット対応**: クライアントが検索ジョブ投入時にサブセットを指定可能（「サブセット指定なし」も対応）
10. **複数GINインデックス対応**: クライアントが検索ジョブ投入時にGINインデックスを指定可能
11. **起動時検証**: 全対象データベースの構成を検証し、利用可能なインデックス一覧を構築

## GINインデックス命名規則（参照: CLAUDE.md）

```
idx_[tablename]_seq_gin_km{N}_ob{N}_mar{NNNN}_man{N}_phk{T/F}
```

- `km{N}`: kmer_size値
- `ob{N}`: occur_bitlen値
- `mar{NNNN}`: max_appearance_rate × 1000 (4桁整数)
- `man{N}`: max_appearance_nrow値
- `phk{T/F}`: preclude_highfreq_kmer フラグ (T=true, F=false)

例: `idx_kafsss_data_seq_gin_km8_ob8_mar0500_man0_phkT`

## 修正対象ファイル

1. `kafsssearch.pl`
2. `kafsssearchserver.pl`
3. `kafsssearchserver.fcgi`
4. `kafsssearchserver.psgi`

---

# Part 1: kafsssearch.pl の修正

## コマンドラインオプション名

`kafssfreq.pl`および`kafssindex.pl`と統一する:

| オプション | 対応するパラメータ | 未指定時の動作 |
|------------|-------------------|---------------|
| `--kmersize` | kmer_size | 未指定可（マッチが1件なら自動決定） |
| `--maxpappear` | max_appearance_rate | 未指定可（マッチが1件なら自動決定） |
| `--maxnappear` | max_appearance_nrow | 未指定可（マッチが1件なら自動決定） |
| `--occurbitlen` | occur_bitlen | 未指定可（マッチが1件なら自動決定） |
| `--precludehighfreqkmer` | preclude_highfreq_kmer | 未指定可（マッチが1件なら自動決定） |

## 修正計画

### 1. GINインデックス一覧取得関数の追加

```perl
sub get_gin_indexes {
    my ($dbh) = @_;

    my $sth = $dbh->prepare(<<SQL);
SELECT indexname
FROM pg_indexes
WHERE tablename = 'kafsss_data'
  AND indexname LIKE 'idx_kafsss_data_seq_gin_km%'
ORDER BY indexname
SQL
    $sth->execute();

    my @indexes = ();
    while (my ($indexname) = $sth->fetchrow_array()) {
        push @indexes, $indexname;
    }
    $sth->finish();

    return \@indexes;
}
```

### 2. インデックス名パース関数の追加

```perl
sub parse_gin_index_name {
    my ($indexname) = @_;

    if ($indexname =~ /idx_kafsss_data_seq_gin_km(\d+)_ob(\d+)_mar(\d{4})_man(\d+)_phk([TF])/) {
        return {
            kmer_size => int($1),
            occur_bitlen => int($2),
            max_appearance_rate => $3 / 1000,
            max_appearance_nrow => int($4),
            preclude_highfreq_kmer => ($5 eq 'T' ? 1 : 0)
        };
    }

    return undef;
}
```

### 3. GINインデックス選択関数の追加（部分一致対応）

```perl
sub select_gin_index {
    my ($dbh, $indexes, $target_params) = @_;

    my $index_count = scalar(@$indexes);

    if ($index_count == 0) {
        die "Error: No GIN indexes found on kafsss_data.seq column.\n" .
            "Please create indexes first using: kafssindex --mode=create <database>\n";
    }

    if ($index_count == 1) {
        my $parsed = parse_gin_index_name($indexes->[0]);
        return {
            index_name => $indexes->[0],
            params => $parsed
        };
    }

    my @matching_indexes = ();

    for my $indexname (@$indexes) {
        my $parsed = parse_gin_index_name($indexname);
        next unless $parsed;

        my $matches = 1;

        if (defined $target_params->{kmer_size}) {
            $matches = 0 if $parsed->{kmer_size} != $target_params->{kmer_size};
        }
        if (defined $target_params->{occur_bitlen}) {
            $matches = 0 if $parsed->{occur_bitlen} != $target_params->{occur_bitlen};
        }
        if (defined $target_params->{max_appearance_rate}) {
            $matches = 0 if abs($parsed->{max_appearance_rate} - $target_params->{max_appearance_rate}) >= 0.0001;
        }
        if (defined $target_params->{max_appearance_nrow}) {
            $matches = 0 if $parsed->{max_appearance_nrow} != $target_params->{max_appearance_nrow};
        }
        if (defined $target_params->{preclude_highfreq_kmer}) {
            $matches = 0 if $parsed->{preclude_highfreq_kmer} != $target_params->{preclude_highfreq_kmer};
        }

        if ($matches) {
            push @matching_indexes, {
                index_name => $indexname,
                params => $parsed
            };
        }
    }

    my $match_count = scalar(@matching_indexes);

    if ($match_count == 0) {
        # エラー: マッチするインデックスなし
        die "Error: No matching GIN index found.\n";
    }

    if ($match_count == 1) {
        return $matching_indexes[0];
    }

    # エラー: 複数マッチ
    die "Error: Multiple GIN indexes match. Please specify additional parameters.\n";
}
```

---

# Part 2: kafsssearchserver.* の修正

## サーバーファイル冒頭の設定欄

以下の設定項目を追加:

```perl
# Database configuration
my @available_databases = ('mykmersearch');  # Array of available database names

# Subset configuration
# Format: "database_name:subset_name" (e.g., "mykmersearch:bacteria")
my @available_subsets = ();  # Array of available subsets

# Default database (required)
my $default_database = 'mykmersearch';

# Default subset (optional)
# Format: "database_name:subset_name" or empty string for no subset
my $default_subset = '';

# Default GIN index parameters (all optional, used for index selection)
my $default_kmersize = '';           # Default kmer_size value (empty = unspecified)
my $default_occurbitlen = '';        # Default occur_bitlen value
my $default_maxpappear = '';         # Default max_appearance_rate (max 3 decimal places)
my $default_maxnappear = '';         # Default max_appearance_nrow value
my $default_precludehighfreqkmer = '';  # Default preclude_highfreq_kmer (1, 0, or empty)
```

### 設定フォーマットの詳細

- **available_databases**: データベース名の配列
- **available_subsets**: 「データベース名:サブセット名」形式の配列（例: `"mykmersearch:bacteria"`）
- **default_subset**: 「データベース名:サブセット名」形式、または空文字列（サブセット指定なし）
- **default_maxpappear**: 小数点以下3桁まで。4桁以上の場合は起動時エラー

## 起動時の検証処理

サーバー起動時に以下の検証を行う:

### 1. 全対象データベースの検証

```perl
sub validate_all_databases {
    my @available_indices = ();

    for my $dbname (@available_databases) {
        # データベースに接続
        my $dbh = connect_to_database($dbname);

        # kafsss_dataとkafsss_metaの存在確認
        validate_required_tables($dbh, $dbname);

        # seqカラムのGINインデックスが1つ以上あることを確認
        my $indexes = get_gin_indexes($dbh);
        if (scalar(@$indexes) == 0) {
            die "Error: No GIN indexes found in database '$dbname'.\n";
        }

        # available_indicesに追加（形式: "データベース名:GINインデックス名"）
        for my $idx (@$indexes) {
            push @available_indices, "$dbname:$idx";
        }

        # available_subsetsに記載されたサブセットの存在確認
        for my $subset_spec (@available_subsets) {
            my ($subset_db, $subset_name) = split(/:/, $subset_spec, 2);
            if ($subset_db eq $dbname) {
                validate_subset_exists($dbh, $dbname, $subset_name);
            }
        }

        $dbh->disconnect();
    }

    return \@available_indices;
}
```

### 2. デフォルトデータベースの追加検証

```perl
sub validate_default_database {
    my $dbh = connect_to_database($default_database);

    # default_subsetの検証（指定されている場合）
    if ($default_subset ne '') {
        my ($subset_db, $subset_name) = split(/:/, $default_subset, 2);

        # データベース名の一致確認
        if ($subset_db ne $default_database) {
            die "Error: default_subset database '$subset_db' does not match default_database '$default_database'.\n";
        }

        # サブセットの存在確認
        validate_subset_exists($dbh, $default_database, $subset_name);
    }

    # デフォルトパラメータでGINインデックスが1件に絞れるか検証
    my $indexes = get_gin_indexes($dbh);
    my $target_params = build_target_params_from_defaults();

    my @matching = filter_matching_indexes($indexes, $target_params);

    if (scalar(@matching) == 0) {
        die "Error: No matching GIN index found in default database with default parameters.\n";
    }

    if (scalar(@matching) > 1) {
        my @list = map { "  - $_" } @matching;
        die "Error: Multiple GIN indexes match default parameters in default database.\n" .
            "Matching indexes:\n" . join("\n", @list) . "\n" .
            "Please specify more specific default parameters.\n";
    }

    $dbh->disconnect();
}
```

### 3. max_appearance_rateの桁数検証

```perl
sub validate_maxpappear_precision {
    my ($value, $context) = @_;

    return if $value eq '';  # 未指定は許容

    # 小数点以下4桁以上チェック
    if ($value =~ /\.\d{4,}/) {
        die "Error: $context max_appearance_rate '$value' has more than 3 decimal places.\n" .
            "Maximum 3 decimal places allowed (e.g., 0.050, 0.125).\n";
    }
}
```

## GET /metadata エンドポイントの拡張

### レスポンス

```json
{
    "success": true,
    "server_version": "1.0.0",
    "default_database": "mykmersearch",
    "default_subset": "mykmersearch:bacteria",
    "default_maxnseq": 1000,
    "default_minscore": 1,
    "default_kmersize": 8,
    "default_occurbitlen": 8,
    "default_maxpappear": 0.05,
    "default_maxnappear": 0,
    "default_precludehighfreqkmer": true,
    "available_databases": ["mykmersearch", "otherdb"],
    "available_subsets": ["mykmersearch:bacteria", "mykmersearch:virus", "otherdb:fungi"],
    "available_indices": [
        "mykmersearch:idx_kafsss_data_seq_gin_km8_ob8_mar0500_man0_phkT",
        "mykmersearch:idx_kafsss_data_seq_gin_km15_ob8_mar0100_man0_phkT",
        "otherdb:idx_kafsss_data_seq_gin_km8_ob8_mar0500_man0_phkF"
    ],
    "accept_gzip_request": true,
    "supported_endpoints": ["/search", "/result", "/status", "/cancel", "/metadata"]
}
```

## POST /search エンドポイントの拡張

### リクエスト

```json
{
    "queryseq": "ATCGATCGATCG...",        // 必須
    "querylabel": "my_sequence",          // オプション、デフォルト: "queryseq"
    "database": "mykmersearch",           // オプション、dbの別名（両方指定はエラー）
    "db": "mykmersearch",                 // オプション、databaseのサブ
    "subset": "bacteria",                 // オプション
    "index": "idx_kafsss_data_seq_gin_km8_ob8_mar0500_man0_phkT",  // オプション、排他
    "kmersize": 8,                        // オプション、indexと排他
    "occurbitlen": 8,                     // オプション、indexと排他
    "maxpappear": 0.05,                   // オプション、indexと排他
    "maxnappear": 0,                      // オプション、indexと排他
    "precludehighfreqkmer": true,         // オプション、indexと排他
    "maxnseq": 1000,                      // オプション
    "minscore": 10,                       // オプション
    "minpsharedkmer": 0.5,                // オプション
    "mode": "matchscore"                  // オプション
}
```

### パラメータの排他関係

- `database`と`db`は両方指定するとエラー
- `index`と`kmersize/occurbitlen/maxpappear/maxnappear/precludehighfreqkmer`は排他
  - `index`が指定されている場合、インデックス名からパラメータを抽出
  - 両方指定されている場合はエラー

### バリデーション

```perl
sub validate_search_request {
    my ($request) = @_;

    # queryseqは必須
    die "Missing required field: queryseq\n" unless $request->{queryseq};

    # querylabelのデフォルト
    $request->{querylabel} ||= 'queryseq';

    # databaseとdbの排他チェック
    if (defined $request->{database} && $request->{database} ne '' &&
        defined $request->{db} && $request->{db} ne '') {
        die "Cannot specify both 'database' and 'db'. Use one or the other.\n";
    }

    # databaseをメインに
    $request->{database} ||= $request->{db};
    $request->{database} ||= $default_database;

    # indexと個別パラメータの排他チェック
    if (defined $request->{index} && $request->{index} ne '') {
        my @individual_params = qw(kmersize occurbitlen maxpappear maxnappear precludehighfreqkmer);
        for my $param (@individual_params) {
            if (defined $request->{$param} && $request->{$param} ne '') {
                die "Cannot specify both 'index' and '$param'. Use 'index' alone or individual parameters.\n";
            }
        }

        # インデックス名からパラメータを抽出
        my $parsed = parse_gin_index_name($request->{index});
        die "Invalid index name format: $request->{index}\n" unless $parsed;

        $request->{kmersize} = $parsed->{kmer_size};
        $request->{occurbitlen} = $parsed->{occur_bitlen};
        $request->{maxpappear} = $parsed->{max_appearance_rate};
        $request->{maxnappear} = $parsed->{max_appearance_nrow};
        $request->{precludehighfreqkmer} = $parsed->{preclude_highfreq_kmer};
    }

    # maxpappearの桁数検証
    validate_maxpappear_precision($request->{maxpappear}, "Request");

    return $request;
}
```

## POST /result, POST /status エンドポイントのレスポンス拡張

### レスポンス（共通部分）

完了・未完了に関わらず、以下の情報を返す:

```json
{
    "success": true,
    "status": "running",
    "job_id": "20250703T120000-AbCdEfGhIjKlMnOpQrStUvWxYz012345",
    "database": "mykmersearch",
    "subset": "bacteria",
    "index": "idx_kafsss_data_seq_gin_km8_ob8_mar0500_man0_phkT",
    "kmersize": 8,
    "occurbitlen": 8,
    "maxpappear": 0.05,
    "maxnappear": 0,
    "precludehighfreqkmer": true,
    "maxnseq": 1000,
    "minscore": 10
}
```

**注意**: `subset`はユーザーのクエリでも`default_subset`でも未指定の場合は省略（キー自体を含めない）

### SQLiteジョブテーブルの拡張

```sql
CREATE TABLE IF NOT EXISTS kafsssearchserver_jobs (
    job_id TEXT PRIMARY KEY,
    time TEXT NOT NULL,
    querylabel TEXT NOT NULL,
    queryseq TEXT NOT NULL,
    database TEXT NOT NULL,          -- 追加
    subset TEXT,                     -- 既存
    index_name TEXT NOT NULL,        -- 追加
    kmersize INTEGER NOT NULL,       -- 追加
    occurbitlen INTEGER NOT NULL,    -- 追加
    maxpappear REAL NOT NULL,        -- 追加
    maxnappear INTEGER NOT NULL,     -- 追加
    precludehighfreqkmer INTEGER NOT NULL,  -- 追加
    maxnseq INTEGER NOT NULL,
    minscore INTEGER NOT NULL,
    mode TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'running',
    pid INTEGER,
    timeout_time TEXT
)
```

## 処理フロー

### 起動時

```
1. 設定ファイル読み込み
2. default_maxpappearの桁数検証
3. 全対象データベースの検証:
   a. 各データベースに接続
   b. kafsss_data, kafsss_metaの存在確認
   c. seqカラムのGINインデックス確認（1つ以上必要）
   d. available_subsetsのサブセット存在確認
   e. GINインデックス一覧をavailable_indicesに追加
4. デフォルトデータベースの追加検証:
   a. default_subsetの検証（指定時）
   b. デフォルトパラメータでGINインデックスが1件に絞れるか検証
5. SQLiteデータベース初期化
6. サーバー開始
```

### POST /search リクエスト処理

```
1. リクエストパース
2. バリデーション:
   a. queryseq必須チェック
   b. database/db排他チェック
   c. index/個別パラメータ排他チェック
   d. maxpappear桁数検証
3. デフォルト値適用
4. データベース存在確認（available_databasesに含まれるか）
5. サブセット存在確認（指定時、available_subsetsに含まれるか）
6. GINインデックス選択:
   a. indexが指定されていればそれを使用
   b. 個別パラメータが指定されていれば部分一致検索
   c. マッチ1件→使用、マッチ複数→エラー
7. ジョブ作成（拡張されたカラムに保存）
8. バックグラウンドジョブ開始
9. job_id返却
```

## エラーメッセージ例

### database/db両方指定
```json
{
    "error": true,
    "code": "INVALID_REQUEST",
    "message": "Cannot specify both 'database' and 'db'. Use one or the other."
}
```

### index/個別パラメータ両方指定
```json
{
    "error": true,
    "code": "INVALID_REQUEST",
    "message": "Cannot specify both 'index' and 'kmersize'. Use 'index' alone or individual parameters."
}
```

### maxpappear桁数エラー
```json
{
    "error": true,
    "code": "INVALID_REQUEST",
    "message": "max_appearance_rate '0.0501' has more than 3 decimal places. Maximum 3 decimal places allowed."
}
```

### 複数インデックスマッチ
```json
{
    "error": true,
    "code": "MULTIPLE_INDEX_MATCH",
    "message": "Multiple GIN indexes match the specified parameters. Please specify additional parameters.",
    "matching_indexes": [
        "idx_kafsss_data_seq_gin_km8_ob8_mar0500_man0_phkT",
        "idx_kafsss_data_seq_gin_km8_ob8_mar0500_man0_phkF"
    ]
}
```

---

## 影響範囲

### kafsssearch.pl
- コマンドラインオプション追加（5つ、全てオプション）
- インデックス選択ロジック追加

### kafsssearchserver.pl / kafsssearchserver.fcgi / kafsssearchserver.psgi
- 設定欄の大幅拡張
- 起動時検証処理の追加
- GET /metadataレスポンス拡張
- POST /searchリクエストパラメータ拡張
- POST /result, POST /statusレスポンス拡張
- SQLiteテーブルスキーマ拡張

### ドキュメント
- CLAUDE.md: 新オプション・新API仕様の説明追加
- ヘルプメッセージ: 新オプションの説明追加

---

## テスト計画

### kafsssearch.pl

1. GINインデックスが1つだけの場合の動作確認
2. 複数GINインデックスがある場合:
   a. パラメータ未指定で複数マッチ → エラー
   b. 部分指定で1件にマッチ → 成功
   c. 部分指定で複数マッチ → エラー
3. インデックスが存在しない場合のエラー確認

### kafsssearchserver.*

1. **起動時検証**
   a. 正常なデータベース構成での起動成功
   b. kafsss_dataがないデータベースでの起動エラー
   c. GINインデックスがないデータベースでの起動エラー
   d. 存在しないサブセットでの起動エラー
   e. デフォルトパラメータで複数インデックスマッチでの起動エラー
   f. maxpappear桁数エラーでの起動エラー

2. **GET /metadata**
   a. available_databases, available_subsets, available_indicesが正しく返却されること

3. **POST /search**
   a. queryseqのみでの検索成功（デフォルト値使用）
   b. database指定での検索成功
   c. db指定での検索成功
   d. database/db両方指定でエラー
   e. index指定での検索成功
   f. index/個別パラメータ両方指定でエラー
   g. 部分パラメータ指定での検索成功
   h. maxpappear桁数エラー
   i. 存在しないデータベース指定でエラー
   j. 存在しないサブセット指定でエラー

4. **POST /result, POST /status**
   a. レスポンスに全ての追加フィールドが含まれること
   b. subset未指定時にsubsetキーが省略されること

---

## 補足：kafssindex.plの動作確認

`kafssindex.pl`の現在の動作（変更不要）:
- `kmersearch_highfreq_kmer_meta`テーブルに一致するエントリがあれば`preclude_highfreq_kmer=true`でインデックス作成
- 一致するエントリがなければ`preclude_highfreq_kmer=false`でインデックス作成（エラーではなく警告のみで続行）
- これにより、同じパラメータでも`phkT`と`phkF`の両方のインデックスが存在する可能性がある

---

# Part 3: ドキュメント更新

## CLAUDE.md の更新

### 更新箇所

#### 1. Performance Parameters セクション

以下の説明を追加:

```markdown
### Multiple GIN Index Selection

When multiple GIN indexes exist on the `seq` column, kafsssearch and kafsssearchserver.* can select the appropriate index based on parameters:

**kafsssearch.pl options:**
- `--kmersize=INT` - K-mer size for index selection
- `--occurbitlen=INT` - Occurrence bit length
- `--maxpappear=REAL` - Max appearance rate (max 3 decimal places)
- `--maxnappear=INT` - Max appearance nrow
- `--precludehighfreqkmer` - Use index with preclude_highfreq_kmer=true

**Selection logic:**
- If only one GIN index exists, it is automatically selected
- If multiple indexes exist and parameters match exactly one, that index is used
- If multiple indexes match, an error is returned requesting additional parameters
- If no indexes match, an error is returned
```

#### 2. Server Configuration セクション

以下の説明を追加:

```markdown
### Server Multi-Database Configuration

Server components support multiple databases and GIN indexes:

**Configuration variables:**
- `@available_databases` - Array of available database names
- `@available_subsets` - Array of "database_name:subset_name" format strings
- `$default_database` - Default database name
- `$default_subset` - Default subset in "database_name:subset_name" format
- `$default_kmersize`, `$default_occurbitlen`, `$default_maxpappear`, `$default_maxnappear`, `$default_precludehighfreqkmer` - Default GIN index parameters

**Startup validation:**
- All databases in @available_databases are validated for kafsss_data, kafsss_meta, and GIN indexes
- All subsets in @available_subsets are validated for existence
- Default database must have exactly one GIN index matching default parameters
```

#### 3. API endpoints セクション

既存のエンドポイント説明を拡張:

```markdown
### Extended API Endpoints

**GET /metadata** response includes:
- `available_databases`: Array of available database names
- `available_subsets`: Array of "database_name:subset_name" strings
- `available_indices`: Array of "database_name:index_name" strings
- `default_kmersize`, `default_occurbitlen`, `default_maxpappear`, `default_maxnappear`, `default_precludehighfreqkmer`

**POST /search** request accepts:
- `database` or `db`: Target database name (mutually exclusive)
- `index`: Direct GIN index name specification (mutually exclusive with individual params)
- `kmersize`, `occurbitlen`, `maxpappear`, `maxnappear`, `precludehighfreqkmer`: Index selection parameters

**POST /result, POST /status** response includes:
- `database`, `subset`, `index`, `kmersize`, `occurbitlen`, `maxpappear`, `maxnappear`, `precludehighfreqkmer`, `maxnseq`, `minscore`
```

---

## README.md の更新

### 更新箇所

Featuresセクションに以下を追加:

```markdown
- **Multiple GIN Index Support**: Select from multiple k-mer indexes with different parameters
- **Multi-Database Servers**: Server components support multiple databases with load balancing
```

Quick Startセクションに複数インデックスの例を追加:

```markdown
# Search with specific index parameters (when multiple indexes exist)
kafsssearch --db=mydb --kmersize=8 --precludehighfreqkmer query.fasta results.tsv
```

---

## doc/kafsss.en.md の更新

### 更新箇所

#### 1. kafsssearch セクション

Optionsに以下を追加:

```markdown
- `--kmersize=INT` - K-mer size for index selection (optional, auto-detected if only one index)
- `--occurbitlen=INT` - Occurrence bit length for index selection
- `--maxpappear=REAL` - Max appearance rate for index selection (max 3 decimal places)
- `--maxnappear=INT` - Max appearance nrow for index selection
- `--precludehighfreqkmer` - Select index with preclude_highfreq_kmer=true
```

Multiple Index Selectionサブセクションを追加:

```markdown
#### Multiple Index Selection

When multiple GIN indexes exist on the kafsss_data.seq column:

- **Single index**: Automatically selected, no parameters needed
- **Multiple indexes**: Specify parameters to narrow down to one index
- **Partial specification allowed**: Only specify parameters needed to uniquely identify the index

Examples:
```bash
# Single index - no parameters needed
kafsssearch --db=mydb query.fasta results.tsv

# Multiple indexes - specify kmersize to narrow down
kafsssearch --db=mydb --kmersize=8 query.fasta results.tsv

# Multiple indexes with same kmersize - add precludehighfreqkmer
kafsssearch --db=mydb --kmersize=8 --precludehighfreqkmer query.fasta results.tsv
```
```

#### 2. kafsssearchserver.* セクション

Configurationサブセクションを拡張:

```markdown
#### Multi-Database Configuration

Edit default values in the script header:
```perl
# Database configuration
my @available_databases = ('mykmersearch', 'otherdb');
my @available_subsets = ('mykmersearch:bacteria', 'mykmersearch:virus');
my $default_database = 'mykmersearch';
my $default_subset = 'mykmersearch:bacteria';

# GIN index parameters
my $default_kmersize = 8;
my $default_occurbitlen = 8;
my $default_maxpappear = 0.05;
my $default_maxnappear = 0;
my $default_precludehighfreqkmer = 1;
```
```

API Endpointsサブセクションを拡張:

```markdown
**GET /metadata** - Extended response

Response JSON:
```json
{
  "success": true,
  "server_version": "1.0.0",
  "default_database": "mykmersearch",
  "default_subset": "mykmersearch:bacteria",
  "default_kmersize": 8,
  "default_occurbitlen": 8,
  "default_maxpappear": 0.05,
  "default_maxnappear": 0,
  "default_precludehighfreqkmer": true,
  "available_databases": ["mykmersearch", "otherdb"],
  "available_subsets": ["mykmersearch:bacteria", "mykmersearch:virus"],
  "available_indices": ["mykmersearch:idx_kafsss_data_seq_gin_km8_ob8_mar0500_man0_phkT"],
  "accept_gzip_request": true
}
```

**POST /search** - Extended request

Request JSON:
```json
{
  "queryseq": "ATCGATCG...",
  "querylabel": "sequence_name",
  "database": "mykmersearch",
  "subset": "bacteria",
  "index": "idx_kafsss_data_seq_gin_km8_ob8_mar0500_man0_phkT",
  "kmersize": 8,
  "occurbitlen": 8,
  "maxpappear": 0.05,
  "maxnappear": 0,
  "precludehighfreqkmer": true,
  "maxnseq": 1000,
  "minscore": 10
}
```

Notes:
- `database` and `db` are mutually exclusive
- `index` and individual parameters (kmersize, etc.) are mutually exclusive
- If `index` is specified, parameters are extracted from the index name

**POST /result, POST /status** - Extended response

Response includes job parameters:
```json
{
  "job_id": "...",
  "status": "completed",
  "database": "mykmersearch",
  "subset": "bacteria",
  "index": "idx_kafsss_data_seq_gin_km8_ob8_mar0500_man0_phkT",
  "kmersize": 8,
  "occurbitlen": 8,
  "maxpappear": 0.05,
  "maxnappear": 0,
  "precludehighfreqkmer": true,
  "maxnseq": 1000,
  "minscore": 10,
  "results": [...]
}
```
```

---

## doc/kafsss.ja.md の更新

### 更新箇所

doc/kafsss.en.mdと同様の内容を日本語で記述:

#### 1. kafsssearch セクション

オプションに以下を追加:

```markdown
- `--kmersize=INT` - インデックス選択用k-merサイズ（オプション、インデックスが1つの場合は自動検出）
- `--occurbitlen=INT` - インデックス選択用出現ビット長
- `--maxpappear=REAL` - インデックス選択用最大出現率（小数点以下3桁まで）
- `--maxnappear=INT` - インデックス選択用最大出現行数
- `--precludehighfreqkmer` - preclude_highfreq_kmer=trueのインデックスを選択
```

複数インデックス選択サブセクションを追加:

```markdown
#### 複数インデックス選択

kafsss_data.seqカラムに複数のGINインデックスが存在する場合:

- **単一インデックス**: 自動的に選択、パラメータ指定不要
- **複数インデックス**: パラメータを指定して1つに絞り込み
- **部分指定可能**: 一意に特定できる最小限のパラメータ指定で可

使用例:
```bash
# 単一インデックス - パラメータ不要
kafsssearch --db=mydb query.fasta results.tsv

# 複数インデックス - kmersizeで絞り込み
kafsssearch --db=mydb --kmersize=8 query.fasta results.tsv

# 同じkmersizeの複数インデックス - precludehighfreqkmerを追加
kafsssearch --db=mydb --kmersize=8 --precludehighfreqkmer query.fasta results.tsv
```
```

#### 2. kafsssearchserver.* セクション

設定サブセクションを拡張:

```markdown
#### 複数データベース設定

スクリプトヘッダーでデフォルト値を編集:
```perl
# データベース設定
my @available_databases = ('mykmersearch', 'otherdb');
my @available_subsets = ('mykmersearch:bacteria', 'mykmersearch:virus');
my $default_database = 'mykmersearch';
my $default_subset = 'mykmersearch:bacteria';

# GINインデックスパラメータ
my $default_kmersize = 8;
my $default_occurbitlen = 8;
my $default_maxpappear = 0.05;
my $default_maxnappear = 0;
my $default_precludehighfreqkmer = 1;
```
```

APIエンドポイントサブセクションを拡張（doc/kafsss.en.mdのJSON例と同様、説明文を日本語化）

---

## ドキュメント更新の実施順序

1. `CLAUDE.md` - 開発者向け技術情報の更新
2. `README.md` - 概要とFeatures、Quick Startの更新
3. `doc/kafsss.en.md` - 詳細英語ドキュメントの更新
4. `doc/kafsss.ja.md` - 詳細日本語ドキュメントの更新

## ドキュメント更新の注意点

- 既存のセクション構造を維持
- 新機能は既存の関連セクション内に追加
- JSON例は実際の実装と一致させる
- 日本語ドキュメントは英語版と同等の情報量を維持
