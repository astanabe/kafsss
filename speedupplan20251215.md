# kafsssearch.pl / kafsssearchclient.pl 速度改善計画

作成日: 2025-12-15

## 背景

`--numthreads=32` を指定しても実際には数個のCPUしか使用されない問題の調査から、以下の原因が判明した：

1. **ディスクI/Oボトルネック**: 親プロセスが `balance_dirty_pages` でブロックされ、新しい子プロセスを起動できない
2. **一時ファイルの非効率な処理**: 行数カウントのために一時ファイルを2回読み込んでいる
3. **一時ファイルが非圧縮**: 大量の結果がディスクI/Oを圧迫

## 適用対象

**すべての改善項目は以下の両方のスクリプトに適用される**:
- `kafsssearch.pl` - ローカル検索ツール
- `kafsssearchclient.pl` - リモートAPIクライアント

両スクリプトは並列処理と一時ファイル処理のコード構造がほぼ同一であるため、同じ改善を適用する。

## 改善項目

### 1. 一時ファイルの二重読み込み廃止（優先度: 高）

**現状の問題**:
- `process_sequences_parallel_streaming` 関数内で、子プロセス完了時に一時ファイルを行数カウントのためだけに1回読み込む（757-764行目、827-834行目）
- その後、実際の出力時にもう1回読み込む（797行目、869行目等）
- 同じファイルを2回読み込むのは完全に無駄

**改善案**:
- 事前の行数カウント処理を完全に削除
- 子プロセス完了時は一時ファイルのパスのみを保存
- 出力時に読み込みながら行数をカウント（1回の読み込みで完了）

**該当箇所** (kafsssearch.pl):
- 757-776行目: 削除
- 827-846行目: 削除
- `$completed_results{$query_number}` の構造を `{ file => $temp_file }` に簡略化

**該当箇所** (kafsssearchclient.pl):
- 474-493行目: 削除
- 544-563行目: 削除

### 2. 圧縮処理の最適化（優先度: 高）

**現状の問題**:
- 一時ファイルが非圧縮で、1クエリあたり最大2.4GB程度になる場合がある
- 32並列だと最大80GB近くのディスクI/Oが発生

**重要な設計原則**:
- **圧縮は1回だけ**: 圧縮→解凍→再圧縮は絶対に行わない
- **展開しない**: 圧縮ファイルはバイナリのまま連結または移動

**出力形式別の最適な処理**:

| 出力形式 | 一時ファイル | 子プロセスの処理 | 親プロセスの処理 |
|----------|-------------|-----------------|-----------------|
| multiTSV.gz | 圧縮 | 圧縮一時ファイルを作成 | 最終ファイルに移動（rename） |
| multiFASTA.gz | 圧縮 | 圧縮一時ファイルを作成 | 最終ファイルに移動（rename） |
| multiTSV | 非圧縮 | 非圧縮一時ファイルを作成 | 最終ファイルに移動（rename） |
| multiFASTA | 非圧縮 | 非圧縮一時ファイルを作成 | 最終ファイルに移動（rename） |
| TSV.gz | 圧縮 | 圧縮一時ファイルを作成 | バイナリ連結（展開しない） |
| TSV | 非圧縮 | 非圧縮一時ファイルを作成 | テキスト連結 |
| BLASTDB | **不要** | 直接最終ファイルを作成 | 完了確認のみ |

**注意**: gzip, bzip2, xz, zstd はすべて圧縮ファイルの連結が可能。
連結されたファイルを展開すると、各ストリームの内容が順番に展開される。

**改善案A: multiファイル形式（multiTSV, multiFASTA）**:

子プロセスが一時ファイルを作成し、親プロセスが最終ファイルにリネーム。

```perl
# 子プロセス
sub process_single_sequence {
    my ($fasta_entry, $query_number, $temp_file, $metadata, $output_info) = @_;

    my $results = search_sequence(...);

    # 一時ファイルに書き込み（圧縮形式の場合は圧縮パイプ経由）
    my $fh = open_output_with_optional_compression($temp_file, $output_info->{compression_type});
    write_results($fh, $results, $output_info->{base_format});
    close $fh;
}

# 親プロセス
sub finalize_multifile_output {
    my ($temp_file, $query_number, $output_info) = @_;

    # 一時ファイルを最終ファイルにリネーム
    my $final_file = sprintf("%s_%d%s",
        $output_info->{prefix},
        $query_number,
        $output_info->{extension}  # 例: ".tsv.gz"
    );
    rename($temp_file, $final_file) or die "Cannot rename $temp_file to $final_file: $!\n";
}
```

**改善案A': BLASTDB形式**:

子プロセスが直接最終ファイルを作成。一時ファイルは使用しない。

```perl
# 子プロセス（--mode=sequence の場合）
sub process_single_sequence_blastdb {
    my ($fasta_entry, $query_number, $output_info, $metadata) = @_;

    my $results = search_sequence(...);

    # 直接 makeblastdb にパイプ出力
    my $db_name = sprintf("%s_%d", $output_info->{prefix}, $query_number);
    my @cmd = (
        'makeblastdb',
        '-dbtype', 'nucl',
        '-input_type', 'fasta',
        '-hash_index',
        '-parse_seqids',
        '-in', '-',
        '-out', $db_name,
        '-title', $db_name
    );

    open my $pipe, '|-', @cmd or die "Cannot open pipe to makeblastdb: $!\n";
    for my $result (@$results) {
        my $seqid = $result->[2];
        my $sequence = $result->[3];
        print $pipe ">$seqid\n$sequence\n";
    }
    close $pipe or die "makeblastdb failed: $!\n";
}

# 子プロセス（--mode=minimum の場合）
sub process_single_sequence_blastdb_alias {
    my ($fasta_entry, $query_number, $output_info, $metadata) = @_;

    my $results = search_sequence(...);

    # 直接 blastdb_aliastool にパイプ出力
    my $bsl_file = sprintf("%s_%d.bsl", $output_info->{prefix}, $query_number);
    my $nal_file = sprintf("%s_%d", $output_info->{prefix}, $query_number);

    # BSL作成
    my @bsl_cmd = (
        'blastdb_aliastool',
        '-seqid_dbtype', 'nucl',
        '-seqid_db', $output_info->{seqid_db},
        '-seqid_file_in', '/dev/stdin',
        '-seqid_file_out', $bsl_file
    );

    open my $pipe, '|-', @bsl_cmd or die "Cannot open pipe: $!\n";
    for my $result (@$results) {
        my @seqids = split(/,/, $result->[2]);
        for my $seqid (@seqids) {
            $seqid =~ s/:\d+:\d+$//;
            print $pipe "$seqid\n";
        }
    }
    close $pipe or die "blastdb_aliastool failed: $!\n";

    # NAL作成
    system('blastdb_aliastool', '-dbtype', 'nucl', '-db', $output_info->{seqid_db},
           '-seqidlist', $bsl_file, '-out', $nal_file) == 0
        or die "blastdb_aliastool (NAL) failed: $!\n";
}
```

**改善案B: 単一ファイル形式（TSV.gz等の圧縮）**:

子プロセスが圧縮一時ファイルを作成し、親プロセスがバイナリ連結。

```perl
# 親プロセスでの出力処理
# 圧縮ファイルをバイナリモードで連結（展開しない）
open my $temp_fh, '<:raw', $temp_file or die ...;
binmode $output_fh;
while (read($temp_fh, my $buffer, 65536)) {
    print $output_fh $buffer;
}
close $temp_fh;
unlink $temp_file;
```

**改善案C: 単一ファイル形式（TSV非圧縮）**:

子プロセスが非圧縮一時ファイルを作成し、親プロセスがテキスト連結。

```perl
# 親プロセスでの出力処理
open my $temp_fh, '<', $temp_file or die ...;
while (my $line = <$temp_fh>) {
    print $output_fh $line;
}
close $temp_fh;
unlink $temp_file;
```

**この設計のメリット**:
1. 圧縮処理は子プロセスで1回だけ
2. 親プロセスは展開せずにバイナリ連結または移動のみ
3. multiファイル形式ではrename()で瞬時に完了（I/Oなし）
4. 一時ファイルも圧縮されるためディスク使用量が大幅削減

**該当箇所** (kafsssearch.pl):
- `process_single_sequence` 関数（886-946行目）: 一時ファイル書き込み処理を圧縮対応に変更
- `process_sequences_parallel_streaming` 関数: 出力形式に応じた処理分岐を追加

**該当箇所** (kafsssearchclient.pl):
- `process_single_sequence_client` 関数: 一時ファイル書き込み処理を圧縮対応に変更
- `process_sequences_parallel_streaming` 関数: 出力形式に応じた処理分岐を追加

### 3. コード重複の解消（優先度: 中）

**現状の問題**:
- 非ブロッキングwait後の処理とブロッキングwait後の処理がほぼ同一
- 出力処理も重複

**該当箇所** (kafsssearch.pl):
- 非ブロッキングwait後: 746-783行目
- ブロッキングwait後: 816-853行目
- 出力処理: 785-808行目、857-880行目

**該当箇所** (kafsssearchclient.pl):
- 非ブロッキングwait後: 463-500行目
- ブロッキングwait後: 533-582行目
- 出力処理: 502-536行目、585-619行目

**改善案**:
- 子プロセス完了処理をヘルパー関数に抽出
- 出力処理もヘルパー関数に抽出

```perl
sub handle_completed_child {
    my ($pid, $active_children, $completed_results) = @_;
    # 子プロセス完了時の共通処理
}

sub output_completed_results {
    my ($completed_results, $next_query, $output_fh, $output_handles, $compression_type) = @_;
    # 出力処理の共通化
}
```

### 4. 出力処理のヘルパー関数整理（優先度: 中）

**適用対象**: kafsssearch.pl, kafsssearchclient.pl 両方

**改善案**:
- 圧縮出力を透過的に扱うヘルパー関数を整理
- 解凍関数は不要（展開しないため）

```perl
# 圧縮対応の出力ファイルを開く
sub open_output_with_optional_compression {
    my ($file, $compression_type) = @_;
    if ($compression_type) {
        my $cmd = get_compression_command($compression_type);
        open my $fh, '|-', "$cmd > '$file'" or die "Cannot open compressed output '$file': $!\n";
        return $fh;
    } else {
        open my $fh, '>', $file or die "Cannot open '$file': $!\n";
        return $fh;
    }
}

# バイナリ読み込み用（圧縮ファイルの連結用）
sub open_file_binary_read {
    my ($file) = @_;
    open my $fh, '<:raw', $file or die "Cannot open '$file': $!\n";
    return $fh;
}

# テキスト読み込み用（非圧縮ファイルの連結用）
sub open_file_text_read {
    my ($file) = @_;
    open my $fh, '<', $file or die "Cannot open '$file': $!\n";
    return $fh;
}

# バイナリファイルの連結（圧縮ファイル用）
sub concatenate_binary_file {
    my ($src_file, $dst_fh) = @_;
    open my $src_fh, '<:raw', $src_file or die "Cannot open '$src_file': $!\n";
    while (read($src_fh, my $buffer, 65536)) {
        print $dst_fh $buffer;
    }
    close $src_fh;
}
```

### 5. process_temp_file_for_multifile の簡素化（優先度: 中）

**現状の問題**:
- `process_temp_file_for_multifile` は一時ファイルを読み込んで最終ファイルに書き出す
- 読み込み→書き出しはI/O的に無駄

**改善案**:
- 一時ファイルを最終ファイルにリネーム（rename）するだけに簡素化
- I/Oをほぼゼロに削減

```perl
sub finalize_multifile_output {
    my ($temp_file, $query_number, $output_info) = @_;

    my $final_file = sprintf("%s_%d%s",
        $output_info->{prefix},
        $query_number,
        $output_info->{extension}
    );

    rename($temp_file, $final_file)
        or die "Cannot rename $temp_file to $final_file: $!\n";
}
```

**該当箇所** (kafsssearch.pl): 1662-1704行目 → 上記に置換
**該当箇所** (kafsssearchclient.pl): 該当関数 → 上記に置換

**注意**: `rename()` は同一ファイルシステム内でのみ動作。
`TMPDIR` が異なるファイルシステムの場合は `File::Copy::move()` を使用。

### 6. 圧縮形式判定の統一（優先度: 低）

**適用対象**: kafsssearch.pl, kafsssearchclient.pl 両方

**現状の問題**:
- `$outfmt` の圧縮判定が複数箇所に散在
- `get_base_format()` と `get_compression_type()` を毎回呼び出している

**改善案**:
- スクリプト開始時に一度だけ判定し、グローバル変数または構造体に保存
- 各関数では保存された値を参照

```perl
# スクリプト開始時に一度だけ判定
my $output_info = {
    base_format => get_base_format($outfmt),
    compression_type => get_compression_type($outfmt),
    is_multifile => ($outfmt =~ /^multi/i || $outfmt eq 'BLASTDB'),
    extension => get_output_extension($outfmt),
};
```

### 7. 単一スレッド処理の廃止（優先度: 中）

**適用対象**: kafsssearch.pl, kafsssearchclient.pl 両方

**現状の問題**:
- `--numthreads=1` の場合に `process_sequences_single_threaded` 関数を使用
- `--numthreads>=2` の場合に `process_sequences_parallel_streaming` 関数を使用
- 2つの異なるコードパスがあり、保守が困難
- 単一スレッド専用関数は並列処理の改善が適用されない

**改善案**:
- `process_sequences_single_threaded` 関数を廃止
- `--numthreads=1` でも `process_sequences_parallel_streaming` を使用
- fork()のオーバーヘッドは無視できるレベル

**該当箇所** (kafsssearch.pl):
- 345-351行目: 条件分岐を削除、常に並列処理関数を使用
- 669-702行目: `process_sequences_single_threaded` 関数を削除

**該当箇所** (kafsssearchclient.pl):
- 同様の条件分岐と関数を削除

### 8. 結果のストリーミング書き込み（優先度: 高）

**適用対象**: kafsssearch.pl のみ（kafsssearchclient.pl はサーバーから結果を受信するため対象外）

**現状の問題**:
- `search_sequence_with_validation` 関数が全結果を配列に格納してから返す
- 結果が大量の場合、メモリを大量に消費
- その後、配列をループして一時ファイルに書き込む

**改善案**:
- DBカーソルから取得しながら直接一時ファイルに書き込む
- 結果を配列に保持しない

```perl
# 現状（非効率）
my @results;
while (my $row = $sth->fetchrow_arrayref) {
    push @results, [@$row];
}
return \@results;

# 改善後（ストリーミング）
my $fh = open_output_with_optional_compression($temp_file, $compression_type);
my $count = 0;
while (my $row = $sth->fetchrow_arrayref) {
    print $fh join("\t", @$row) . "\n";
    $count++;
}
close $fh;
return $count;
```

**該当箇所** (kafsssearch.pl):
- `search_sequence_with_validation` 関数（948-1059行目）を `search_and_write_results` に変更
- `process_single_sequence` 関数（886-946行目）を簡素化

## 廃止する関数

以下の関数は改善後に不要となるため削除する：

### kafsssearch.pl

| 関数名 | 行番号 | 廃止理由 |
|--------|--------|----------|
| `process_sequences_single_threaded` | 669-702 | 並列処理に統一（項目7） |
| `process_temp_file_for_multifile` | 1662-1704 | rename()に置き換え（項目5） |
| `write_results_to_file` | 1597-1660 | 子プロセスが直接出力（項目2, 8） |

### kafsssearchclient.pl

| 関数名 | 行番号 | 廃止理由 |
|--------|--------|----------|
| `process_temp_file_for_multifile` | 1866-1908 | rename()に置き換え（項目5） |
| `write_results_to_file_multi` | 1803-1864 | 子プロセスが直接出力（項目2） |

## 実装順序

1. **項目1（二重読み込み廃止）** - 最も効果が高く、他の改善の前提となる
2. **項目8（ストリーミング書き込み）** - メモリ効率とI/O効率の大幅改善
3. **項目2（一時ファイル圧縮）** - I/O削減効果が大きい
4. **項目4（ヘルパー関数）** - 項目2の実装を簡潔にする
5. **項目7（単一スレッド廃止）** - コード統一
6. **項目3（コード重複解消）** - 保守性向上
7. **項目5（multifile効率化）** - 圧縮対応の完成
8. **項目6（判定統一）** - 細かい最適化

## テスト計画

1. 各出力形式（TSV, multiTSV, FASTA, multiFASTA, BLASTDB）での動作確認
2. 各圧縮形式（.gz, .bz2, .xz, .zst）での動作確認
3. `--numthreads` の各値（1, 4, 16, 32）での並列性確認
4. 大規模データでのベンチマーク比較
5. `TMPDIR=/dev/shm` との組み合わせテスト

## 期待される効果

### multiファイル形式（multiTSV.gz, multiFASTA.gz 等）の場合
- **一時ファイルサイズ**: 圧縮により1/5〜1/10に削減
- **最終出力**: rename() のみでI/Oほぼゼロ
- **圧縮処理**: 子プロセスで1回のみ
- **従来比**: 書き込み2回+読み込み2回 → 書き込み1回+rename

### BLASTDB形式の場合
- **一時ファイル**: 完全廃止
- **最終出力**: 子プロセスが直接作成（makeblastdb/blastdb_aliastoolへパイプ）
- **従来比**: 書き込み2回+読み込み2回 → 直接出力1回

### 単一ファイル形式（TSV.gz 等の圧縮）の場合
- **一時ファイルサイズ**: 圧縮により1/5〜1/10に削減
- **一時ファイル読み込み**: 2回 → 1回（50%削減）
- **圧縮処理**: 子プロセスで1回のみ（展開せずバイナリ連結）

### 単一ファイル形式（TSV 非圧縮）の場合
- **一時ファイル読み込み**: 2回 → 1回（50%削減）

### ストリーミング書き込み（kafsssearch.pl）の場合
- **メモリ使用量**: 結果を配列に保持しないため大幅削減
- **レイテンシ**: DB取得と同時に書き込み開始

### 共通
- 並列処理の実効性向上（32スレッド指定時に実際に32並列で動作）
- ディスクI/Oボトルネック（balance_dirty_pages）の大幅な緩和
- 一時ファイルの圧縮によりTMPDIR=/dev/shm使用時のメモリ消費削減
- コード簡素化による保守性向上
- 廃止関数により総コード行数削減（約200行削減見込み）
