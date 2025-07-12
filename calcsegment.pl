#!/usr/bin/env perl
use strict;
use warnings;
use POSIX qw(floor);

# パラメータ設定
my $seqlen = 10000;        # 元の塩基配列長
my $minsplitlen = 1500;    # 分割後の長さの下限
my $ovllen = 200;          # オーバーラップ長

# コマンドライン引数から値を取得（オプション）
if (@ARGV >= 3) {
    $seqlen = $ARGV[0];
    $minsplitlen = $ARGV[1];
    $ovllen = $ARGV[2];
}

# 入力値の妥当性チェック
if ($seqlen <= 0 || $minsplitlen <= 0 || $ovllen < 0) {
    die "エラー: 不正な入力値です。seqlen > 0, minsplitlen > 0, ovllen >= 0 である必要があります。\n";
}

if ($minsplitlen <= $ovllen) {
    die "エラー: 分割後の最小長がオーバーラップ長以下です。minsplitlen > ovllen である必要があります。\n";
}

if ($seqlen < $minsplitlen) {
    die "エラー: 元の配列長が最小分割長より短いです。\n";
}

# 最適分割数の直接計算（forループ不要）
sub calculate_optimal_split_direct {
    my ($seqlen, $minsplitlen, $ovllen) = @_;
    
    # 制約式: minsplitlen * nsplit - ovllen * (nsplit - 1) <= seqlen
    # 変形: nsplit * (minsplitlen - ovllen) <= seqlen - ovllen
    # 最大nsplit: floor((seqlen - ovllen) / (minsplitlen - ovllen))
    
    my $effective_unit = $minsplitlen - $ovllen;
    my $nsplit = floor(($seqlen - $ovllen) / $effective_unit);
    
    # 最小値は1
    $nsplit = 1 if $nsplit < 1;
    
    # セグメント長を計算
    my ($splitlenmax, $splitlenmin);
    
    if ($nsplit == 1) {
        $splitlenmax = $splitlenmin = $seqlen;
    } else {
        my $total_effective_length = $seqlen + ($nsplit - 1) * $ovllen;
        my $base_length = floor($total_effective_length / $nsplit);
        my $remainder = $total_effective_length % $nsplit;
        
        $splitlenmin = $base_length;
        $splitlenmax = $base_length + ($remainder > 0 ? 1 : 0);
    }
    
    return ($nsplit, $splitlenmax, $splitlenmin);
}

# 分割境界値の計算と表示
sub show_split_boundaries {
    my ($minsplitlen, $ovllen) = @_;
    
    print "分割数の境界値:\n";
    print "nsplit\t必要最小長\t適用範囲\n";
    print "-" x 50 . "\n";
    
    for my $n (1..10) {  # 最初の10分割まで表示
        my $min_required = $minsplitlen * $n - $ovllen * ($n - 1);
        my $next_min_required = $minsplitlen * ($n + 1) - $ovllen * $n;
        
        if ($n == 1) {
            printf "%5d\t%10d\t%d <= seqlen < %d\n", 
                   $n, $min_required, $minsplitlen, $next_min_required;
        } else {
            printf "%5d\t%10d\t%d <= seqlen < %d\n", 
                   $n, $min_required, $min_required, $next_min_required;
        }
    }
    print "\n";
}

# 数式による計算過程の表示
sub show_calculation_process {
    my ($seqlen, $minsplitlen, $ovllen, $nsplit) = @_;
    
    my $effective_unit = $minsplitlen - $ovllen;
    
    print "計算過程:\n";
    print "  制約式: minsplitlen * nsplit - ovllen * (nsplit - 1) <= seqlen\n";
    print "  変形1: nsplit * (minsplitlen - ovllen) + ovllen <= seqlen\n";
    print "  変形2: nsplit * (minsplitlen - ovllen) <= seqlen - ovllen\n";
    print "  変形3: nsplit <= (seqlen - ovllen) / (minsplitlen - ovllen)\n\n";
    
    print "  実効単位長 = minsplitlen - ovllen = $minsplitlen - $ovllen = $effective_unit\n";
    print "  最大nsplit = floor((seqlen - ovllen) / 実効単位長)\n";
    print "             = floor(($seqlen - $ovllen) / $effective_unit)\n";
    print "             = floor(" . ($seqlen - $ovllen) . " / $effective_unit)\n";
    print "             = floor(" . sprintf("%.2f", ($seqlen - $ovllen) / $effective_unit) . ")\n";
    print "             = $nsplit\n\n";
}

# メイン処理
print "DNA塩基配列分割最適化（数式直接計算版）\n";
print "=" x 50 . "\n";
print "入力パラメータ:\n";
print "  元の配列長 (seqlen): $seqlen\n";
print "  最小分割長 (minsplitlen): $minsplitlen\n";
print "  オーバーラップ長 (ovllen): $ovllen\n\n";

# 分割境界値を表示
show_split_boundaries($minsplitlen, $ovllen);

# 最適解を直接計算（O(1)）
my ($nsplit, $splitlenmax, $splitlenmin) = calculate_optimal_split_direct($seqlen, $minsplitlen, $ovllen);

# 計算過程を表示
show_calculation_process($seqlen, $minsplitlen, $ovllen, $nsplit);

# 結果を出力
print "最適化結果:\n";
print "  最適分割数 (nsplit): $nsplit\n";
print "  最大セグメント長 (splitlenmax): $splitlenmax\n";
print "  最小セグメント長 (splitlenmin): $splitlenmin\n";
print "  長さの差: " . ($splitlenmax - $splitlenmin) . "\n\n";

# 制約条件の検証
my $required_min_length = $minsplitlen * $nsplit - $ovllen * ($nsplit - 1);
print "制約条件の検証:\n";
print "  必要最小総長 = $minsplitlen * $nsplit - $ovllen * " . ($nsplit - 1) . " = $required_min_length\n";
print "  実際の配列長 = $seqlen\n";
print "  制約満足: " . ($required_min_length <= $seqlen ? "✓ ($seqlen >= $required_min_length)" : "✗") . "\n";
print "  最小セグメント長: $splitlenmin >= $minsplitlen " . ($splitlenmin >= $minsplitlen ? "✓" : "✗") . "\n\n";

# ユーザー例での検証
if ($minsplitlen == 50000 && $ovllen == 500) {
    print "ユーザー例での検証 (minsplitlen=50000, ovllen=500):\n";
    print "  seqlen < 99500: nsplit = 1\n";
    print "  99500 <= seqlen < 149000: nsplit = 2\n";
    print "  149000 <= seqlen < 198500: nsplit = 3\n";
    print "  現在のseqlen = $seqlen\n";
    
    if ($seqlen < 99500) {
        print "  → nsplit = 1 (分割なし)\n";
    } elsif ($seqlen < 149000) {
        print "  → nsplit = 2 (2分割)\n";
    } elsif ($seqlen < 198500) {
        print "  → nsplit = 3 (3分割)\n";
    } else {
        print "  → nsplit = 4以上\n";
    }
    print "\n";
}

# Perl変数として出力
print "Perl変数での出力:\n";
print "\$nsplit = $nsplit;\n";
print "\$splitlenmax = $splitlenmax;\n";
print "\$splitlenmin = $splitlenmin;\n";

print "\n# forループは完全に不要でした！数式一発で計算完了。\n";

exit 0;
