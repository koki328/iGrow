#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
④ 日次資産評価額・損益計算スクリプト
- holdings_daily.csv × nav_history.csv を結合
- 評価額 = 保有口数 × 基準価額 / 10000
- 損益額 = 評価額 - 累積元本
- 損益率 = 損益額 / 累積元本 × 100
- NAV 非営業日は直前営業日の基準価額を使用 (前方補完, 最大7日)
- 出力: pnl_daily.csv
"""

import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

from pathlib import Path
import pandas as pd
import numpy as np

# ==========================================
# パス設定
# ==========================================
BASE_DIR    = Path(__file__).parent.parent
DATA_DIR    = BASE_DIR / "データ"
HOLDINGS    = DATA_DIR / "holdings_daily.csv"
NAV_HIST    = DATA_DIR / "nav_history.csv"
OUTPUT_CSV  = DATA_DIR / "pnl_daily.csv"

# ==========================================
# データ読み込み
# ==========================================
df_h = pd.read_csv(HOLDINGS, encoding="utf-8-sig")
df_n = pd.read_csv(NAV_HIST,  encoding="utf-8-sig")

df_h["日付"] = pd.to_datetime(df_h["日付"]).dt.date
df_n["日付"] = pd.to_datetime(df_n["日付"]).dt.date

print(f"holdings_daily : {len(df_h)}行, {df_h['日付'].min()} 〜 {df_h['日付'].max()}")
print(f"nav_history    : {len(df_n)}行, {df_n['日付'].min()} 〜 {df_n['日付'].max()}")

# ==========================================
# 各ファンドごとに NAV を前方補完してから結合
# ==========================================
funds = df_h["ファンド名"].unique()
results = []

for fund in funds:
    h = df_h[df_h["ファンド名"] == fund].copy()
    n = df_n[df_n["ファンド名"] == fund][["日付", "基準価額"]].copy()

    if n.empty:
        print(f"\n⚠ [{fund}] NAVデータなし。スキップ。")
        continue

    # holdings の日付範囲で NAV を前方補完
    # 1. holdings の全日付を index にした空 DataFrame を作る
    all_dates = pd.DataFrame({"日付": h["日付"].unique()})
    all_dates = all_dates.sort_values("日付").reset_index(drop=True)

    # 2. NAV を結合 → 欠損日を前方補完 (最大7営業日分)
    nav_filled = all_dates.merge(n, on="日付", how="left")
    nav_filled = nav_filled.sort_values("日付")
    nav_filled["基準価額"] = nav_filled["基準価額"].ffill(limit=7)

    # 3. holdings に基準価額を結合
    merged = h.merge(nav_filled, on="日付", how="left")

    # 4. 計算
    merged["評価額"]   = (merged["保有口数"] * merged["基準価額"] / 10000).round(0).astype("Int64")
    merged["損益額"]   = (merged["評価額"] - merged["累積元本"]).round(0).astype("Int64")
    merged["損益率(%)"] = (merged["損益額"] / merged["累積元本"] * 100).round(2)

    results.append(merged)
    print(f"\n[{fund}]")
    print(f"  保有期間: {merged['日付'].min()} 〜 {merged['日付'].max()} ({len(merged)}日)")
    nav_ok = merged["基準価額"].notna().sum()
    print(f"  NAV取得率: {nav_ok}/{len(merged)} ({nav_ok/len(merged)*100:.1f}%)")

# ==========================================
# 結合・出力
# ==========================================
if not results:
    print("\n⚠ データなし。終了。")
    sys.exit(1)

df_out = pd.concat(results, ignore_index=True)
df_out = df_out.sort_values(["日付", "ファンド名"]).reset_index(drop=True)

# 出力列順
cols = ["日付", "ファンド名", "口座区分", "保有口数", "累積元本",
        "平均取得価額_万口", "基準価額", "評価額", "損益額", "損益率(%)"]
df_out = df_out[cols]

df_out.to_csv(OUTPUT_CSV, encoding="utf-8-sig", index=False)

# ==========================================
# サマリー表示 (最新日)
# ==========================================
latest_date = df_out["日付"].max()
df_latest = df_out[df_out["日付"] == latest_date].copy()

print(f"\n{'='*65}")
print(f"最新日 ({latest_date}) のポートフォリオ")
print(f"{'='*65}")

total_cost   = df_latest["累積元本"].sum()
total_value  = df_latest["評価額"].dropna().sum()
total_pnl    = total_value - total_cost
total_pnl_r  = total_pnl / total_cost * 100 if total_cost else 0

for _, row in df_latest.iterrows():
    nav_str  = f"{int(row['基準価額']):,}" if pd.notna(row["基準価額"]) else "N/A"
    val_str  = f"¥{int(row['評価額']):,}"  if pd.notna(row["評価額"])  else "N/A"
    pnl_str  = f"¥{int(row['損益額']):+,}" if pd.notna(row["損益額"])  else "N/A"
    rate_str = f"{row['損益率(%)']:+.2f}%" if pd.notna(row["損益率(%)"]) else "N/A"
    print(f"\n  {row['ファンド名'][:30]}")
    print(f"    基準価額: {nav_str} 円/万口  評価額: {val_str}")
    print(f"    元本: ¥{int(row['累積元本']):,}  損益: {pnl_str} ({rate_str})")

print(f"\n  {'─'*50}")
print(f"  合計  評価額: ¥{int(total_value):,}")
print(f"        元本:   ¥{int(total_cost):,}")
pnl_sign = "+" if total_pnl >= 0 else ""
print(f"        損益:   ¥{pnl_sign}{int(total_pnl):,} ({total_pnl_r:+.2f}%)")

print(f"\n出力: {OUTPUT_CSV.name}  ({len(df_out)}行)")
print("✅ 損益計算完了")
