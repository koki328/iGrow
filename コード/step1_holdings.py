#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
楽天証券 投資信託 取引履歴解析
① 取引履歴Excelから各時点の保有口数・元本推移を計算し出力
"""

import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

import pandas as pd
import numpy as np
from pathlib import Path

# ==========================================
# 列名定数 (unicode escape で文字化け回避)
# ==========================================
C_DATE    = "\u7d04\u5b9a\u65e5"                    # 約定日
C_SETTLE  = "\u53d7\u6e21\u65e5"                    # 受渡日
C_FUND    = "\u30d5\u30a1\u30f3\u30c9\u540d"        # ファンド名
C_ACCT    = "\u53e3\u5ea7\u533a\u5206"              # 口座区分
C_TXTYPE  = "\u53d6\u5f15\u533a\u5206"              # 取引区分
C_METHOD  = "\u53d6\u5f15\u65b9\u6cd5"              # 取引方法
C_CALCM   = "\u8a08\u7b97\u65b9\u6cd5"              # 計算方法
C_NAV     = "\u57fa\u6e96\u4fa1\u984d_\u4e07\u53e3" # 基準価額_万口
C_UNITS   = "\u53e3\u6570"                           # 口数
C_PTS_NAV = "\u30dd\u30a4\u30f3\u30c8\u5229\u7528_\u4e07\u53e3"  # ポイント利用_万口
C_PTS_AMT = "\u30dd\u30a4\u30f3\u30c8\u7b49\u5229\u7528\u984d"   # ポイント等利用額
C_PROFIT  = "\u5229\u76ca\u984d"                     # 利益額
C_AMOUNT  = "\u53d6\u5f15\u91d1\u984d"              # 取引金額
C_TAX     = "\u8ab2\u7a0e\u533a\u5206"              # 課税区分

# df_hist 用の列名
H_TX_UNITS   = "\u53d6\u5f15\u53e3\u6570"                              # 取引口数
H_EXEC_NAV   = "\u7d04\u5b9a\u6642\u57fa\u6e96\u4fa1\u984d_\u4e07\u53e3"  # 約定時基準価額_万口
H_HOLD_UNITS = "\u4fdd\u6709\u53e3\u6570"                              # 保有口数
H_PRINCIPAL  = "\u7d2f\u7a4d\u5143\u672c"                              # 累積元本
H_AVG_NAV    = "\u5e73\u5747\u53d6\u5f97\u4fa1\u984d_\u4e07\u53e3"    # 平均取得価額_万口

# df_daily 用
D_DATE       = "\u65e5\u4ed8"                        # 日付

# ==========================================
# パス設定
# ==========================================
BASE_DIR  = Path(__file__).parent.parent
DATA_DIR  = BASE_DIR / "\u30c7\u30fc\u30bf"          # データ/
XLSX_PATH = DATA_DIR / "tradehistory(INVST)_20260303.xlsx"
OUT_HIST  = DATA_DIR / "holdings_history.csv"
OUT_DAILY = DATA_DIR / "holdings_daily.csv"

# ==========================================
# Excel 読み込み
# ==========================================
print("=" * 60)
print("取引履歴を読み込み中...")

df_raw = pd.read_excel(XLSX_PATH, header=0, dtype=str)
print(f"  行数: {len(df_raw)}, 列数: {len(df_raw.columns)}")

# 列名を定数で置き換え
COL_14 = [C_DATE, C_SETTLE, C_FUND, C_CALCM, C_ACCT,
          C_TXTYPE, C_METHOD, C_UNITS, C_NAV,
          C_PTS_NAV, C_PTS_AMT, C_PROFIT, C_AMOUNT, C_TAX]
COL_13 = [C_DATE, C_SETTLE, C_FUND, C_CALCM, C_ACCT,
          C_TXTYPE, C_METHOD, C_UNITS, C_NAV,
          C_PTS_NAV, C_PTS_AMT, C_AMOUNT, C_TAX]

if len(df_raw.columns) == 14:
    df_raw.columns = COL_14
elif len(df_raw.columns) == 13:
    df_raw.columns = COL_13
else:
    raise ValueError(f"予期しない列数: {len(df_raw.columns)}")

# ==========================================
# 型変換
# ==========================================
df_raw[C_DATE]   = pd.to_datetime(df_raw[C_DATE],   errors="coerce")
df_raw[C_SETTLE] = pd.to_datetime(df_raw[C_SETTLE], errors="coerce")

def to_num(series):
    return pd.to_numeric(
        series.astype(str).str.replace(",", "", regex=False).str.strip(),
        errors="coerce"
    )

df_raw[C_UNITS]  = to_num(df_raw[C_UNITS])
df_raw[C_NAV]    = to_num(df_raw[C_NAV])
df_raw[C_AMOUNT] = to_num(df_raw[C_AMOUNT])

df_raw = df_raw.dropna(subset=[C_DATE]).reset_index(drop=True)
print(f"  有効行数: {len(df_raw)}")

# 確認表示
print("\n取引区分の内訳:")
print(df_raw[C_TXTYPE].value_counts().to_string())
print("\nファンド一覧:")
for f in df_raw[C_FUND].unique():
    print(f"  {f}")

# ==========================================
# ① 保有口数・元本推移（平均取得法）
# ==========================================
# 買付 = 購入, 解約 = 売却, 再投資 = 買付の一種（元本に加算）
BUY_TYPES  = {"買付", "再投資"}
SELL_TYPES = {"解約"}

df_trades = (
    df_raw[df_raw[C_TXTYPE].isin(BUY_TYPES | SELL_TYPES)]
    .sort_values([C_FUND, C_ACCT, C_DATE])
    .reset_index(drop=True)
)

records = []

for (fund, account), grp in df_trades.groupby([C_FUND, C_ACCT], sort=False):
    grp = grp.sort_values(C_DATE).reset_index(drop=True)

    cum_units     = 0.0
    cum_principal = 0.0

    for _, row in grp.iterrows():
        units  = float(row[C_UNITS])
        amount = float(row[C_AMOUNT])
        tx     = row[C_TXTYPE]

        if tx in BUY_TYPES:
            cum_units     += units
            cum_principal += amount
        elif tx in SELL_TYPES:
            if cum_units > 0:
                reduction = cum_principal * (units / cum_units)
            else:
                reduction = 0.0
            cum_units     -= units
            cum_principal -= reduction

        avg_nav = (cum_principal / cum_units * 10_000) if cum_units > 0 else 0.0

        records.append({
            C_DATE:      row[C_DATE],
            C_SETTLE:    row[C_SETTLE],
            C_FUND:      fund,
            C_ACCT:      account,
            C_TXTYPE:    tx,
            H_EXEC_NAV:  row[C_NAV],
            H_TX_UNITS:  units if tx in BUY_TYPES else -units,
            C_AMOUNT:    amount,
            H_HOLD_UNITS: round(cum_units, 3),
            H_PRINCIPAL:  round(cum_principal, 0),
            H_AVG_NAV:    round(avg_nav, 2),
        })

df_hist = pd.DataFrame(records)

# ==========================================
# 分配情報（参考）
# ==========================================
df_dist = df_raw[df_raw[C_TXTYPE] == "\u518d\u6295\u8cc7"].copy()  # 再投資
if len(df_dist) > 0:
    print("\n=== 再投資（分配再投資）===")
    print(df_dist[[C_DATE, C_FUND, C_ACCT, C_NAV, C_UNITS, C_AMOUNT]].to_string(index=False))

# ==========================================
# 結果表示
# ==========================================
print("\n" + "=" * 60)
print("各時点の保有口数・元本推移")
print("=" * 60)

for (fund, account), grp in df_hist.groupby([C_FUND, C_ACCT]):
    print(f"\n【{fund}】({account})")
    disp = grp[[C_DATE, C_TXTYPE, H_TX_UNITS, C_AMOUNT,
                H_HOLD_UNITS, H_PRINCIPAL, H_AVG_NAV]].copy()
    disp[C_DATE] = disp[C_DATE].dt.strftime("%Y/%m/%d")
    print(disp.to_string(index=False))

    latest = grp.iloc[-1]
    print(
        f"  → 最新保有: {latest[H_HOLD_UNITS]:>12,.0f} 口 | "
        f"元本: \u00a5{latest[H_PRINCIPAL]:>12,.0f} | "
        f"平均取得価額: {latest[H_AVG_NAV]:>8,.2f} 円/万口"
    )

# ==========================================
# 日次展開（後工程用）
# ==========================================
print("\n日次データを作成中...")
all_daily = []

for (fund, account), grp in df_hist.groupby([C_FUND, C_ACCT]):
    start = grp[C_DATE].min()
    end   = pd.Timestamp.today().normalize()
    idx   = pd.date_range(start, end, freq="D")

    sub = grp[[C_DATE, H_HOLD_UNITS, H_PRINCIPAL, H_AVG_NAV]].set_index(C_DATE)
    daily = sub.reindex(idx).ffill()
    daily.index.name = D_DATE
    daily[C_FUND] = fund
    daily[C_ACCT] = account
    all_daily.append(daily.reset_index())

df_daily = pd.concat(all_daily, ignore_index=True)
df_daily = df_daily[[D_DATE, C_FUND, C_ACCT, H_HOLD_UNITS, H_PRINCIPAL, H_AVG_NAV]]
print(f"  日次データ: {len(df_daily):,} 行")

# ==========================================
# CSV 出力（UTF-8 BOM付き → Excel で直接開ける）
# ==========================================
df_hist.to_csv(OUT_HIST,  encoding="utf-8-sig", index=False)
df_daily.to_csv(OUT_DAILY, encoding="utf-8-sig", index=False)

print(f"\n出力完了:")
print(f"  取引ベース推移 → {OUT_HIST.name}")
print(f"  日次展開データ → {OUT_DAILY.name}")
