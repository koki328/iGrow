#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
③ 基準価額（NAV）履歴取得スクリプト
- isin_mapping.csv に記載された全ファンドの NAV 履歴を取得
- データソース: toushin-lib.fwg.ne.jp (csv-file-download)
- 出力: nav_history.csv
  列: 日付, ファンド名, 基準価額
"""

import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

import time
from pathlib import Path
from io import BytesIO

import requests
import pandas as pd

# ==========================================
# パス設定
# ==========================================
BASE_DIR    = Path(__file__).parent.parent
DATA_DIR    = BASE_DIR / "データ"
ISIN_CSV    = DATA_DIR / "isin_mapping.csv"
OUTPUT_CSV  = DATA_DIR / "nav_history.csv"

# ==========================================
# 定数
# ==========================================
TLIB      = "https://toushin-lib.fwg.ne.jp"
NISA_INIT = f"{TLIB}/FdsWeb/FDST999903"
DETAIL    = f"{TLIB}/FdsWeb/FDST030000"
CSV_DL    = f"{TLIB}/FdsWeb/FDST030000/csv-file-download"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ja,en;q=0.9",
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)


# ==========================================
# NAV CSV 取得
# ==========================================
def fetch_nav(fund_name: str, isin: str, assoc_cd: str) -> pd.DataFrame | None:
    """
    toushin-lib から NAV 履歴 CSV を取得して DataFrame を返す。
    列: 日付 (date), 基準価額 (int)
    """
    # 詳細ページを先にアクセスして Cookie を取得
    SESSION.get(f"{DETAIL}?isinCd={isin}", timeout=15)

    r = SESSION.get(
        CSV_DL,
        params={"isinCd": isin, "associFundCd": assoc_cd},
        timeout=30,
    )

    if r.status_code != 200 or len(r.content) < 500:
        print(f"  ⚠ 取得失敗: status={r.status_code}, size={len(r.content)}")
        return None

    # Shift-JIS で CSV 解析
    try:
        df = pd.read_csv(BytesIO(r.content), encoding="shift-jis")
    except UnicodeDecodeError:
        df = pd.read_csv(BytesIO(r.content), encoding="utf-8")

    # 列名確認
    if "年月日" not in df.columns or "基準価額(円)" not in df.columns:
        print(f"  ⚠ 想定外の列名: {df.columns.tolist()}")
        return None

    # 日付パース: 「2018年07月03日」→ date
    df["日付"] = pd.to_datetime(df["年月日"], format="%Y年%m月%d日").dt.date
    df["基準価額"] = pd.to_numeric(df["基準価額(円)"], errors="coerce")
    df["ファンド名"] = fund_name

    df = df[["日付", "ファンド名", "基準価額"]].dropna(subset=["基準価額"])
    df["基準価額"] = df["基準価額"].astype(int)

    return df


# ==========================================
# メイン
# ==========================================
# セッション確立
print("セッション初期化中...")
try:
    SESSION.get(NISA_INIT, timeout=15)
    jsid = SESSION.cookies.get("JSESSIONID", "")
    print(f"  JSESSIONID={jsid[:8]}...")
except Exception as e:
    print(f"  警告: セッション初期化失敗: {e}")

# isin_mapping.csv 読み込み
df_isin = pd.read_csv(ISIN_CSV, encoding="utf-8-sig")
print(f"\n対象ファンド: {len(df_isin)}本")

all_nav = []

for _, row in df_isin.iterrows():
    fund_name = row["ファンド名"]
    isin      = row["ISIN"]
    assoc_cd  = str(row["協会コード"]) if pd.notna(row["協会コード"]) else None

    print(f"\n[{fund_name}]")
    print(f"  ISIN={isin}, assocCd={assoc_cd}")

    if not isin or not assoc_cd:
        print("  ⚠ ISIN または assocCd が未取得。スキップ。")
        continue

    df_nav = fetch_nav(fund_name, isin, assoc_cd)

    if df_nav is not None:
        print(f"  ✅ {len(df_nav)}件取得 ({df_nav['日付'].min()} 〜 {df_nav['日付'].max()})")
        all_nav.append(df_nav)
    else:
        print(f"  ❌ 取得失敗")

    time.sleep(0.5)

# ==========================================
# 結合・出力
# ==========================================
if not all_nav:
    print("\n⚠ 取得データなし。終了。")
    sys.exit(1)

df_all = pd.concat(all_nav, ignore_index=True)
df_all = df_all.sort_values(["ファンド名", "日付"]).reset_index(drop=True)

df_all.to_csv(OUTPUT_CSV, encoding="utf-8-sig", index=False)

print(f"\n{'='*60}")
print(f"出力: {OUTPUT_CSV.name}")
print(f"総レコード数: {len(df_all)}")
print(f"\nファンド別件数:")
for name, grp in df_all.groupby("ファンド名"):
    print(f"  {name}: {len(grp)}件 ({grp['日付'].min()} 〜 {grp['日付'].max()})")

print("\n✅ NAV 履歴取得完了")
