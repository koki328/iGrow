#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
② ISINコード取得スクリプト
- holdings_daily.csv のファンド名を動的に読み込み
- キャッシュ (isin_cache.json) を優先参照
- キャッシュ未登録は toushin-lib API で自動検索:
    楽天投信  → s_instCd=["1009I"] 絞り込み + NFKC名前マッチ
    その他    → 全ファンドスキャン (最大 5815 件) + NFKC名前マッチ
- toushin-lib 非対応ファンドは assocCd=null (Step3 で代替ソース使用)
- 出力: isin_mapping.csv
"""

import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

import json, re, time, unicodedata
from pathlib import Path

import requests
import pandas as pd
from bs4 import BeautifulSoup

# ==========================================
# パス設定
# ==========================================
BASE_DIR   = Path(__file__).parent.parent
DATA_DIR   = BASE_DIR / "データ"
DAILY_CSV  = DATA_DIR / "holdings_daily.csv"
CACHE_FILE = DATA_DIR / "isin_cache.json"
OUTPUT_CSV = DATA_DIR / "isin_mapping.csv"

# ==========================================
# 定数
# ==========================================
TLIB      = "https://toushin-lib.fwg.ne.jp"
NISA_INIT = f"{TLIB}/FdsWeb/FDST999903"
SEARCH    = f"{TLIB}/FdsWeb/FDST999900/fundDataSearch"
DETAIL    = f"{TLIB}/FdsWeb/FDST030000"

HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/120.0.0.0 Safari/537.36"),
    "Accept-Language": "ja,en;q=0.9",
}
AJAX = {
    **HEADERS,
    "Content-Type": "application/json",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "X-Requested-With": "XMLHttpRequest",
    "Referer": NISA_INIT,
}
BASE_REQ = {
    "s_investAssetKindCd": [], "s_investArea3kindCd": [], "s_instCd": [],
    "s_fdsInstCd": [], "s_dcFundCD": [], "t_investArea10kindCd": [],
    "t_investAssetKindCd": [], "t_instCd": [], "t_fdsInstCd": [],
    "s_investArea10kindCd": [], "s_setlFqcy": [], "s_dividend1y": [],
    "s_totalNetAssets": [], "s_nowToRedemptionDate": [], "s_establishedDateToNow": [],
    "s_isinCd": [], "startNo": 0,
}

RAKUTEN_INST_CD = "1009I"   # 楽天投信投資顧問

SESSION = requests.Session()
SESSION.headers.update(HEADERS)


# ==========================================
# ユーティリティ
# ==========================================
def normalize(s: str) -> str:
    """全角英数字・記号を半角に統一し、空白を圧縮"""
    if not s:
        return ""
    s = unicodedata.normalize("NFKC", s)
    return re.sub(r'\s+', ' ', s).strip()


def clean_fund_name(name: str) -> str:
    """
    末尾のニックネーム括弧を除去。
    例:
      楽天・インド株Nifty50インデックス・ファンド(楽天・インド株Nifty50)
        → 楽天・インド株Nifty50インデックス・ファンド
      楽天・ゴールド・ファンド(為替ヘッジなし)(楽天・ゴールド(為替ヘッジなし))
        → 楽天・ゴールド・ファンド(為替ヘッジなし)
      eMAXIS Slim 米国株式(S&P500)         → そのまま (・を含まない)
    """
    pattern = r'\([^()]*(?:\([^()]*\)[^()]*)*\)$'
    m = re.search(pattern, name)
    if m:
        content = m.group()[1:-1]
        if '\u30fb' in content:     # ・ が含まれる = ニックネーム
            name = name[:m.start()].strip()
    return name


def name_score(a: str, b: str) -> float:
    """正規化済み2文字列の類似スコア (0.0〜1.0)"""
    if not a or not b:
        return 0.0
    if a == b:
        return 1.0
    if len(a) >= 15 and len(b) >= 15 and a[:15] == b[:15]:
        return 0.9
    if len(a) >= 10 and len(b) >= 10 and a[:10] == b[:10]:
        return 0.8
    if a in b or b in a:
        return 0.7
    shorter, longer = (a, b) if len(a) <= len(b) else (b, a)
    for L in range(min(len(shorter), 15), 9, -1):
        for start in range(len(shorter) - L + 1):
            if shorter[start:start + L] in longer:
                return 0.4 + 0.4 * (L / max(len(a), len(b)))
    return 0.0


def best_match(norm_target: str, items: list[dict]) -> dict | None:
    """items リストからスコア最高のファンドを返す"""
    best, best_sc = None, 0.0
    for item in items:
        fname = item.get("fundStNm") or item.get("fundNm") or ""
        sc = name_score(norm_target, normalize(fname))
        if sc > best_sc:
            best, best_sc = item, sc
    if best and best_sc >= 0.7:
        return best
    return None


# ==========================================
# toushin-lib API
# ==========================================
def api_post(extra: dict) -> list[dict]:
    try:
        r = SESSION.post(SEARCH, json={**BASE_REQ, **extra}, headers=AJAX, timeout=30)
        if r.status_code != 200:
            return []
        si = r.json().get("searchResultInfo", {})
        return si.get("resultInfoMapList", []), int(si.get("recordsTotal", 0) or 0)
    except Exception:
        return [], 0


def search_rakuten(norm_target: str) -> dict | None:
    """楽天投信 101 件を取得してマッチ"""
    items, total = api_post({"s_instCd": [RAKUTEN_INST_CD], "startNo": 0})
    return best_match(norm_target, items)


def search_full_scan(norm_target: str) -> dict | None:
    """全 5815 件スキャンしてマッチ"""
    PAGE = 20
    _, total = api_post({"startNo": 0})
    if total == 0:
        total = 5815
    pages = (total + PAGE - 1) // PAGE
    print(f"    全スキャン開始 ({total}件, {pages}ページ)")
    for page in range(pages):
        items, _ = api_post({"startNo": page * PAGE})
        hit = best_match(norm_target, items)
        if hit:
            print(f"    page {page} でヒット")
            return hit
        time.sleep(0.08)
    return None


def get_assoc_cd_from_page(isin: str) -> str | None:
    """FDST030000 ページ (サーバーレンダリング) から #associFundCd を取得"""
    try:
        r = SESSION.get(f"{DETAIL}?isinCd={isin}", timeout=15)
        r.encoding = "utf-8"
        soup = BeautifulSoup(r.text, "html.parser")
        inp = soup.find("input", id="associFundCd")
        val = inp.get("value", "").strip() if inp else ""
        return val if val else None
    except Exception:
        return None


# ==========================================
# メイン: ISIN + assocCd 取得
# ==========================================
def resolve(fund_name: str, cache: dict) -> tuple[str | None, str | None, str]:
    """
    戻り値: (isin, assocCd, source_label)
    source: "キャッシュ" / "楽天検索" / "全スキャン" / "未取得"
    """
    # 1. キャッシュ確認 (raw名 → cleaned名)
    for key in [fund_name, clean_fund_name(fund_name)]:
        if key in cache:
            entry = cache[key]
            if isinstance(entry, dict):
                isin   = entry.get("isin")
                assoc  = entry.get("assocCd")   # None の場合あり
                return isin, assoc, "キャッシュ"
            else:  # 旧形式 (ISIN文字列のみ)
                return entry, None, "キャッシュ(旧)"

    # 2. API 検索
    clean = clean_fund_name(fund_name)
    norm  = normalize(clean)
    print(f"  API 検索: {clean!r}")

    is_rakuten = "楽天" in clean
    if is_rakuten:
        hit = search_rakuten(norm)
        src = "楽天投信検索"
    else:
        hit = search_full_scan(norm)
        src = "全スキャン"

    if not hit:
        return None, None, "未取得"

    isin  = hit.get("isinCd", "")
    assoc = hit.get("associFundCd") or None
    print(f"    ヒット: [{isin}] {hit.get('fundStNm','')}")

    # assocCd が空の場合、FDST030000 ページから取得
    if isin and not assoc:
        assoc = get_assoc_cd_from_page(isin)

    # assocCd が取れた場合は FDST030000 確認 (念の為)
    # (hit に assocCd がある場合はそのまま使う)

    # キャッシュに保存
    cache[fund_name] = {"isin": isin, "assocCd": assoc}
    return isin, assoc, src


# ==========================================
# 初期化
# ==========================================
# キャッシュ読み込み
if CACHE_FILE.exists():
    with open(CACHE_FILE, "r", encoding="utf-8") as f:
        cache: dict = json.load(f)
    print(f"キャッシュ読み込み: {len(cache)}件")
else:
    cache = {}
    print("キャッシュなし。新規作成します。")

# セッション確立 (JSESSIONID 取得)
print("\nセッション初期化中...")
try:
    SESSION.get(NISA_INIT, timeout=15)
    jsid = SESSION.cookies.get("JSESSIONID", "")
    print(f"  セッション確立: JSESSIONID={jsid[:8]}...")
except Exception as e:
    print(f"  セッション初期化失敗: {e}")

# ==========================================
# 保有ファンド一覧
# ==========================================
df = pd.read_csv(DAILY_CSV, encoding="utf-8-sig")
fund_names = df["ファンド名"].unique().tolist()

print(f"\n保有ファンド: {len(fund_names)}本")
for n in fund_names:
    print(f"  {n}")

# ==========================================
# 各ファンドの ISIN 取得
# ==========================================
print("\n" + "=" * 60)
print("ISINコード取得中...")

results = []
for fund_name in fund_names:
    print(f"\n[{fund_name}]")
    isin, assoc_cd, source = resolve(fund_name, cache)

    if isin is None:
        print(f"  ⚠ 自動取得失敗。手動入力 (Enter=スキップ):")
        try:
            user_in = input("  > ").strip()
        except (EOFError, OSError):
            user_in = ""
        if user_in:
            isin = user_in
            source = "手動入力"
            cache[fund_name] = {"isin": isin, "assocCd": None}

    status = ("✅" if isin else "❌") + (" [assocCd有]" if assoc_cd else " [assocCd無]")
    print(f"  {status}  ISIN={isin}  assocCd={assoc_cd}  [{source}]")

    results.append({
        "ファンド名":   fund_name,
        "検索用名称":   clean_fund_name(fund_name),
        "ISIN":         isin,
        "協会コード":   assoc_cd,
        "取得方法":     source,
    })
    time.sleep(0.3)

# ==========================================
# キャッシュ保存
# ==========================================
with open(CACHE_FILE, "w", encoding="utf-8") as f:
    json.dump(cache, f, ensure_ascii=False, indent=2)
print(f"\nキャッシュ保存: {CACHE_FILE.name}")

# ==========================================
# 結果出力
# ==========================================
df_out = pd.DataFrame(results)

print("\n" + "=" * 60)
print("取得結果")
print(df_out.to_string(index=False))

df_out.to_csv(OUTPUT_CSV, encoding="utf-8-sig", index=False)
print(f"\n出力: {OUTPUT_CSV.name}")

missing = df_out[df_out["ISIN"].isna()]
if len(missing) > 0:
    print(f"\n⚠ 未取得: {len(missing)}本")
    for _, row in missing.iterrows():
        print(f"  {row['ファンド名']}")
    print("\nisin_cache.json を手動編集して ISIN を追記してください。")
else:
    print("\n✅ 全ファンドの ISIN コード取得完了")
