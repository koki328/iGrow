#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
楽天証券 投資信託 ポートフォリオ管理ダッシュボード
Streamlit アプリ

起動方法:
  streamlit run コード/app.py
"""

import time
import re
import unicodedata
from io import BytesIO
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests
import streamlit as st
import json
from bs4 import BeautifulSoup

# ==========================================
# パス設定
# ==========================================
BASE_DIR   = Path(__file__).parent.parent
DATA_DIR   = BASE_DIR / "データ"
CACHE_FILE = DATA_DIR / "isin_cache.json"
PNL_CSV    = DATA_DIR / "pnl_daily.csv"
NAV_CSV    = DATA_DIR / "nav_history.csv"
HOLD_CSV   = DATA_DIR / "holdings_daily.csv"

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
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ja,en;q=0.9",
}

# ファンド名の短縮表示
FUND_SHORT = {
    "eMAXIS Slim 米国株式(S&P500)": "eMAXIS Slim S&P500",
    "日経平均高配当利回り株ファンド": "日経高配当",
    "楽天・インド株Nifty50インデックス・ファンド(楽天・インド株Nifty50)": "楽天インド株",
    "楽天・ゴールド・ファンド(為替ヘッジなし)(楽天・ゴールド(為替ヘッジなし))": "楽天ゴールド",
}

FUND_COLORS = {
    "eMAXIS Slim S&P500": "#3498db",
    "日経高配当":          "#e74c3c",
    "楽天インド株":        "#2ecc71",
    "楽天ゴールド":        "#f39c12",
}


def short(name: str) -> str:
    return FUND_SHORT.get(name, name[:20])


# ==========================================
# ① Excel → holdings DataFrame
# ==========================================
def parse_excel(file_bytes: bytes) -> pd.DataFrame:
    """取引履歴ExcelをDataFrameに変換して日次データを返す"""
    df_raw = pd.read_excel(BytesIO(file_bytes), header=0, dtype=str)

    # 列順: 約定日,受渡日,ファンド名,分配金,口座,取引,買付方法,数量[口],単価,経費,為替,受付金額,受渡金額,決済通貨
    COL_14 = ["約定日", "受渡日", "ファンド名", "分配金", "口座", "取引", "買付方法",
              "口数", "基準価額_万口", "経費", "為替レート", "受付金額", "取引金額", "決済通貨"]
    COL_13 = ["約定日", "受渡日", "ファンド名", "分配金", "口座", "取引", "買付方法",
              "口数", "基準価額_万口", "経費", "受付金額", "取引金額", "決済通貨"]

    if len(df_raw.columns) == 14:
        df_raw.columns = COL_14
    elif len(df_raw.columns) == 13:
        df_raw.columns = COL_13
    else:
        raise ValueError(f"想定外の列数: {len(df_raw.columns)}")

    def to_num(s):
        return pd.to_numeric(
            s.astype(str).str.replace(",", "", regex=False).str.strip(),
            errors="coerce"
        )

    df_raw["約定日"]      = pd.to_datetime(df_raw["約定日"], errors="coerce")
    df_raw["口数"]         = to_num(df_raw["口数"])
    df_raw["基準価額_万口"] = to_num(df_raw["基準価額_万口"])
    df_raw["取引金額"]     = to_num(df_raw["取引金額"])
    df_raw = df_raw.dropna(subset=["約定日"]).reset_index(drop=True)

    BUY_TYPES  = {"買付", "再投資"}
    SELL_TYPES = {"解約"}

    df_trades = (
        df_raw[df_raw["取引"].isin(BUY_TYPES | SELL_TYPES)]
        .sort_values(["ファンド名", "口座", "約定日"])
        .reset_index(drop=True)
    )

    records = []
    for (fund, acct), grp in df_trades.groupby(["ファンド名", "口座"], sort=False):
        grp = grp.sort_values("約定日").reset_index(drop=True)
        cum_units, cum_principal = 0.0, 0.0

        for _, row in grp.iterrows():
            units  = float(row["口数"])
            amount = float(row["取引金額"])
            tx     = row["取引"]

            if tx in BUY_TYPES:
                cum_units     += units
                cum_principal += amount
            elif tx in SELL_TYPES:
                reduction      = cum_principal * (units / cum_units) if cum_units > 0 else 0
                cum_units     -= units
                cum_principal -= reduction

            avg_nav = (cum_principal / cum_units * 10_000) if cum_units > 0 else 0

            records.append({
                "約定日":          row["約定日"],
                "ファンド名":      fund,
                "口座区分":        acct,
                "保有口数":        round(cum_units, 3),
                "累積元本":        round(cum_principal, 0),
                "平均取得価額_万口": round(avg_nav, 2),
            })

    df_hist = pd.DataFrame(records)

    # 日次展開
    all_daily = []
    for (fund, acct), grp in df_hist.groupby(["ファンド名", "口座区分"], sort=False):
        start = grp["約定日"].min()
        end   = pd.Timestamp.today().normalize()
        idx   = pd.date_range(start, end, freq="D")
        sub   = grp[["約定日", "保有口数", "累積元本", "平均取得価額_万口"]].set_index("約定日")
        daily = sub.reindex(idx).ffill()
        daily.index.name = "日付"
        daily["ファンド名"] = fund
        daily["口座区分"]   = acct
        all_daily.append(daily.reset_index())

    df_daily = pd.concat(all_daily, ignore_index=True)
    return df_daily[["日付", "ファンド名", "口座区分", "保有口数", "累積元本", "平均取得価額_万口"]]


# ==========================================
# ② ISIN 自動検索（step2 のロジック）
# ==========================================
RAKUTEN_INST_CD = "1009I"
SEARCH_URL = f"{TLIB}/FdsWeb/FDST999900/fundDataSearch"
AJAX_HEADERS = {
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


def _normalize(s: str) -> str:
    if not s:
        return ""
    s = unicodedata.normalize("NFKC", s)
    return re.sub(r'\s+', ' ', s).strip()


def _clean_fund_name(name: str) -> str:
    """末尾のニックネーム括弧（・を含む）を除去"""
    pattern = r'\([^()]*(?:\([^()]*\)[^()]*)*\)$'
    m = re.search(pattern, name)
    if m and '\u30fb' in m.group()[1:-1]:
        name = name[:m.start()].strip()
    return name


def _name_score(a: str, b: str) -> float:
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


def _best_match(norm_target: str, items: list) -> dict | None:
    best, best_sc = None, 0.0
    for item in items:
        fname = item.get("fundStNm") or item.get("fundNm") or ""
        sc = _name_score(norm_target, _normalize(fname))
        if sc > best_sc:
            best, best_sc = item, sc
    return best if best and best_sc >= 0.7 else None


def _api_post(sess: requests.Session, extra: dict) -> tuple[list, int]:
    try:
        r = sess.post(SEARCH_URL, json={**BASE_REQ, **extra},
                      headers=AJAX_HEADERS, timeout=30)
        if r.status_code != 200:
            return [], 0
        si = r.json().get("searchResultInfo", {})
        return si.get("resultInfoMapList", []), int(si.get("recordsTotal", 0) or 0)
    except Exception:
        return [], 0


def _get_assoc_cd(sess: requests.Session, isin: str) -> str | None:
    try:
        r = sess.get(f"{DETAIL}?isinCd={isin}", timeout=15)
        r.encoding = "utf-8"
        soup = BeautifulSoup(r.text, "html.parser")
        inp = soup.find("input", id="associFundCd")
        val = inp.get("value", "").strip() if inp else ""
        return val if val else None
    except Exception:
        return None


def resolve_isin(sess: requests.Session, fund_name: str,
                 msg_fn=None) -> tuple[str | None, str | None, str]:
    """
    fund_name から (isin, assocCd, source) を返す。
    msg_fn: 進捗メッセージを表示するコールバック (str → None)
    """
    clean = _clean_fund_name(fund_name)
    norm  = _normalize(clean)

    if "楽天" in clean:
        if msg_fn:
            msg_fn(f"楽天投信DB検索中: {clean[:25]}…")
        items, _ = _api_post(sess, {"s_instCd": [RAKUTEN_INST_CD], "startNo": 0})
        hit = _best_match(norm, items)
        src = "楽天投信検索"
    else:
        if msg_fn:
            msg_fn(f"全ファンドスキャン中: {clean[:25]}… (1〜2分)")
        PAGE = 20
        _, total = _api_post(sess, {"startNo": 0})
        total = total or 5815
        hit   = None
        for page in range((total + PAGE - 1) // PAGE):
            items, _ = _api_post(sess, {"startNo": page * PAGE})
            hit = _best_match(norm, items)
            if hit:
                break
            time.sleep(0.08)
        src = "全スキャン"

    if not hit:
        return None, None, "未取得"

    isin  = hit.get("isinCd", "")
    assoc = hit.get("associFundCd") or None
    if isin and not assoc:
        assoc = _get_assoc_cd(sess, isin)

    return isin, assoc, src


def search_missing_funds(fund_names: list[str], cache: dict, progress_bar) -> dict:
    """
    キャッシュ未登録のファンドを自動検索してキャッシュを更新して返す。
    """
    missing = [f for f in fund_names if f not in cache]
    if not missing:
        return cache

    sess = requests.Session()
    sess.headers.update(HEADERS)
    sess.get(NISA_INIT, timeout=15)

    for i, fund in enumerate(missing):
        progress_bar.progress(
            i / len(missing),
            text=f"ISIN検索中 ({i+1}/{len(missing)}): {short(fund)}"
        )
        isin, assoc, src = resolve_isin(
            sess, fund,
            msg_fn=lambda m: progress_bar.progress(i / len(missing), text=m)
        )
        if isin:
            cache[fund] = {"isin": isin, "assocCd": assoc}
        else:
            cache[fund] = {"isin": None, "assocCd": None}
        time.sleep(0.3)

    # ローカル環境のみファイル保存（Streamlit Cloud では書き込みが一時的）
    try:
        with open(CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

    return cache


# ==========================================
# ② ISIN キャッシュ読み込み
# ==========================================
def load_isin_cache() -> dict:
    if CACHE_FILE.exists():
        with open(CACHE_FILE, encoding="utf-8") as f:
            return json.load(f)
    return {}


# ==========================================
# ③ NAV 取得（toushin-lib）
# ==========================================
def fetch_nav_all(df_holdings: pd.DataFrame, cache: dict, progress_bar) -> pd.DataFrame:
    """全ファンドのNAV履歴をtoushin-libから取得"""
    fund_names = df_holdings["ファンド名"].unique().tolist()
    valid = [(f, cache[f]["isin"], cache[f]["assocCd"])
             for f in fund_names
             if f in cache and cache[f].get("isin") and cache[f].get("assocCd")]

    sess = requests.Session()
    sess.headers.update(HEADERS)
    sess.get(NISA_INIT, timeout=15)

    results = []
    for i, (fund, isin, assoc_cd) in enumerate(valid):
        progress_bar.progress((i) / len(valid), text=f"NAV取得中: {short(fund)}")
        try:
            sess.get(f"{DETAIL}?isinCd={isin}", timeout=15)
            r = sess.get(CSV_DL,
                         params={"isinCd": isin, "associFundCd": assoc_cd},
                         timeout=30)
            if r.status_code == 200 and len(r.content) > 500:
                df = pd.read_csv(BytesIO(r.content), encoding="shift-jis")
                df["日付"]    = pd.to_datetime(df["年月日"], format="%Y年%m月%d日").dt.date
                df["基準価額"] = pd.to_numeric(df["基準価額(円)"], errors="coerce")
                df["ファンド名"] = fund
                results.append(df[["日付", "ファンド名", "基準価額"]].dropna())
        except Exception as e:
            st.warning(f"{short(fund)} のNAV取得失敗: {e}")
        time.sleep(0.4)

    progress_bar.progress(1.0, text="計算中...")
    if not results:
        return pd.DataFrame(columns=["日付", "ファンド名", "基準価額"])
    return pd.concat(results, ignore_index=True)


# ==========================================
# ④ 損益計算
# ==========================================
def calc_pnl(df_h: pd.DataFrame, df_n: pd.DataFrame) -> pd.DataFrame:
    df_h = df_h.copy()
    df_n = df_n.copy()
    df_h["日付"] = pd.to_datetime(df_h["日付"]).dt.date
    df_n["日付"] = pd.to_datetime(df_n["日付"]).dt.date

    results = []
    for fund in df_h["ファンド名"].unique():
        h = df_h[df_h["ファンド名"] == fund].copy()
        n = df_n[df_n["ファンド名"] == fund][["日付", "基準価額"]].copy()
        if n.empty:
            continue
        all_dates  = pd.DataFrame({"日付": sorted(h["日付"].unique())})
        nav_filled = all_dates.merge(n, on="日付", how="left")
        nav_filled["基準価額"] = nav_filled["基準価額"].ffill(limit=7)
        merged = h.merge(nav_filled, on="日付", how="left")
        merged["評価額"]    = (merged["保有口数"] * merged["基準価額"] / 10_000).round(0)
        merged["損益額"]    = (merged["評価額"] - merged["累積元本"]).round(0)
        merged["損益率(%)"] = (merged["損益額"] / merged["累積元本"] * 100).round(2)
        results.append(merged)

    if not results:
        return pd.DataFrame()
    return pd.concat(results, ignore_index=True).sort_values(["日付", "ファンド名"]).reset_index(drop=True)


# ==========================================
# ⑤ リスク・リターン分析関数
# ==========================================
def calc_daily_returns(df_pnl: pd.DataFrame) -> pd.DataFrame:
    """各ファンドの日次リターンを計算（基準価額ベース）"""
    df_nav = (
        df_pnl
        .groupby(["日付", "ファンド名"])["基準価額"]
        .first()
        .reset_index()
    )
    df_wide = df_nav.pivot(index="日付", columns="ファンド名", values="基準価額")
    df_wide.columns.name = None
    df_wide = df_wide.sort_index()
    return df_wide.pct_change().dropna(how="all")


def _max_drawdown(cum_ret: pd.Series) -> float:
    dd = (cum_ret / cum_ret.cummax()) - 1
    return float(dd.min())


def calc_risk_metrics(returns: pd.DataFrame, rf_annual: float = 0.001) -> pd.DataFrame:
    """各ファンドの年率リターン・シャープ・VaR等を計算"""
    rf_daily = rf_annual / 252
    rows = []
    for col in returns.columns:
        r = returns[col].dropna()
        if len(r) < 20:
            continue
        n = len(r)
        cum        = float((1 + r).prod())
        ann_ret    = cum ** (252 / n) - 1
        ann_vol    = float(r.std() * np.sqrt(252))
        sharpe     = float((r - rf_daily).mean() / r.std() * np.sqrt(252)) if r.std() > 0 else float("nan")
        cum_s      = (1 + r).cumprod()
        mdd        = _max_drawdown(cum_s)
        var95      = float(r.quantile(0.05))
        cvar95     = float(r[r <= var95].mean())
        win_rate   = float((r > 0).mean())
        rows.append({
            "ファンド":        col,
            "年率リターン(%)": round(ann_ret  * 100, 2),
            "年率ボラ(%)":     round(ann_vol  * 100, 2),
            "シャープ":        round(sharpe,         3),
            "最大DD(%)":       round(mdd      * 100, 2),
            "VaR95(日次,%)":   round(var95    * 100, 2),
            "CVaR95(日次,%)":  round(cvar95   * 100, 2),
            "勝率(%)":         round(win_rate * 100, 1),
        })
    return pd.DataFrame(rows)


def calc_beta_alpha(returns: pd.DataFrame, benchmark_col: str) -> pd.DataFrame:
    """各ファンドの β・α（対ベンチマーク）を計算"""
    if benchmark_col not in returns.columns:
        return pd.DataFrame()
    bench = returns[benchmark_col].dropna()
    rows  = []
    for col in returns.columns:
        if col == benchmark_col:
            continue
        r      = returns[col].dropna()
        common = r.index.intersection(bench.index)
        if len(common) < 20:
            continue
        r_c = r.loc[common].values
        b_c = bench.loc[common].values
        cov  = np.cov(r_c, b_c)
        beta = cov[0, 1] / cov[1, 1] if cov[1, 1] > 0 else float("nan")
        alpha_d = float(np.mean(r_c)) - beta * float(np.mean(b_c))
        alpha_a = alpha_d * 252 * 100          # 年率 %
        ss_res  = np.sum((r_c - (alpha_d + beta * b_c)) ** 2)
        ss_tot  = np.sum((r_c - np.mean(r_c)) ** 2)
        r2      = float(1 - ss_res / ss_tot) if ss_tot > 0 else float("nan")
        te      = float(np.std(r_c - b_c) * np.sqrt(252) * 100)
        rows.append({
            "ファンド":    col,
            "β":           round(beta,    3),
            "α年率(%)":    round(alpha_a, 2),
            "R²":          round(r2,      3),
            "TE年率(%)":   round(te,      2),
        })
    return pd.DataFrame(rows)


@st.cache_data(ttl=3600)
def fetch_benchmark_yahoo(symbol: str, start_date: str) -> pd.Series | None:
    """Yahoo Finance から日次リターンを取得（キャッシュ1時間）"""
    try:
        url    = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
        params = {
            "interval": "1d",
            "period1":  int(pd.Timestamp(start_date).timestamp()),
            "period2":  int(pd.Timestamp.now().timestamp()),
        }
        r = requests.get(url, params=params, headers=HEADERS, timeout=15)
        if r.status_code != 200:
            return None
        result = r.json()["chart"]["result"][0]
        dates  = pd.to_datetime(result["timestamp"], unit="s").normalize()
        closes = result["indicators"]["quote"][0]["close"]
        s      = pd.Series(closes, index=dates, name=symbol, dtype=float).dropna()
        return s.pct_change().dropna()
    except Exception:
        return None


# ==========================================
# ページ設定
# ==========================================
st.set_page_config(
    page_title="楽天証券ポートフォリオ",
    page_icon="📊",
    layout="wide",
)

# ==========================================
# セッション状態の初期化
# ==========================================
if "df_pnl" not in st.session_state:
    if PNL_CSV.exists():
        df_tmp = pd.read_csv(PNL_CSV, encoding="utf-8-sig")
        df_tmp["日付"] = pd.to_datetime(df_tmp["日付"])
        df_tmp["ファンド短縮名"] = df_tmp["ファンド名"].map(short)
        st.session_state.df_pnl = df_tmp
    else:
        st.session_state.df_pnl = None

# ==========================================
# サイドバー
# ==========================================
with st.sidebar:
    st.title("📊 楽天証券")
    st.subheader("ポートフォリオ管理")
    st.markdown("---")

    uploaded = st.file_uploader(
        "取引履歴Excelをアップロード",
        type=["xlsx"],
        help="楽天証券からダウンロードした取引履歴(INVST)ファイル",
    )

    update_btn = st.button("📡 NAV・損益を更新", type="primary", use_container_width=True)

    st.markdown("---")
    if st.session_state.df_pnl is not None:
        latest_date = st.session_state.df_pnl["日付"].max().date()
        st.caption(f"データ最終日: **{latest_date}**")
        st.caption(f"ファンド数: {st.session_state.df_pnl['ファンド名'].nunique()}")

# ==========================================
# メイン処理: Excelアップロード
# ==========================================
do_update = update_btn

if uploaded is not None:
    file_id = uploaded.file_id  # アップロードごとに一意なID
    if st.session_state.get("last_processed_file_id") != file_id:
        # 新しいファイルのみ処理
        with st.spinner("取引履歴を処理中..."):
            try:
                df_holdings = parse_excel(uploaded.read())
                df_holdings.to_csv(HOLD_CSV, encoding="utf-8-sig", index=False)
                st.sidebar.success(f"✅ {len(df_holdings):,}行を読み込みました")
                st.session_state.last_processed_file_id = file_id
                do_update = True
            except Exception as e:
                st.sidebar.error(f"読み込みエラー: {e}")
                df_holdings = None
    else:
        # 同じファイルは再処理しない（reruns でのループ防止）
        df_holdings = (
            pd.read_csv(HOLD_CSV, encoding="utf-8-sig")
            if HOLD_CSV.exists() else None
        )
else:
    df_holdings = (
        pd.read_csv(HOLD_CSV, encoding="utf-8-sig")
        if HOLD_CSV.exists() else None
    )

# ==========================================
# メイン処理: NAV・損益更新
# ==========================================
if do_update:
    if df_holdings is None:
        st.warning("先に取引履歴Excelをアップロードしてください。")
    else:
        # セッション内キャッシュ（再アップロード不要にするため session_state で保持）
        if "isin_cache" not in st.session_state:
            st.session_state.isin_cache = load_isin_cache()
        cache = st.session_state.isin_cache

        fund_names  = df_holdings["ファンド名"].unique().tolist()
        missing     = [f for f in fund_names if f not in cache
                       or not cache[f].get("isin")]

        # ── 未登録ファンドを自動検索 ──
        if missing:
            st.info(f"🔍 新しいファンドが {len(missing)} 本見つかりました。ISIN を自動検索します…")
            isin_prog = st.progress(0, text="ISIN検索準備中…")
            try:
                cache = search_missing_funds(fund_names, cache, isin_prog)
                st.session_state.isin_cache = cache
                isin_prog.empty()

                still_missing = [f for f in fund_names
                                 if not cache.get(f, {}).get("isin")]
                if still_missing:
                    st.warning(
                        "以下のファンドのISINが自動取得できませんでした。\n"
                        "isin_cache.json に手動で追加してください：\n"
                        + "\n".join(f"  - {f}" for f in still_missing)
                    )
            except Exception as e:
                isin_prog.empty()
                st.error(f"ISIN検索エラー: {e}")

        # ── NAV取得 & 損益計算 ──
        prog = st.progress(0, text="NAVデータ取得中…")
        try:
            df_nav = fetch_nav_all(df_holdings, cache, prog)
            df_nav.to_csv(NAV_CSV, encoding="utf-8-sig", index=False)

            df_pnl = calc_pnl(df_holdings, df_nav)
            df_pnl["ファンド短縮名"] = df_pnl["ファンド名"].map(short)
            df_pnl["日付"] = pd.to_datetime(df_pnl["日付"])
            df_pnl.to_csv(PNL_CSV, encoding="utf-8-sig", index=False)

            st.session_state.df_pnl = df_pnl
            prog.empty()
            st.success("✅ データ更新完了")
        except Exception as e:
            prog.empty()
            st.error(f"更新エラー: {e}")

# ==========================================
# データなし → アップロード案内
# ==========================================
if st.session_state.df_pnl is None:
    st.markdown("## 📊 楽天証券 ポートフォリオ管理")
    st.info(
        "**はじめ方**\n\n"
        "1. 楽天証券にログイン → 「取引履歴」→ 投資信託 → Excel ダウンロード\n"
        "2. 左サイドバーからファイルをアップロード\n"
        "3. 「NAV・損益を更新」ボタンをクリック"
    )
    st.stop()

# ==========================================
# 分析画面
# ==========================================
df = st.session_state.df_pnl.copy()
df["日付"] = pd.to_datetime(df["日付"])
latest_dt = df["日付"].max()

tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📊 サマリー",
    "📈 評価額推移",
    "📉 損益推移",
    "🔍 ファンド詳細",
    "📐 リスク・リターン",
])

# ──────────────────────────────
# Tab 1: サマリー
# ──────────────────────────────
with tab1:
    df_latest = df[df["日付"] == latest_dt].copy()

    total_value = df_latest["評価額"].sum()
    total_cost  = df_latest["累積元本"].sum()
    total_pnl   = total_value - total_cost
    total_pnl_r = total_pnl / total_cost * 100 if total_cost else 0

    # --- メトリクス ---
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("💴 評価額合計",  f"¥{int(total_value):,}")
    c2.metric("💰 元本合計",    f"¥{int(total_cost):,}")
    c3.metric(
        "📈 損益額",
        f"¥{int(total_pnl):+,}",
        delta=f"{total_pnl_r:+.2f}%",
        delta_color="normal",
    )
    c4.metric("📅 最終データ日", str(latest_dt.date()))

    st.markdown("---")

    # --- ドーナツグラフ + テーブル ---
    col_l, col_r = st.columns([1, 1])

    with col_l:
        df_donut = (
            df_latest
            .groupby("ファンド短縮名")["評価額"]
            .sum()
            .reset_index()
        )
        fig_donut = px.pie(
            df_donut, values="評価額", names="ファンド短縮名",
            hole=0.45,
            title=f"ポートフォリオ構成比  ({latest_dt.date()})",
            color="ファンド短縮名",
            color_discrete_map=FUND_COLORS,
        )
        fig_donut.update_traces(textposition="outside", textinfo="percent+label")
        fig_donut.update_layout(showlegend=False, height=380)
        st.plotly_chart(fig_donut, use_container_width=True)

    with col_r:
        # ファンド × 口座区分 サマリーテーブル
        df_tbl = (
            df_latest
            .groupby(["ファンド短縮名", "口座区分"])
            .agg(
                保有口数=("保有口数", "sum"),
                評価額=("評価額", "sum"),
                元本=("累積元本", "sum"),
                損益額=("損益額", "sum"),
            )
            .reset_index()
        )
        df_tbl["損益率(%)"] = (df_tbl["損益額"] / df_tbl["元本"] * 100).round(2)

        # 表示フォーマット
        df_disp = df_tbl.copy()
        df_disp["評価額"]    = df_disp["評価額"].apply(lambda x: f"¥{int(x):,}")
        df_disp["元本"]      = df_disp["元本"].apply(lambda x: f"¥{int(x):,}")
        df_disp["損益額"]    = df_disp["損益額"].apply(lambda x: f"¥{int(x):+,}")
        df_disp["損益率(%)"] = df_disp["損益率(%)"].apply(lambda x: f"{x:+.2f}%")
        df_disp["保有口数"]  = df_disp["保有口数"].apply(lambda x: f"{int(x):,}")

        st.markdown(f"**ファンド別サマリー（{latest_dt.date()}時点）**")
        st.dataframe(
            df_disp[["ファンド短縮名", "口座区分", "保有口数", "評価額", "元本", "損益額", "損益率(%)"]],
            use_container_width=True,
            hide_index=True,
        )

        # 合計行
        st.markdown(
            f"**合計: 評価額 ¥{int(total_value):,} ／ 元本 ¥{int(total_cost):,} "
            f"／ 損益 ¥{int(total_pnl):+,} ({total_pnl_r:+.2f}%)**"
        )

# ──────────────────────────────
# Tab 2: 評価額推移
# ──────────────────────────────
with tab2:
    period_opts = {"1ヶ月": 1, "3ヶ月": 3, "6ヶ月": 6, "1年": 12, "全期間": None}
    sel_period = st.radio("期間", list(period_opts.keys()), horizontal=True, index=4)

    months = period_opts[sel_period]
    start_dt = (
        latest_dt - pd.DateOffset(months=months)
        if months else df["日付"].min()
    )
    df_p = df[df["日付"] >= start_dt].copy()

    # 積み上げエリアグラフ用: 日次ファンド別評価額合計
    df_area = (
        df_p.groupby(["日付", "ファンド短縮名"])["評価額"]
        .sum()
        .reset_index()
    )
    # 元本合計（参照ライン用）
    df_total = (
        df_p.groupby("日付")
        .agg(評価額=("評価額", "sum"), 元本=("累積元本", "sum"))
        .reset_index()
    )

    fig2 = px.area(
        df_area, x="日付", y="評価額", color="ファンド短縮名",
        title="日次評価額の推移（ファンド別積み上げ）",
        color_discrete_map=FUND_COLORS,
        labels={"評価額": "評価額（円）"},
    )
    fig2.add_trace(go.Scatter(
        x=df_total["日付"], y=df_total["元本"],
        name="元本合計",
        line=dict(color="rgba(100,100,100,0.8)", dash="dash", width=2),
        mode="lines",
    ))
    fig2.update_layout(
        yaxis_tickprefix="¥", yaxis_tickformat=",",
        hovermode="x unified",
        height=480,
    )
    st.plotly_chart(fig2, use_container_width=True)

    # 最新の評価額と元本の差（利益額）を補足表示
    latest_total  = df_total[df_total["日付"] == latest_dt]
    if not latest_total.empty:
        lv = int(latest_total["評価額"].iloc[0])
        lc = int(latest_total["元本"].iloc[0])
        st.caption(
            f"最新日 {latest_dt.date()} ─ 評価額: **¥{lv:,}**  元本: ¥{lc:,}  "
            f"損益: **¥{lv - lc:+,}** ({(lv - lc) / lc * 100:+.2f}%)"
        )

# ──────────────────────────────
# Tab 3: 損益推移
# ──────────────────────────────
with tab3:
    c_mode, c_period3 = st.columns([1, 2])
    with c_mode:
        mode = st.radio("表示モード", ["損益額(¥)", "損益率(%)"], horizontal=False)
    with c_period3:
        sel_p3 = st.radio("期間 ", list(period_opts.keys()), horizontal=True, index=4, key="p3")

    months3 = period_opts[sel_p3]
    start3  = (
        latest_dt - pd.DateOffset(months=months3)
        if months3 else df["日付"].min()
    )

    all_shorts = sorted(df["ファンド短縮名"].unique().tolist())
    sel_funds = st.multiselect("ファンド選択", all_shorts, default=all_shorts)

    df_p3 = df[(df["日付"] >= start3) & (df["ファンド短縮名"].isin(sel_funds))].copy()

    # 口座区分を統合してファンド別集計
    df_grp3 = (
        df_p3.groupby(["日付", "ファンド短縮名"])
        .agg(評価額=("評価額", "sum"), 累積元本=("累積元本", "sum"))
        .reset_index()
    )
    df_grp3["損益額"]    = df_grp3["評価額"] - df_grp3["累積元本"]
    df_grp3["損益率(%)"] = (df_grp3["損益額"] / df_grp3["累積元本"] * 100).round(2)

    y_col  = "損益額" if "額" in mode else "損益率(%)"
    y_unit = "¥" if "額" in mode else "%"

    fig3 = px.line(
        df_grp3, x="日付", y=y_col, color="ファンド短縮名",
        title=f"{y_col}の推移",
        color_discrete_map=FUND_COLORS,
    )
    fig3.add_hline(y=0, line_dash="dot", line_color="gray", line_width=1)
    if "額" in mode:
        fig3.update_layout(yaxis_tickprefix="¥", yaxis_tickformat=",")
    else:
        fig3.update_layout(yaxis_ticksuffix="%")
    fig3.update_layout(hovermode="x unified", height=450)
    st.plotly_chart(fig3, use_container_width=True)

    # 全ファンド合計の損益推移
    st.markdown("**全ファンド合計**")
    df_total3 = (
        df_p3.groupby("日付")
        .agg(評価額=("評価額", "sum"), 累積元本=("累積元本", "sum"))
        .reset_index()
    )
    df_total3["損益額"]    = df_total3["評価額"] - df_total3["累積元本"]
    df_total3["損益率(%)"] = (df_total3["損益額"] / df_total3["累積元本"] * 100).round(2)

    fig3b = px.line(
        df_total3, x="日付", y=y_col,
        title=f"合計 {y_col}",
        color_discrete_sequence=["#8e44ad"],
    )
    fig3b.add_hline(y=0, line_dash="dot", line_color="gray", line_width=1)
    if "額" in mode:
        fig3b.update_layout(yaxis_tickprefix="¥", yaxis_tickformat=",")
    else:
        fig3b.update_layout(yaxis_ticksuffix="%")
    fig3b.update_layout(height=300)
    st.plotly_chart(fig3b, use_container_width=True)

# ──────────────────────────────
# Tab 4: ファンド詳細
# ──────────────────────────────
with tab4:
    fund_options = sorted(df["ファンド短縮名"].unique().tolist())
    sel_fund_short = st.selectbox("ファンド選択", fund_options)

    df_fund = df[df["ファンド短縮名"] == sel_fund_short].copy()

    if not df_fund.empty:
        # 口座区分を統合して1行/日にする（基準価額は口座区分によらず同一）
        df_fund_day = (
            df_fund.groupby("日付")
            .agg(
                基準価額=("基準価額", "first"),
                平均取得価額_万口=("平均取得価額_万口", "mean"),  # 口座平均
                保有口数=("保有口数", "sum"),
                評価額=("評価額", "sum"),
                累積元本=("累積元本", "sum"),
                損益額=("損益額", "sum"),
            )
            .reset_index()
        )
        df_fund_day["損益率(%)"] = (
            df_fund_day["損益額"] / df_fund_day["累積元本"] * 100
        ).round(2)

        # --- 最新ステータス ---
        latest_row = df_fund_day.sort_values("日付").iloc[-1]
        mc1, mc2, mc3, mc4 = st.columns(4)
        mc1.metric("基準価額",    f"¥{int(latest_row['基準価額']):,} /万口")
        mc2.metric("平均取得価額", f"¥{int(latest_row['平均取得価額_万口']):,} /万口")
        mc3.metric(
            "含み損益",
            f"¥{int(latest_row['損益額']):+,}",
            delta=f"{latest_row['損益率(%)']:+.2f}%",
        )
        mc4.metric("評価額", f"¥{int(latest_row['評価額']):,}")

        st.markdown("---")

        # --- 基準価額チャート ---
        fig4 = go.Figure()
        color = FUND_COLORS.get(sel_fund_short, "#3498db")
        rgba_fill = color.replace("#", "")
        r, g, b = int(rgba_fill[0:2], 16), int(rgba_fill[2:4], 16), int(rgba_fill[4:6], 16)

        fig4.add_trace(go.Scatter(
            x=df_fund_day["日付"],
            y=df_fund_day["基準価額"],
            name="基準価額",
            line=dict(color=color, width=2),
            fill="tozeroy",
            fillcolor=f"rgba({r},{g},{b},0.1)",
        ))
        fig4.add_trace(go.Scatter(
            x=df_fund_day["日付"],
            y=df_fund_day["平均取得価額_万口"],
            name="平均取得価額",
            line=dict(color="red", dash="dash", width=1.5),
        ))
        fig4.update_layout(
            title=f"{sel_fund_short} ─ 基準価額の推移",
            yaxis_title="円 / 万口",
            yaxis_tickprefix="¥",
            yaxis_tickformat=",",
            hovermode="x unified",
            height=400,
        )
        st.plotly_chart(fig4, use_container_width=True)

        # --- 損益率チャート ---
        fig4b = go.Figure()
        fig4b.add_trace(go.Scatter(
            x=df_fund_day["日付"],
            y=df_fund_day["損益率(%)"],
            name="損益率",
            line=dict(color=color, width=2),
            fill="tozeroy",
            fillcolor=f"rgba({r},{g},{b},0.15)",
        ))
        fig4b.add_hline(y=0, line_dash="dot", line_color="gray")
        fig4b.update_layout(
            title=f"{sel_fund_short} ─ 損益率の推移",
            yaxis_title="損益率 (%)",
            yaxis_ticksuffix="%",
            hovermode="x unified",
            height=300,
        )
        st.plotly_chart(fig4b, use_container_width=True)

        # --- 口座区分別の詳細 ---
        st.markdown("**口座区分別の詳細**")
        df_acct_latest = df_fund[df_fund["日付"] == latest_dt]
        if not df_acct_latest.empty:
            df_acct_disp = df_acct_latest[
                ["口座区分", "保有口数", "累積元本", "評価額", "損益額", "損益率(%)"]
            ].copy()
            df_acct_disp["保有口数"] = df_acct_disp["保有口数"].apply(lambda x: f"{int(x):,}")
            df_acct_disp["累積元本"] = df_acct_disp["累積元本"].apply(lambda x: f"¥{int(x):,}")
            df_acct_disp["評価額"]   = df_acct_disp["評価額"].apply(lambda x: f"¥{int(x):,}")
            df_acct_disp["損益額"]   = df_acct_disp["損益額"].apply(lambda x: f"¥{int(x):+,}")
            df_acct_disp["損益率(%)"] = df_acct_disp["損益率(%)"].apply(lambda x: f"{x:+.2f}%")
            st.dataframe(df_acct_disp, use_container_width=True, hide_index=True)

# ──────────────────────────────
# Tab 5: リスク・リターン分析
# ──────────────────────────────
with tab5:
    # 分析期間
    period5_opts = {"1年": 12, "2年": 24, "3年": 36, "全期間": None}
    sel_p5  = st.radio("分析期間", list(period5_opts.keys()), horizontal=True, index=3, key="p5")
    months5 = period5_opts[sel_p5]
    start5  = (latest_dt - pd.DateOffset(months=months5)) if months5 else df["日付"].min()
    df5     = df[df["日付"] >= start5].copy()

    returns = calc_daily_returns(df5)
    returns.columns = [short(c) for c in returns.columns]

    if returns.empty or len(returns) < 20:
        st.warning("リターン計算に必要なデータが不足しています（最低20営業日）。")
    else:
        # ─── ① パフォーマンス指標テーブル ───
        st.markdown("#### 📊 パフォーマンス指標")
        metrics_df = calc_risk_metrics(returns)
        st.dataframe(metrics_df.set_index("ファンド"), use_container_width=True)

        with st.expander("指標の説明"):
            st.markdown("""
| 指標 | 説明 |
|------|------|
| 年率リターン | 保有期間のリターンを1年に換算 |
| 年率ボラ | 日次リターンの標準偏差 × √252（リスクの大きさ） |
| シャープ | 超過リターン / ボラティリティ。1以上が目安 |
| 最大DD | 最高値からの最大下落率（最大ドローダウン） |
| VaR95 | 95%信頼区間の最悪日次損失（ヒストリカル法） |
| CVaR95 | VaR超過時の平均損失（テールリスク・ES） |
| 勝率 | 日次リターンがプラスの割合 |
""")

        st.markdown("---")

        # ─── ② リスク・リターン散布図 / 相関マトリクス ───
        col_sc, col_cr = st.columns(2)

        with col_sc:
            st.markdown("#### 🎯 リスク・リターン散布図")
            latest5     = df5[df5["日付"] == df5["日付"].max()]
            eval_by_f   = latest5.groupby("ファンド短縮名")["評価額"].sum().reset_index()
            scatter_df  = metrics_df.merge(
                eval_by_f, left_on="ファンド", right_on="ファンド短縮名", how="left"
            )
            fig_sc = px.scatter(
                scatter_df,
                x="年率ボラ(%)", y="年率リターン(%)",
                size="評価額", color="ファンド", text="ファンド",
                title="効率的フロンティア図（バブル = 評価額）",
                color_discrete_map=FUND_COLORS,
            )
            fig_sc.add_hline(y=0, line_dash="dot", line_color="gray", opacity=0.5)
            fig_sc.update_traces(textposition="top center")
            fig_sc.update_layout(
                xaxis_ticksuffix="%", yaxis_ticksuffix="%",
                showlegend=False, height=380,
            )
            st.plotly_chart(fig_sc, use_container_width=True)

        with col_cr:
            st.markdown("#### 🔗 相関マトリクス")
            corr = returns.corr()
            fig_corr = px.imshow(
                corr, text_auto=".2f",
                color_continuous_scale="RdBu_r", zmin=-1, zmax=1,
                title="日次リターンの相関係数",
            )
            fig_corr.update_layout(height=380)
            st.plotly_chart(fig_corr, use_container_width=True)

        st.caption("相関が低い（青い）ほど分散効果が高く、同一方向に動きにくい")
        st.markdown("---")

        # ─── ③ ドローダウン推移 ───
        st.markdown("#### 📉 ドローダウン推移")
        cum_ret = (1 + returns).cumprod()
        dd_pct  = ((cum_ret / cum_ret.cummax()) - 1) * 100
        dd_long = dd_pct.reset_index().melt(
            id_vars="日付", var_name="ファンド", value_name="DD(%)"
        )
        fig_dd = px.line(
            dd_long, x="日付", y="DD(%)",
            color="ファンド", title="ドローダウン推移",
            color_discrete_map=FUND_COLORS,
        )
        fig_dd.add_hline(y=0, line_dash="dot", line_color="gray", opacity=0.5)
        fig_dd.update_layout(yaxis_ticksuffix="%", hovermode="x unified", height=340)
        st.plotly_chart(fig_dd, use_container_width=True)
        st.markdown("---")

        # ─── ④ 日次リターン分布 & VaR ───
        st.markdown("#### 📊 日次リターン分布 & VaR")
        sel_vf = st.selectbox("ファンド選択", returns.columns.tolist(), key="var_fund")
        r_sel  = returns[sel_vf].dropna()
        var95  = float(r_sel.quantile(0.05))
        var99  = float(r_sel.quantile(0.01))

        fig_hist = go.Figure()
        fig_hist.add_trace(go.Histogram(
            x=(r_sel * 100).values, nbinsx=60,
            name="日次リターン", marker_color="#3498db", opacity=0.75,
        ))
        for v, c, lbl in [(var95, "orange", "VaR95"), (var99, "red", "VaR99")]:
            fig_hist.add_vline(
                x=v * 100, line_dash="dash", line_color=c,
                annotation_text=f"{lbl}: {v*100:.2f}%",
            )
        fig_hist.update_layout(
            title=f"{sel_vf} ─ 日次リターン分布",
            xaxis_title="日次リターン (%)", yaxis_title="日数",
            xaxis_ticksuffix="%", bargap=0.05, height=320,
        )
        st.plotly_chart(fig_hist, use_container_width=True)
        st.caption(
            f"VaR95: 95%の確率で1日の損失は **{abs(var95)*100:.2f}%** 以内  "
            f"｜  VaR99: 99%の確率で **{abs(var99)*100:.2f}%** 以内"
        )
        st.markdown("---")

        # ─── ⑤ β / α 分析 ───
        st.markdown("#### 🔢 β / α 分析")
        col_bi, col_be = st.columns(2)

        with col_bi:
            sel_bench = st.selectbox(
                "ベンチマーク（ポートフォリオ内）",
                list(returns.columns), index=0, key="bench_sel",
            )
        with col_be:
            ext_bench_map = {
                "なし":                     None,
                "日経225 (^N225)":          "^N225",
                "S&P500 (^GSPC)":           "^GSPC",
                "全世界株 MSCI ACWI":       "ACWI",
                "金 (GLD)":                 "GLD",
            }
            sel_ext = st.selectbox(
                "外部ベンチマーク（Yahoo Finance）",
                list(ext_bench_map.keys()), index=0, key="ext_bench",
            )

        # ポートフォリオ内ベンチマーク
        ba_df = calc_beta_alpha(returns, sel_bench)
        if not ba_df.empty:
            st.markdown(f"**vs {sel_bench}（ポートフォリオ内）**")
            st.dataframe(ba_df.set_index("ファンド"), use_container_width=True)

        # 外部ベンチマーク（Yahoo Finance）
        if sel_ext != "なし":
            symbol = ext_bench_map[sel_ext]
            with st.spinner(f"{sel_ext} のデータを Yahoo Finance から取得中…"):
                bench_ret = fetch_benchmark_yahoo(
                    symbol, str(returns.index.min().date())
                )
            if bench_ret is not None:
                bench_ret.index = pd.to_datetime(bench_ret.index).normalize()
                common_idx = returns.index.intersection(bench_ret.index)
                if len(common_idx) >= 20:
                    ret_ext          = returns.loc[common_idx].copy()
                    ret_ext[sel_ext] = bench_ret.loc[common_idx]
                    ba_ext           = calc_beta_alpha(ret_ext, sel_ext)
                    if not ba_ext.empty:
                        st.markdown(f"**vs {sel_ext}（外部ベンチマーク）**")
                        st.dataframe(ba_ext.set_index("ファンド"), use_container_width=True)
                else:
                    st.warning(f"共通期間のデータが不足しています（{len(common_idx)}日）。")
            else:
                st.warning(f"{sel_ext} のデータ取得に失敗しました（Yahoo Finance API制限の可能性）。")

        with st.expander("β / α の解釈"):
            st.markdown("""
| 指標 | 説明 |
|------|------|
| **β** | 市場感応度。β=1.0 はベンチマークと同じ動き、β>1 は増幅 |
| **α年率** | 市場リターンでは説明できない超過リターン（正なら市場を上回る） |
| **R²** | ベンチマークによる説明力（0〜1）。高いほど連動性が高い |
| **TE年率** | トラッキングエラー（年率）。ベンチマークからの乖離の大きさ |
""")
        st.markdown("---")

        # ─── ⑥ ローリングシャープレシオ ───
        st.markdown("#### 📈 ローリングシャープレシオ")
        rf_d   = 0.001 / 252
        window = min(252, max(60, len(returns) // 3))
        roll_s = (
            (returns - rf_d).rolling(window).mean()
            / returns.rolling(window).std()
            * np.sqrt(252)
        ).dropna(how="all")

        if len(roll_s) >= 1:
            rs_long = roll_s.reset_index().melt(
                id_vars="日付", var_name="ファンド", value_name="シャープレシオ"
            )
            fig_rs = px.line(
                rs_long, x="日付", y="シャープレシオ",
                color="ファンド",
                title=f"ローリングシャープレシオ（{window}日窓）",
                color_discrete_map=FUND_COLORS,
            )
            fig_rs.add_hline(y=0,  line_dash="dot", line_color="gray",  opacity=0.5)
            fig_rs.add_hline(y=1,  line_dash="dot", line_color="green", opacity=0.6,
                             annotation_text="1.0", annotation_position="right")
            fig_rs.add_hline(y=-1, line_dash="dot", line_color="red",   opacity=0.4,
                             annotation_text="-1.0", annotation_position="right")
            fig_rs.update_layout(hovermode="x unified", height=380)
            st.plotly_chart(fig_rs, use_container_width=True)
            st.caption("シャープレシオ > 1 が高パフォーマンスの目安。時系列で見ることで市場局面の変化を把握できる。")
