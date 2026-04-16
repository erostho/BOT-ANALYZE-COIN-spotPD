import os
import json
import time
import traceback
from datetime import datetime, timezone, timedelta

import requests
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials


# =========================================================
# CONFIG
# =========================================================
SHEET_NAME = os.getenv("SHEET_NAME", "OKX_SPOT_HUNTER")
GOOGLE_CREDS_FILE = os.getenv("GOOGLE_CREDS_FILE", "credentials.json")

OKX_TIMEFRAME = os.getenv("OKX_TIMEFRAME", "1H")
OKX_CANDLE_LIMIT = int(os.getenv("OKX_CANDLE_LIMIT", "100"))
PRICE_MAX = float(os.getenv("PRICE_MAX", "1"))
ONLY_USDT_QUOTE = os.getenv("ONLY_USDT_QUOTE", "true").lower() == "true"

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

COINGECKO_API_KEY = os.getenv("COINGECKO_API_KEY", "")
MORALIS_API_KEY = os.getenv("MORALIS_API_KEY", "")

REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "20"))
SLEEP_BETWEEN_SYMBOLS = float(os.getenv("SLEEP_BETWEEN_SYMBOLS", "0.06"))
TRACKING_MAX_DAYS = int(os.getenv("TRACKING_MAX_DAYS", "90"))
SENT_FILE = os.getenv("SENT_FILE", "sent.json")

# chỉ enrich holder cho nhóm mạnh
RESOLVE_TOP_N = int(os.getenv("RESOLVE_TOP_N", "80"))
RESOLVE_STATUS_SET = set(
    s.strip().upper()
    for s in os.getenv("RESOLVE_STATUS_SET", "EARLY_FLOW,PRE_BREAK,BREAKOUT,PUMPING").split(",")
    if s.strip()
)

ALERT_MIN_SCORE_PREBREAK = float(os.getenv("ALERT_MIN_SCORE_PREBREAK", "7.0"))
ALERT_MIN_SCORE_EARLY = float(os.getenv("ALERT_MIN_SCORE_EARLY", "6.5"))
ALERT_MIN_SCORE_BREAKOUT = float(os.getenv("ALERT_MIN_SCORE_BREAKOUT", "7.5"))


# =========================================================
# HTTP
# =========================================================
session = requests.Session()
session.headers.update({"User-Agent": "okx-spot-hunter-v4/1.0"})


def utc_now():
    return datetime.now(timezone.utc)


def utc_now_str():
    return utc_now().strftime("%Y-%m-%d %H:%M:%S")


def safe_float(x, default=0.0):
    try:
        if x is None or x == "":
            return default
        return float(x)
    except Exception:
        return default


def clamp(v, lo, hi):
    return max(lo, min(hi, v))
def safe_holder_pct(pct):
    try:
        pct = float(pct)
    except:
        return None

    # dữ liệu lỗi
    if pct < 0:
        return None

    # clamp về max 100%
    if pct > 100:
        return 100.0

    return pct
def clean_symbol_base(inst_id: str) -> str:
    # RAVE-USDT -> RAVE
    return inst_id.split("-")[0].strip().lower()


def json_load_file(path, default):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def json_save_file(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


# =========================================================
# GOOGLE SHEETS
# =========================================================


def init_sheet():
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]

    raw = os.getenv("GOOGLE_CREDS_FILE", "")

    # fix JSON vs file path
    if raw.strip().startswith("{"):
        creds = ServiceAccountCredentials.from_json_keyfile_dict(json.loads(raw), scope)
    else:
        creds = ServiceAccountCredentials.from_json_keyfile_name(raw, scope)

    client = gspread.authorize(creds)

    # 🔥 QUAN TRỌNG: CHỈ OPEN, KHÔNG CREATE
    sheet_name = os.getenv("SHEET_NAME", "OKX_SPOT_HUNTER")
    sh = client.open(sheet_name)

    return sh

def get_or_create_ws(sh, title, rows=3000, cols=40):
    try:
        return sh.worksheet(title)
    except gspread.WorksheetNotFound:
        return sh.add_worksheet(title=title, rows=str(rows), cols=str(cols))


def worksheet_to_df(ws):
    values = ws.get_all_values()
    if not values:
        return pd.DataFrame()
    header = values[0]
    rows = values[1:]
    if not rows:
        return pd.DataFrame(columns=header)
    return pd.DataFrame(rows, columns=header)


def overwrite_worksheet(ws, df: pd.DataFrame):
    ws.clear()
    if df.empty:
        ws.update([["EMPTY"]])
        return
    values = [df.columns.tolist()] + df.fillna("").astype(str).values.tolist()
    ws.update(values)


def append_rows(ws, rows):
    if rows:
        ws.append_rows(rows, value_input_option="USER_ENTERED")


# =========================================================
# TELEGRAM
# =========================================================
def send_telegram(text: str):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }
    try:
        session.post(url, json=payload, timeout=REQUEST_TIMEOUT)
    except Exception:
        pass


def load_sent():
    return json_load_file(SENT_FILE, {})


def save_sent(sent):
    json_save_file(SENT_FILE, sent)


def cleanup_sent(sent, keep_days=7):
    cutoff = utc_now() - timedelta(days=keep_days)
    out = {}
    for k, v in sent.items():
        try:
            ts = datetime.fromisoformat(v["time"])
            if ts >= cutoff:
                out[k] = v
        except Exception:
            out[k] = v
    return out


# =========================================================
# OKX
# =========================================================
def fetch_okx_spot_tickers():
    url = "https://www.okx.com/api/v5/market/tickers"
    params = {"instType": "SPOT"}
    r = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.json().get("data", [])


def fetch_okx_candles(inst_id: str):
    url = "https://www.okx.com/api/v5/market/candles"
    params = {
        "instId": inst_id,
        "bar": OKX_TIMEFRAME,
        "limit": str(OKX_CANDLE_LIMIT)
    }
    r = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    data = r.json().get("data", [])
    if not data:
        return pd.DataFrame()

    df = pd.DataFrame(data, columns=[
        "ts", "open", "high", "low", "close",
        "vol", "volCcy", "volCcyQuote", "confirm"
    ])
    for c in ["ts", "open", "high", "low", "close", "vol", "volCcy", "volCcyQuote"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.sort_values("ts").reset_index(drop=True)
    return df


# =========================================================
# FEATURE ENGINEERING
# =========================================================
def calc_features(df: pd.DataFrame):
    if df.empty or len(df) < 60:
        return None

    close = df["close"]
    high = df["high"]
    low = df["low"]
    vol = df["vol"]

    price = safe_float(close.iloc[-1], 0.0)
    if price <= 0:
        return None

    vol_recent = safe_float(vol.tail(10).mean(), 0.0)
    vol_old = safe_float(vol.iloc[-60:-10].mean(), 0.0)
    vol_ratio = vol_recent / (vol_old + 1e-9)

    range_now = safe_float(high.tail(20).max() - low.tail(20).min(), 0.0)
    range_old = safe_float(high.iloc[-60:-20].max() - low.iloc[-60:-20].min(), 0.0)
    compression = range_now / (range_old + 1e-9) if range_old > 0 else 1.0

    low_recent = safe_float(low.tail(20).min(), 0.0)
    low_old = safe_float(low.iloc[-60:-20].min(), 0.0)
    higher_low = low_recent > low_old

    high_60 = safe_float(high.tail(60).max(), 0.0)
    low_60 = safe_float(low.tail(60).min(), 0.0)
    break_pressure = price / (high_60 + 1e-9)

    close_10 = safe_float(close.iloc[-10], price)
    close_20 = safe_float(close.iloc[-20], price)
    roc_10 = (price / (close_10 + 1e-9)) - 1.0
    roc_20 = (price / (close_20 + 1e-9)) - 1.0
    pos_in_range = (price - low_60) / ((high_60 - low_60) + 1e-9)

    returns = close.pct_change().dropna()
    vol_20 = safe_float(returns.tail(20).std(), 0.0)

    return {
        "price": round(price, 8),
        "vol_ratio": round(vol_ratio, 6),
        "compression": round(compression, 6),
        "higher_low": bool(higher_low),
        "break_pressure": round(break_pressure, 6),
        "roc_10": round(roc_10, 6),
        "roc_20": round(roc_20, 6),
        "pos_in_range": round(pos_in_range, 6),
        "volatility_20": round(vol_20, 6),
        "range_high_60": round(high_60, 8),
        "range_low_60": round(low_60, 8),
    }


def classify_status(f):
    if f is None:
        return "DEAD"

    vr = f["vol_ratio"]
    cp = f["compression"]
    hl = f["higher_low"]
    bp = f["break_pressure"]
    roc10 = f["roc_10"]

    # Chết / ngủ
    if vr < 0.75 and bp < 0.85:
        return "DEAD"

    if vr < 0.95 and cp >= 0.90:
        return "SLEEPING"

    # Bắt đầu có dòng tiền
    if vr >= 1.10 and hl and bp >= 0.80 and cp > 0.80:
        return "EARLY_FLOW"

    # Giai đoạn ngon nhất: nén + áp sát đỉnh + có vol
    if (
        vr >= 1.35 and
        hl and
        cp <= 0.80 and
        bp >= 0.92 and
        roc10 <= 0.12
    ):
        return "PRE_BREAK"

    # Mới break sớm
    if bp >= 0.985 and roc10 > 0.03:
        return "BREAKOUT"

    # Chạy quá mạnh rồi
    if roc10 > 0.12 and bp >= 0.99:
        return "PUMPING"

    # Dump
    if roc10 < -0.10:
        return "DUMPING"

    return "SLEEPING"

def calc_score(f, status, holder_score=0.0):
    if f is None:
        return 0.0

    vol_component = clamp(f["vol_ratio"], 0.0, 3.0) * 2.0
    compression_component = (1.0 - clamp(f["compression"], 0.0, 1.2)) * 2.0
    structure_component = 1.0 if f["higher_low"] else 0.0
    break_component = clamp(f["break_pressure"], 0.0, 1.1) * 2.0
    momentum_component = clamp(max(f["roc_10"], 0.0), 0.0, 0.20) * 10.0

    status_bonus = {
        "WAKING_UP": 0.5,
        "EARLY_FLOW": 2.0,
        "PRE_BREAK": 3.0,
        "BREAKOUT": 2.0,
        "PUMPING": 1.0,
        "DUMPING": -1.0,
        "DEAD": -1.0,
    }.get(status, 0.0)

    total = (
        vol_component +
        compression_component +
        structure_component +
        break_component +
        momentum_component +
        status_bonus +
        holder_score
    )
    return round(max(total, 0.0), 2)


def calc_priority(status, score):
    if status in ["PRE_BREAK", "EARLY_FLOW"] and score >= 8:
        return "EXTREME"
    if status in ["PRE_BREAK", "EARLY_FLOW", "BREAKOUT"] and score >= 6.5:
        return "HOT"
    if status in ["WAKING_UP", "EARLY_FLOW", "BREAKOUT"] and score >= 5:
        return "GOOD"
    if status in ["DEAD", "DUMPING", "LATE"]:
        return "AVOID"
    return "WATCH"


# =========================================================
# COINGECKO AUTO-RESOLVE
# =========================================================
def cg_headers():
    headers = {}
    if COINGECKO_API_KEY:
        headers["x-cg-pro-api-key"] = COINGECKO_API_KEY
    return headers


def cg_get(path, params=None):
    base = "https://pro-api.coingecko.com/api/v3" if COINGECKO_API_KEY else "https://api.coingecko.com/api/v3"
    url = f"{base}{path}"
    r = session.get(url, headers=cg_headers(), params=params or {}, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.json()


def search_coingecko_coin_candidates(symbol_base: str):
    """
    Tìm candidate coin-level trước bằng /search
    """
    try:
        js = cg_get("/search", params={"query": symbol_base})
        coins = js.get("coins", []) or []
    except Exception:
        return []

    out = []
    sym_l = symbol_base.lower().strip()
    for c in coins[:15]:
        cg_symbol = str(c.get("symbol", "")).lower()
        cg_name = str(c.get("name", "")).lower()
        score = 0.0
        if cg_symbol == sym_l:
            score += 70
        elif sym_l in cg_symbol:
            score += 35

        if sym_l == cg_name:
            score += 30
        elif sym_l in cg_name:
            score += 10

        if c.get("market_cap_rank") is not None:
            score += 5

        out.append({
            "source": "cg_search",
            "id": c.get("id"),
            "name": c.get("name"),
            "symbol": c.get("symbol"),
            "market_cap_rank": c.get("market_cap_rank"),
            "score": round(score, 2),
        })

    out.sort(key=lambda x: x["score"], reverse=True)
    return out


def search_coingecko_onchain_candidates(symbol_base: str):
    """
    Tìm candidate token-level bằng onchain search pools
    """
    try:
        js = cg_get("/onchain/search/pools", params={"query": symbol_base})
    except Exception:
        return []

    data = js.get("data", []) or []
    included = js.get("included", []) or []

    token_map = {}
    for inc in included:
        inc_type = inc.get("type")
        attrs = inc.get("attributes", {}) or {}
        if inc_type == "token":
            token_map[inc.get("id")] = {
                "token_name": attrs.get("name"),
                "token_symbol": attrs.get("symbol"),
                "address": attrs.get("address"),
            }

    sym_l = symbol_base.lower().strip()
    out = []

    for item in data[:30]:
        attrs = item.get("attributes", {}) or {}
        rel = item.get("relationships", {}) or {}
        net = item.get("relationships", {}).get("network", {}).get("data", {})
        network_id = net.get("id")

        token_links = rel.get("base_token", {}).get("data") or {}
        token_id = token_links.get("id")
        token_info = token_map.get(token_id, {})

        token_symbol = str(token_info.get("token_symbol", "")).lower()
        token_name = str(token_info.get("token_name", "")).lower()
        address = token_info.get("address")

        score = 0.0
        if token_symbol == sym_l:
            score += 80
        elif sym_l in token_symbol:
            score += 40

        if sym_l == token_name:
            score += 25
        elif sym_l in token_name:
            score += 10

        reserve_usd = safe_float(attrs.get("reserve_in_usd"), 0.0)
        volume_usd = safe_float(attrs.get("volume_usd", {}).get("h24"), 0.0)
        tx_count = safe_float(attrs.get("transactions", {}).get("h24", {}).get("buys"), 0.0) + safe_float(
            attrs.get("transactions", {}).get("h24", {}).get("sells"), 0.0
        )

        if reserve_usd > 10000:
            score += 10
        if reserve_usd > 100000:
            score += 10
        if volume_usd > 10000:
            score += 5
        if tx_count > 20:
            score += 5

        out.append({
            "source": "cg_onchain_search",
            "network": network_id,
            "token_address": address,
            "token_symbol": token_info.get("token_symbol"),
            "token_name": token_info.get("token_name"),
            "pool_address": attrs.get("address"),
            "reserve_usd": reserve_usd,
            "volume_usd_h24": volume_usd,
            "score": round(score, 2),
        })

    out.sort(key=lambda x: x["score"], reverse=True)
    return out


def network_to_moralis_chain(network_id: str):
    """
    map network ID bên CoinGecko onchain sang Moralis chain slug
    """
    if not network_id:
        return None, None

    n = network_id.lower().strip()
    mapping = {
        "eth": ("evm", "eth"),
        "ethereum": ("evm", "eth"),
        "bsc": ("evm", "bsc"),
        "binance-smart-chain": ("evm", "bsc"),
        "polygon_pos": ("evm", "polygon"),
        "polygon": ("evm", "polygon"),
        "arbitrum": ("evm", "arbitrum"),
        "optimism": ("evm", "optimism"),
        "base": ("evm", "base"),
        "avalanche": ("evm", "avalanche"),
        "solana": ("solana", "mainnet"),
    }
    return mapping.get(n, (None, None))


def resolve_with_dexscreener(symbol_base):
    url = f"https://api.dexscreener.com/latest/dex/search?q={symbol_base}"

    try:
        res = requests.get(url).json()
        pairs = res.get("pairs", [])
    except:
        return None

    if not pairs:
        return None

    candidates = []

    for p in pairs[:20]:
        liquidity = float(p.get("liquidity", {}).get("usd", 0) or 0)
        volume = float(p.get("volume", {}).get("h24", 0) or 0)

        score = 0

        if liquidity > 10000:
            score += 20
        if liquidity > 100000:
            score += 20

        if volume > 10000:
            score += 10

        if p.get("txns", {}).get("h24", {}).get("buys", 0) > 20:
            score += 10

        candidates.append((score, p))

    candidates.sort(reverse=True, key=lambda x: x[0])

    best = candidates[0][1]

    return {
        "chain": best.get("chainId"),
        "token_address": best.get("baseToken", {}).get("address"),
        "symbol": best.get("baseToken", {}).get("symbol"),
        "liquidity": best.get("liquidity", {}).get("usd")
    }
def map_chain(chain):
    mapping = {
        "bsc": ("evm", "bsc"),
        "ethereum": ("evm", "eth"),
        "polygon": ("evm", "polygon"),
        "arbitrum": ("evm", "arbitrum"),
        "base": ("evm", "base"),
        "solana": ("solana", "mainnet")
    }
    return mapping.get(chain, (None, None))
# =========================================================
# MORALIS HOLDER
# =========================================================
def moralis_headers():
    return {"X-API-Key": MORALIS_API_KEY} if MORALIS_API_KEY else {}


def fetch_top10_holder_pct_evm(token_address: str, chain: str):
    if not MORALIS_API_KEY:
        return None
    url = f"https://deep-index.moralis.io/api/v2.2/erc20/{token_address}/owners"
    params = {"chain": chain, "order": "DESC", "limit": 10}
    r = session.get(url, headers=moralis_headers(), params=params, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    js = r.json()
    result = js.get("result", []) or []

    total_pct = 0.0
    seen = set()
    
    for owner in result[:20]:  # lấy rộng hơn để tránh duplicate
        addr = owner.get("owner_address")
    
        # tránh cộng trùng ví
        if addr in seen:
            continue
        seen.add(addr)
    
        pct = safe_holder_pct(owner.get("percentage_relative_to_total_supply"))
    
        if pct is None:
            continue
    
        total_pct += pct
    
        # chỉ lấy top 10 holder hợp lệ
        if len(seen) >= 10:
            break
    
    # clamp lần cuối
    if total_pct > 100:
        total_pct = 100.0
    
    return round(total_pct, 4)
    return round(total_pct, 4)


def fetch_top10_holder_pct_solana(token_address: str):
    if not MORALIS_API_KEY:
        return None
    url = f"https://solana-gateway.moralis.io/token/mainnet/holders/{token_address}"
    r = session.get(url, headers=moralis_headers(), timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    js = r.json()
    top10 = js.get("holderSupply", {}).get("top10", {}).get("supplyPercent")
    if top10 is None:
        return None
    return round(safe_float(top10), 4)


def get_holder_info_from_resolve(resolved_info: dict):
    if not resolved_info.get("resolved"):
        return {
            "top10_holder_pct": None,
            "holder_flag": "UNRESOLVED",
            "holder_score": 0.0,
            "holder_note": resolved_info.get("reason", "unresolved"),
        }

    if not MORALIS_API_KEY:
        return {
            "top10_holder_pct": None,
            "holder_flag": "NO_HOLDER_API",
            "holder_score": 0.0,
            "holder_note": "missing moralis api key",
        }

    chain_type = resolved_info.get("chain_type")
    network = resolved_info.get("network")
    token_address = resolved_info.get("token_address")

    try:
        if chain_type == "evm":
            top10_pct = fetch_top10_holder_pct_evm(token_address, network)
        elif chain_type == "solana":
            top10_pct = fetch_top10_holder_pct_solana(token_address)
        else:
            top10_pct = None
    except Exception as e:
        return {
            "top10_holder_pct": None,
            "holder_flag": "ERR",
            "holder_score": 0.0,
            "holder_note": str(e)[:120],
        }

    if top10_pct is None:
        return {
            "top10_holder_pct": None,
            "holder_flag": "NA",
            "holder_score": 0.0,
            "holder_note": "holder unavailable",
        }

    if top10_pct >= 85:
        flag = "VERY_HIGH"
        score = 3.0
    elif top10_pct >= 80:
        flag = "HIGH_80"
        score = 2.5
    elif top10_pct >= 70:
        flag = "HIGH"
        score = 2.0
    elif top10_pct >= 50:
        flag = "MID"
        score = 1.0
    else:
        flag = "LOW"
        score = 0.0

    return {
        "top10_holder_pct": round(top10_pct, 4),
        "holder_flag": flag,
        "holder_score": score,
        "holder_note": "",
    }
def resolve_with_dexscreener(symbol_base):
    url = f"https://api.dexscreener.com/latest/dex/search?q={symbol_base}"

    try:
        res = requests.get(url, timeout=20).json()
        pairs = res.get("pairs", [])
    except Exception:
        return None

    if not pairs:
        return None

    candidates = []

    for p in pairs[:25]:
        base_token = p.get("baseToken", {}) or {}
        chain_id = str(p.get("chainId", "")).lower().strip()
        liquidity = float(p.get("liquidity", {}).get("usd", 0) or 0)
        volume = float(p.get("volume", {}).get("h24", 0) or 0)

        buys = int(p.get("txns", {}).get("h24", {}).get("buys", 0) or 0)
        sells = int(p.get("txns", {}).get("h24", {}).get("sells", 0) or 0)

        token_symbol = str(base_token.get("symbol", "")).lower().strip()
        token_name = str(base_token.get("name", "")).lower().strip()
        symbol_q = symbol_base.lower().strip()

        score = 0.0

        # khớp symbol
        if token_symbol == symbol_q:
            score += 60
        elif symbol_q in token_symbol:
            score += 25

        # khớp name
        if token_name == symbol_q:
            score += 20
        elif symbol_q in token_name:
            score += 8

        # liquidity / volume / tx
        if liquidity > 10000:
            score += 10
        if liquidity > 100000:
            score += 10
        if volume > 10000:
            score += 5
        if volume > 100000:
            score += 5
        if buys + sells > 20:
            score += 5

        candidates.append({
            "score": round(score, 2),
            "chain_id": chain_id,
            "token_address": base_token.get("address"),
            "resolved_symbol": base_token.get("symbol"),
            "resolved_name": base_token.get("name"),
            "liquidity_usd": liquidity,
            "volume_h24": volume,
            "pair_url": p.get("url"),
        })

    if not candidates:
        return None

    candidates = sorted(candidates, key=lambda x: x["score"], reverse=True)
    return candidates[0]


def map_chain_to_moralis(chain_id):
    mapping = {
        "bsc": ("evm", "bsc"),
        "ethereum": ("evm", "eth"),
        "eth": ("evm", "eth"),
        "polygon": ("evm", "polygon"),
        "polygonzk": ("evm", "polygon"),
        "arbitrum": ("evm", "arbitrum"),
        "optimism": ("evm", "optimism"),
        "base": ("evm", "base"),
        "avalanche": ("evm", "avalanche"),
        "solana": ("solana", "mainnet"),
    }
    return mapping.get(chain_id, (None, None))


def resolve_symbol_auto(symbol, resolve_cache):
    # symbol ví dụ: ZEUS-USDT
    if symbol in resolve_cache:
        return resolve_cache[symbol]

    symbol_base = symbol.split("-")[0].strip()
    best = resolve_with_dexscreener(symbol_base)

    if not best:
        result = {
            "resolved": False,
            "resolve_confidence": 0.0,
            "reason": "no_dex_candidate",
            "chain_type": None,
            "network": None,
            "token_address": None,
            "resolved_name": None,
            "resolved_symbol": None,
            "last_verified_at": utc_now_str(),
        }
        resolve_cache[symbol] = result
        return result

    chain_type, network = map_chain_to_moralis(best["chain_id"])
    confidence = float(best["score"])

    resolved = (
        confidence >= 60 and
        best.get("token_address") and
        chain_type is not None and
        network is not None
    )

    result = {
        "resolved": resolved,
        "resolve_confidence": round(confidence, 2),
        "reason": "ok" if resolved else "low_confidence_or_unsupported_chain",
        "chain_type": chain_type,
        "network": network,
        "token_address": best.get("token_address"),
        "resolved_name": best.get("resolved_name"),
        "resolved_symbol": best.get("resolved_symbol"),
        "last_verified_at": utc_now_str(),
    }

    resolve_cache[symbol] = result
    return result

# =========================================================
# TRACKING
# =========================================================
TRACKING_COLUMNS = [
    "symbol",
    "first_seen_at",
    "last_seen_at",
    "days_tracking",
    "status_first",
    "status_current",
    "last_status_change_at",
    "price_first",
    "price_last",
    "performance_pct",
    "top10_holder_pct",
    "holder_flag",
    "resolve_confidence",
    "hunter_priority",
    "score_peak",
    "score_last",
    "seen_count",
    "note"
]


def load_tracking_df(sh):
    ws = get_or_create_ws(sh, "TRACKING_90D", rows=6000, cols=40)
    df = worksheet_to_df(ws)
    if df.empty:
        return pd.DataFrame(columns=TRACKING_COLUMNS)
    for c in TRACKING_COLUMNS:
        if c not in df.columns:
            df[c] = ""
    return df[TRACKING_COLUMNS]


def upsert_tracking(old_tracking_df: pd.DataFrame, current_df: pd.DataFrame):
    now_s = utc_now_str()
    today = utc_now().date()

    tracking_map = {}
    if not old_tracking_df.empty:
        for _, r in old_tracking_df.iterrows():
            tracking_map[str(r["symbol"]).strip()] = dict(r)

    for _, r in current_df.iterrows():
        sym = str(r["symbol"]).strip()
        if not sym:
            continue

        price = safe_float(r["price"], 0.0)
        status = str(r["status"])
        score = safe_float(r["score"], 0.0)
        holder_pct = r.get("top10_holder_pct")
        holder_flag = r.get("holder_flag", "")
        priority = r.get("hunter_priority", "")
        resolve_conf = safe_float(r.get("resolve_confidence"), 0.0)

        if sym not in tracking_map:
            tracking_map[sym] = {
                "symbol": sym,
                "first_seen_at": now_s,
                "last_seen_at": now_s,
                "days_tracking": "0",
                "status_first": status,
                "status_current": status,
                "last_status_change_at": now_s,
                "price_first": str(price),
                "price_last": str(price),
                "performance_pct": "0",
                "top10_holder_pct": "" if holder_pct is None else str(holder_pct),
                "holder_flag": holder_flag,
                "resolve_confidence": str(resolve_conf),
                "hunter_priority": priority,
                "score_peak": str(score),
                "score_last": str(score),
                "seen_count": "1",
                "note": ""
            }
        else:
            item = tracking_map[sym]
            first_seen_raw = item.get("first_seen_at", now_s)
            try:
                first_seen_dt = datetime.strptime(first_seen_raw, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            except Exception:
                first_seen_dt = utc_now()

            item["last_seen_at"] = now_s
            item["days_tracking"] = str((today - first_seen_dt.date()).days)

            prev_status = str(item.get("status_current", "")).strip()
            if prev_status != status:
                item["last_status_change_at"] = now_s

            item["status_current"] = status
            item["price_last"] = str(price)

            price_first = safe_float(item.get("price_first", 0.0), 0.0)
            perf = ((price / (price_first + 1e-9)) - 1.0) * 100 if price_first > 0 else 0.0
            item["performance_pct"] = str(round(perf, 2))

            old_peak = safe_float(item.get("score_peak", 0.0), 0.0)
            item["score_peak"] = str(max(old_peak, score))
            item["score_last"] = str(score)
            item["hunter_priority"] = priority
            item["holder_flag"] = holder_flag
            item["resolve_confidence"] = str(resolve_conf)

            if holder_pct is not None:
                item["top10_holder_pct"] = str(holder_pct)

            old_count = int(float(item.get("seen_count", "0") or 0))
            item["seen_count"] = str(old_count + 1)

    out = pd.DataFrame(list(tracking_map.values()))
    for c in TRACKING_COLUMNS:
        if c not in out.columns:
            out[c] = ""

    out = out[TRACKING_COLUMNS].copy()
    out["days_tracking_num"] = out["days_tracking"].apply(lambda x: safe_float(x, 9999))
    out = out[out["days_tracking_num"] <= TRACKING_MAX_DAYS].copy()
    out = out.drop(columns=["days_tracking_num"])
    return out


# =========================================================
# LOG HELPERS
# =========================================================
def build_alert_log_rows(old_tracking_df: pd.DataFrame, current_df: pd.DataFrame):
    rows = []
    now_s = utc_now_str()

    old_map = {}
    if not old_tracking_df.empty:
        for _, r in old_tracking_df.iterrows():
            old_map[str(r["symbol"]).strip()] = dict(r)

    for _, r in current_df.iterrows():
        sym = str(r["symbol"]).strip()
        new_status = str(r["status"])
        score = r["score"]
        top10 = r.get("top10_holder_pct")
        old = old_map.get(sym)

        if old is None:
            rows.append([now_s, sym, "NEW", new_status, str(score), "" if pd.isna(top10) else str(top10), "first seen"])
        else:
            old_status = str(old.get("status_current", "")).strip()
            if old_status != new_status:
                rows.append([now_s, sym, old_status, new_status, str(score), "" if pd.isna(top10) else str(top10), "status changed"])
    return rows


def build_snapshot_rows(current_df: pd.DataFrame):
    now_s = utc_now_str()
    rows = []
    for _, r in current_df.iterrows():
        rows.append([
            now_s,
            r["symbol"],
            r["price"],
            r["status"],
            r["hunter_priority"],
            r["score"],
            r["vol_ratio"],
            r["compression"],
            r["break_pressure"],
            "" if pd.isna(r["top10_holder_pct"]) else r["top10_holder_pct"],
            r["holder_flag"],
            r["resolve_confidence"],
        ])
    return rows


def build_resolve_cache_df(resolve_cache: dict):
    rows = []
    for symbol, info in resolve_cache.items():
        on = info.get("onchain_candidate") or {}
        coin = info.get("coin_candidate") or {}
        rows.append({
            "symbol": symbol,
            "resolved": info.get("resolved"),
            "resolve_confidence": info.get("resolve_confidence"),
            "reason": info.get("reason"),
            "chain_type": info.get("chain_type"),
            "network": info.get("network"),
            "token_address": info.get("token_address"),
            "resolved_symbol": info.get("resolved_symbol"),
            "resolved_name": info.get("resolved_name"),
            "cg_coin_id": coin.get("id"),
            "cg_coin_symbol": coin.get("symbol"),
            "cg_coin_name": coin.get("name"),
            "pool_address": on.get("pool_address"),
            "reserve_usd": on.get("reserve_usd"),
            "volume_usd_h24": on.get("volume_usd_h24"),
            "last_verified_at": info.get("last_verified_at"),
        })
    return pd.DataFrame(rows)


def build_unresolved_queue_df(current_df: pd.DataFrame):
    df = current_df.copy()
    df = df[df["holder_flag"].isin(["UNRESOLVED"])].copy()
    if df.empty:
        return pd.DataFrame(columns=[
            "symbol", "status", "score", "resolve_confidence", "holder_note"
        ])
    return df[["symbol", "status", "score", "resolve_confidence", "holder_note"]].sort_values(
        ["score", "resolve_confidence"], ascending=[False, False]
    )


# =========================================================
# ALERT LOGIC
# =========================================================
def should_alert(row):
    status = str(row.get("status", ""))
    score = safe_float(row.get("score"), 0.0)
    vol_ratio = safe_float(row.get("vol_ratio"), 0.0)
    compression = safe_float(row.get("compression"), 99.0)
    break_pressure = safe_float(row.get("break_pressure"), 0.0)
    roc_10_pct = safe_float(row.get("roc_10_pct"), 0.0)

    # Chỉ gửi PRE_BREAK thật sự đẹp
    if status == "PRE_BREAK":
        return (
            score >= 8.0 and
            vol_ratio >= 1.35 and
            compression <= 0.80 and
            break_pressure >= 0.92 and
            roc_10_pct <= 12
        )

    # BREAKOUT chỉ lấy loại mới break sớm
    if status == "BREAKOUT":
        return (
            score >= 8.5 and
            break_pressure >= 0.985 and
            roc_10_pct <= 12
        )

    return False
def is_valid_candidate(row):
    price = safe_float(row.get("price"), 0)
    symbol = str(row.get("symbol", ""))
    resolve = safe_float(row.get("resolve_confidence"), 0)
    vol = safe_float(row.get("vol_ratio"), 0)
    comp = safe_float(row.get("compression"), 1)
    holder = safe_float(row.get("top10_holder_pct"), 0)

    # ❌ 1. Loại stablecoin (giá ~1)
    if 0.95 <= price <= 1.05:
        return False

    # ❌ 2. Resolve quá thấp (map không chắc)
    if resolve < 60:
        return False

    # ❌ 3. Không có volume thật
    if vol < 1.2:
        return False

    # ❌ 4. Không có nén (không phải setup)
    if comp > 0.85:
        return False

    # ❌ 5. Holder lỗi (quá vô lý)
    if holder > 100:
        return False

    return True
    
def format_alert(row):
    holder_txt = "N/A" if pd.isna(row["top10_holder_pct"]) else f"{row['top10_holder_pct']}%"
    return (
        "🔥 <b>SPOT GEM ALERT</b>\n\n"
        f"📌 {row['symbol']}\n"
        f"💰 Price: {row['price']}\n\n"
        f"🧭 Status: {row['status']}\n"
        f"📊 Score: {row['score']}\n"
        f"🎯 Priority: {row['hunter_priority']}\n\n"
        f"📈 Vol Ratio: {row['vol_ratio']}\n"
        f"🧱 Compression: {row['compression']}\n"
        f"🚀 Break Pressure: {row['break_pressure']}\n"
        f"🐋 Top10 Holder: {holder_txt}\n"
        f"🔎 Resolve Confidence: {row['resolve_confidence']}\n"
        f"🏷 Holder Flag: {row['holder_flag']}\n"
    )

def format_batch_alert(rows):
    lines = []
    lines.append("🔥 <b>SPOT GEM ALERT</b>")
    lines.append("Các coin diện <b>sắp chạy</b>:")

    for i, row in enumerate(rows, start=1):
        holder_txt = "N/A"
        if row.get("top10_holder_pct") not in [None, "", "nan"]:
            holder_txt = f"{row['top10_holder_pct']}%"

        lines.append("")
        lines.append(f"<b>{i}. {row['symbol']}</b>")
        lines.append(f"💰 Price: {row['price']}")
        lines.append(f"🧭 Status: {row['status']} | 📊 Score: {row['score']}")
        lines.append(
            f"📈 Vol: {row['vol_ratio']} | "
            f"🧱 Comp: {row['compression']} | "
            f"🚀 Break: {row['break_pressure']}"
        )
        lines.append(f"🐋 Holder: {holder_txt} | 🔎 Resolve: {row['resolve_confidence']}")

    lines.append("")
    lines.append("⚠️ Chỉ là radar săn coin sắp chạy, không phải tín hiệu vào lệnh tự động.")

    return "\n".join(lines)
# =========================================================
# MAIN
# =========================================================
def run():
    print("🚀 START OKX SPOT HUNTER V4")
    sent = cleanup_sent(load_sent())
    resolve_cache = json_load_file("resolve_cache.json", {})

    sh = init_sheet()

    print("📡 Fetching OKX spot tickers...")
    tickers = fetch_okx_spot_tickers()

    base_rows = []
    for t in tickers:
        inst_id = str(t.get("instId", "")).strip()
        if not inst_id:
            continue
        if ONLY_USDT_QUOTE and not inst_id.endswith("-USDT"):
            continue

        last_price = safe_float(t.get("last"), 0.0)
        if last_price <= 0 or last_price >= PRICE_MAX:
            continue

        try:
            candles = fetch_okx_candles(inst_id)
            f = calc_features(candles)
            status = classify_status(f)
            score = calc_score(f, status, holder_score=0.0)
            priority = calc_priority(status, score)

            row = {
                "symbol": inst_id,
                "price": round(last_price, 8),
                "status": status,
                "hunter_priority": priority,
                "score": score,
                "vol_ratio": None if not f else round(f["vol_ratio"], 4),
                "compression": None if not f else round(f["compression"], 4),
                "higher_low": None if not f else f["higher_low"],
                "break_pressure": None if not f else round(f["break_pressure"], 4),
                "roc_10_pct": None if not f else round(f["roc_10"] * 100, 2),
                "roc_20_pct": None if not f else round(f["roc_20"] * 100, 2),
                "pos_in_range_pct": None if not f else round(f["pos_in_range"] * 100, 2),
                "top10_holder_pct": None,
                "holder_flag": "NOT_CHECKED",
                "holder_score": 0.0,
                "holder_note": "",
                "resolve_confidence": 0.0,
                "chain_type": None,
                "network": None,
                "token_address": None,
                "resolved_name": None,
                "resolved_symbol": None,
                "time": utc_now_str(),
            }
            base_rows.append(row)
            time.sleep(SLEEP_BETWEEN_SYMBOLS)

        except Exception as e:
            base_rows.append({
                "symbol": inst_id,
                "price": round(last_price, 8),
                "status": "ERR",
                "hunter_priority": "AVOID",
                "score": 0.0,
                "vol_ratio": None,
                "compression": None,
                "higher_low": None,
                "break_pressure": None,
                "roc_10_pct": None,
                "roc_20_pct": None,
                "pos_in_range_pct": None,
                "top10_holder_pct": None,
                "holder_flag": "ERR",
                "holder_score": 0.0,
                "holder_note": str(e)[:150],
                "resolve_confidence": 0.0,
                "chain_type": None,
                "network": None,
                "token_address": None,
                "resolved_name": None,
                "resolved_symbol": None,
                "time": utc_now_str(),
            })

    df = pd.DataFrame(base_rows)
    if df.empty:
        print("❌ No rows.")
        return

    # sort trước, rồi chỉ enrich holder cho nhóm đáng chú ý
    df = df.sort_values(["score"], ascending=[False]).reset_index(drop=True)

    resolve_mask = (
        df["status"].isin(RESOLVE_STATUS_SET) |
        (df.index < RESOLVE_TOP_N)
    )

    for idx in df[resolve_mask].index.tolist():
        symbol = df.at[idx, "symbol"]
        try:
            resolved_info = resolve_symbol_auto(symbol, resolve_cache)

            df.at[idx, "resolve_confidence"] = resolved_info.get("resolve_confidence", 0.0)
            df.at[idx, "chain_type"] = resolved_info.get("chain_type")
            df.at[idx, "network"] = resolved_info.get("network")
            df.at[idx, "token_address"] = resolved_info.get("token_address")
            df.at[idx, "resolved_name"] = resolved_info.get("resolved_name")
            df.at[idx, "resolved_symbol"] = resolved_info.get("resolved_symbol")

            holder_info = get_holder_info_from_resolve(resolved_info)
            df.at[idx, "top10_holder_pct"] = holder_info["top10_holder_pct"]
            df.at[idx, "holder_flag"] = holder_info["holder_flag"]
            df.at[idx, "holder_score"] = holder_info["holder_score"]
            df.at[idx, "holder_note"] = holder_info["holder_note"]

            # recalc score sau khi có holder score
            holder_score = safe_float(holder_info["holder_score"], 0.0)

            f_fake = {
                "vol_ratio": safe_float(df.at[idx, "vol_ratio"], 0.0),
                "compression": safe_float(df.at[idx, "compression"], 1.0),
                "higher_low": str(df.at[idx, "higher_low"]).lower() == "true",
                "break_pressure": safe_float(df.at[idx, "break_pressure"], 0.0),
                "roc_10": safe_float(df.at[idx, "roc_10_pct"], 0.0) / 100.0,
            }
            new_score = calc_score(f_fake, df.at[idx, "status"], holder_score=holder_score)
            df.at[idx, "score"] = new_score
            df.at[idx, "hunter_priority"] = calc_priority(df.at[idx, "status"], new_score)

        except Exception as e:
            df.at[idx, "holder_flag"] = "ERR"
            df.at[idx, "holder_note"] = str(e)[:150]

    # final sort
    df = df.sort_values(["score", "top10_holder_pct"], ascending=[False, False], na_position="last").reset_index(drop=True)

    # anti-spam telegram
    # =========================
    # GOM ALERT THÀNH 1 TIN
    # =========================
    alert_rows = []
    for _, row in df.iterrows():
        key = f"{row['symbol']}_{row['status']}"
    
        if should_alert(row) and is_valid_candidate(row) and key not in sent:
            alert_rows.append(row.to_dict())
            sent[key] = {"time": utc_now().isoformat()}
    if alert_rows:
        alert_rows = sorted(alert_rows, key=lambda x: safe_float(x.get("score"), 0.0), reverse=True)
        alert_rows = alert_rows[:10]
        send_telegram(format_batch_alert(alert_rows))
    save_sent(sent)
    json_save_file("resolve_cache.json", resolve_cache)

    # sheet: HUNTER_BOARD
    ws_board = get_or_create_ws(sh, "HUNTER_BOARD", rows=6000, cols=40)
    overwrite_worksheet(ws_board, df)

    # sheet: TRACKING_90D
    old_tracking_df = load_tracking_df(sh)
    new_tracking_df = upsert_tracking(old_tracking_df, df)
    ws_tracking = get_or_create_ws(sh, "TRACKING_90D", rows=6000, cols=40)
    overwrite_worksheet(ws_tracking, new_tracking_df)

    # sheet: ALERT_LOG
    ws_alert = get_or_create_ws(sh, "ALERT_LOG", rows=20000, cols=20)
    if ws_alert.get_all_values() == []:
        ws_alert.update([["time", "symbol", "old_status", "new_status", "score", "top10_holder_pct", "note"]])
    append_rows(ws_alert, build_alert_log_rows(old_tracking_df, df))

    # sheet: SNAPSHOT_LOG
    ws_snap = get_or_create_ws(sh, "SNAPSHOT_LOG", rows=50000, cols=20)
    if ws_snap.get_all_values() == []:
        ws_snap.update([[
            "time", "symbol", "price", "status", "hunter_priority",
            "score", "vol_ratio", "compression", "break_pressure",
            "top10_holder_pct", "holder_flag", "resolve_confidence"
        ]])
    append_rows(ws_snap, build_snapshot_rows(df))

    # sheet: RESOLVE_CACHE
    ws_resolve = get_or_create_ws(sh, "RESOLVE_CACHE", rows=5000, cols=30)
    overwrite_worksheet(ws_resolve, build_resolve_cache_df(resolve_cache))

    # sheet: UNRESOLVED_QUEUE
    ws_unresolved = get_or_create_ws(sh, "UNRESOLVED_QUEUE", rows=3000, cols=20)
    overwrite_worksheet(ws_unresolved, build_unresolved_queue_df(df))

    # summary telegram
    top_df = df[df["status"].isin(["EARLY_FLOW", "PRE_BREAK", "BREAKOUT", "PUMPING"])].head(10)
    if not top_df.empty:
        lines = ["📊 <b>TOP HUNTER BOARD</b>", ""]
        for _, r in top_df.iterrows():
            holder_txt = "N/A" if pd.isna(r["top10_holder_pct"]) else f"{r['top10_holder_pct']}%"
            lines.append(
                f"• <b>{r['symbol']}</b> | {r['status']} | Score {r['score']} | Holder {holder_txt} | Resolve {r['resolve_confidence']}"
            )
        send_telegram("\n".join(lines))

    print("✅ DONE")
    print(df.head(20).to_string(index=False))


if __name__ == "__main__":
    try:
        run()
    except Exception:
        print("❌ FATAL ERROR")
        traceback.print_exc()
