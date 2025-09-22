#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Hyperliquid SPOT strategy (UETH/USDC, UBTC/USDC)

Policy (best-performing stats from backtests):
- Risk-on when EITHER ETH or BTC is strong vs USDC: z > Z_STRONG (default 0.5).
- Leadership via ratio z with neutral band: pick ETH if z_ratio > +RATIO_NEUTRAL,
  BTC if z_ratio < -RATIO_NEUTRAL, otherwise KEEP CURRENT (if flat, fall back to sign).
- Risk-off (USDC) only when BOTH are not strong.
- 100% allocation to one coin or cash (no partial tilts).

Includes:
- Timestamp-aligned candles
- Slippage/fee-aware bounded IOC execution
- Actual-fill accounting with balance refresh
- Rotating logs + orders.csv + events.csv
- /health endpoint
- Micro-TWAP slicing via MAX_SLICES and optional SLICE_DELAY_SEC per slice
"""

import os, time, uuid, csv, json
from pathlib import Path
import logging
from logging.handlers import RotatingFileHandler

import pandas as pd
import numpy as np
import requests
import ccxt

from flask import Flask
import threading

from hyperliquid.exchange import Exchange
from hyperliquid.utils import constants

from eth_account import Account

# ---------------------- Web health ----------------------
app = Flask(__name__)
last_run = {"time": None, "status": "starting"}

@app.route('/health')
def health():
    return {"status": "ok", "last_run": last_run}, 200

def start_health_server():
    port = int(os.environ.get("PORT", 8080))
    threading.Thread(
        target=lambda: app.run(host='0.0.0.0', port=port, use_reloader=False),
        daemon=True
    ).start()

# ---------------------- Env / Config ----------------------
HL_WALLET_ADDRESS = os.getenv('HL_WALLET_ADDRESS', '').strip()
HL_PRIVATE_KEY    = os.getenv('HL_PRIVATE_KEY', '').strip()
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', '').strip()
TELEGRAM_CHAT_ID   = os.getenv('TELEGRAM_CHAT_ID', '423818438').strip()

STARTING_VALUE     = float(os.getenv('STARTING_VALUE', '3205'))
DRY_RUN            = os.getenv('DRY_RUN', '0') == '1'

# >>> Best-performing stats defaults (env-overridable) <<<
AVG            = int(os.getenv('AVG', '30'))          # 30-day window
Z_STRONG       = float(os.getenv('Z_STRONG', '0.5'))  # strength threshold
RATIO_NEUTRAL  = float(os.getenv('RATIO_NEUTRAL', '0.25'))  # neutral band for z_ratio

# Execution & risk params
MAX_SLIPPAGE_BPS     = int(os.getenv('MAX_SLIPPAGE_BPS', '10'))   # 0.10%
ORDERBOOK_LIMIT      = int(os.getenv('ORDERBOOK_LIMIT', '50'))
MIN_QUOTE_TO_TRADE   = float(os.getenv('MIN_QUOTE_TO_TRADE', '12.0'))
TARGET_TOL_PCT       = float(os.getenv('TARGET_TOL_PCT', '0.99'))
MAX_SLICES           = int(os.getenv('MAX_SLICES', '5'))
SLICE_DELAY_SEC      = float(os.getenv('SLICE_DELAY_SEC', '0'))   # micro-TWAP pacing
MAX_REBALANCE_STEPS  = int(os.getenv('MAX_REBALANCE_STEPS', '6'))
SLEEP_SEC            = int(os.getenv('SLEEP_SEC', str(1 * 15 * 60)))
TELEGRAM_SPAM_GUARD  = int(os.getenv('TELEGRAM_SPAM_GUARD', '1'))

assert HL_WALLET_ADDRESS and HL_PRIVATE_KEY, "Set HL_WALLET_ADDRESS and HL_PRIVATE_KEY."

# Network selection (you want MAINNET)
HL_NET = os.getenv("HL_NET", "mainnet").lower()  # 'mainnet' or 'testnet'
API_URL = constants.MAINNET_API_URL if HL_NET == "mainnet" else constants.TESTNET_API_URL
on_testnet = (HL_NET == "testnet")

# Build a LocalAccount from your API wallet private key
_api_pk = HL_PRIVATE_KEY.strip()
if not _api_pk.startswith("0x"):
    _api_pk = "0x" + _api_pk
agent_account = Account.from_key(_api_pk)

# IMPORTANT: pass the agent (API wallet) account, the API URL, and your MAIN account address
hl_exch = Exchange(agent_account, API_URL, account_address=HL_WALLET_ADDRESS)



# ---------------------- Exchange ----------------------
exchange = ccxt.hyperliquid({
    'walletAddress': HL_WALLET_ADDRESS,
    'privateKey': HL_PRIVATE_KEY,
    'enableRateLimit': True,
    'options': {'defaultType': 'spot'},
})
exchange.load_markets()

# Markets (Hyperliquid spot)
ETH_SYM = 'UETH/USDC'
BTC_SYM = 'UBTC/USDC'
QUOTE   = 'USDC'

# ---------------------- Logging ----------------------
Path("logs").mkdir(parents=True, exist_ok=True)
logger = logging.getLogger("hl_spot_bot")
logger.setLevel(logging.INFO)
fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
fh = RotatingFileHandler("logs/trader.log", maxBytes=2_000_000, backupCount=5)
fh.setFormatter(fmt); logger.addHandler(fh)
ch = logging.StreamHandler(); ch.setFormatter(fmt); logger.addHandler(ch)

ORDERS_CSV = Path("logs/orders.csv")
EVENTS_CSV = Path("logs/events.csv")

def derive_api_address_from_key(pk_hex: str) -> str:
    pk = pk_hex.strip()
    if not pk.startswith("0x"):
        pk = "0x" + pk
    return Account.from_key(pk).address

API_WALLET_ADDRESS = derive_api_address_from_key(HL_PRIVATE_KEY)

logger.info(f"[PRECHECK] Main wallet: {HL_WALLET_ADDRESS}")
logger.info(f"[PRECHECK] API wallet : {API_WALLET_ADDRESS}")

if API_WALLET_ADDRESS.lower() == HL_WALLET_ADDRESS.lower():
    raise SystemExit("HL_PRIVATE_KEY derives SAME address as HL_WALLET_ADDRESS. "
                     "Use the *API wallet* private key created in HL (More → API).")

# Preflight: this *must* succeed for spot
try:
    _ = exchange.fetch_balance({'type': 'spot'})
except Exception as e:
    raise SystemExit(f"[PRECHECK] Spot balance failed for API wallet {API_WALLET_ADDRESS}: {e}")

def _fmt_usd(x: float) -> str:
    return f"${x:,.2f}"

def _fmt_qty(x: float) -> str:
    # show up to 6 decimals, trim trailing zeros
    return f"{x:.6f}".rstrip('0').rstrip('.')

def _fmt_pct(x: float) -> str:
    return f"{100*x:.2f}%"

def _leg_line(rep: dict) -> str | None:
    if not rep or not rep.get("slices"):
        return None
    sym = rep["symbol"]
    act = rep["action"]
    avg_px = rep["slices"][-1].get("avg_px_realized")
    if act == "SELL":
        return (
            f"{act} {sym}: {_fmt_qty(rep['total_base'])} @ ~{_fmt_usd(avg_px)} "
            f"→ proceeds {_fmt_usd(rep['total_usdc'])} "
            f"({len(rep['slices'])} slice{'s' if len(rep['slices'])!=1 else ''})"
        )
    # BUY
    return (
        f"{act} {sym}: {_fmt_qty(rep['total_base'])} @ ~{_fmt_usd(avg_px)} "
        f"→ spent {_fmt_usd(rep['total_usdc'])} "
        f"({len(rep['slices'])} slice{'s' if len(rep['slices'])!=1 else ''})"
    )

def _position_line(pct: dict, assets: dict) -> str:
    return (
        f"New position — ETH {_fmt_pct(pct['ETH'])} (UETH {_fmt_qty(assets['UETH'])}), "
        f"BTC {_fmt_pct(pct['BTC'])} (UBTC {_fmt_qty(assets['UBTC'])}), "
        f"USDC {_fmt_pct(pct['USDC'])} ({_fmt_usd(assets['USDC'])})."
    )

def _safe(val, fallback=0.0):
    try:
        x = float(val)
        return x if np.isfinite(x) else fallback
    except Exception:
        return fallback

def compute_triggers(df_latest: pd.Series) -> dict:
    """
    df_latest must include ETH, BTC, ETH_MA, ETH_SD, BTC_MA, BTC_SD, R_MA, R_SD.
    Returns trigger price levels for strength and leadership.
    """
    ETH = _safe(df_latest['ETH']); BTC = _safe(df_latest['BTC'])
    ETH_MA = _safe(df_latest['ETH_MA']); ETH_SD = max(_safe(df_latest['ETH_SD']), 1e-12)
    BTC_MA = _safe(df_latest['BTC_MA']); BTC_SD = max(_safe(df_latest['BTC_SD']), 1e-12)
    R_MA   = _safe(df_latest['R_MA']);   R_SD   = max(_safe(df_latest['R_SD']),   1e-12)

    # Strength: z > Z_STRONG
    eth_strong_px = ETH_MA + Z_STRONG * ETH_SD
    btc_strong_px = BTC_MA + Z_STRONG * BTC_SD

    # Leadership band ±RATIO_NEUTRAL, convert ratio levels into ETH price given current BTC
    ratio_up  = R_MA + RATIO_NEUTRAL * R_SD   # ETH/BTC boundary where ETH starts to lead
    ratio_dn  = R_MA - RATIO_NEUTRAL * R_SD   # ETH/BTC boundary where BTC starts to lead
    eth_lead_px = ratio_up * BTC              # ETH must be ABOVE this to favor ETH
    btc_lead_px = ratio_dn * BTC              # ETH must be BELOW this to favor BTC

    return {
        "current": {"ETH": ETH, "BTC": BTC},
        "strength": {"ETH>": eth_strong_px, "BTC>": btc_strong_px},
        "leadership": {"ETH_favored_if_ETH>": eth_lead_px, "BTC_favored_if_ETH<": btc_lead_px},
    }

def format_triggers_msg(tr: dict) -> str:
    ETH = tr["current"]["ETH"]; BTC = tr["current"]["BTC"]
    eth_str = tr["strength"]["ETH>"]; btc_str = tr["strength"]["BTC>"]
    eth_lead = tr["leadership"]["ETH_favored_if_ETH>"]; btc_lead = tr["leadership"]["BTC_favored_if_ETH<"]

    return (
        "Triggers — "
        f"Strength: ETH>{_fmt_usd(eth_str)}, BTC>{_fmt_usd(btc_str)} | "
        f"Leadership (given BTC={_fmt_usd(BTC)}): ETH favored if ETH>{_fmt_usd(eth_lead)}; "
        f"BTC favored if ETH<{_fmt_usd(btc_lead)} "
        f"(now ETH={_fmt_usd(ETH)})"
    )



def _append_csv(path: Path, header: list, row: list):
    new = not path.exists()
    with path.open("a", newline="") as f:
        w = csv.writer(f)
        if new: w.writerow(header)
        w.writerow(row)

def log_event(kind: str, message: str, extra: dict = None):
    logger.info(f"{kind}: {message} | extra={extra or {}}")
    _append_csv(EVENTS_CSV,
        ["ts","kind","message","extra_json"],
        [time.strftime("%Y-%m-%d %H:%M:%S"), kind, message, json.dumps(extra or {}, separators=(',',':'))]
    )

def log_order(row: dict):
    header = [
        "ts","symbol","side","tif","client_id","req_amount",
        "limit_px","best_bid","best_ask","min_px","max_px",
        "est_base","est_avg_px","est_slippage_pct","fee_rate","est_fee","est_quote",
        "levels_used","status","order_id","filled","avg_fill_px","raw_info"
    ]
    values = [
        time.strftime("%Y-%m-%d %H:%M:%S"),
        row.get("symbol"), row.get("side"), "IOC", row.get("client_id"), row.get("req_amount"),
        row.get("limit_px"), row.get("best_bid"), row.get("best_ask"), row.get("min_px"), row.get("max_px"),
        row.get("est_base"), row.get("est_avg_px"),
        row.get("est_slippage_pct"), row.get("fee_rate"), row.get("est_fee"), row.get("est_quote"),
        row.get("levels_used"), row.get("status"), row.get("order_id"), row.get("filled"),
        row.get("avg_fill_px"), json.dumps(row.get("raw_info"), separators=(',',':'))[:1000]
    ]
    _append_csv(ORDERS_CSV, header, values)

# ---------------------- Telegram ----------------------
_last_sent = 0
def send_message(message: str):
    global _last_sent
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    if TELEGRAM_SPAM_GUARD and (time.time() - _last_sent < 10):
        return
    _last_sent = time.time()

    wallet_tag = f"[{HL_WALLET_ADDRESS[:6]}…{HL_WALLET_ADDRESS[-4:]}]"
    msg = f"{wallet_tag} {message}"
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            data={"chat_id": TELEGRAM_CHAT_ID, "text": str(message)},
            timeout=10,
        )
    except Exception as e:
        logger.warning(f"Telegram post failed: {e}")

# ---------------------- Data / balances ----------------------
def fetch_data(interval='1d', lookback=max(AVG*4, 50)):
    cols = ['timestamp', 'Open', 'High', 'Low', 'Close', 'Volume']
    e = exchange.fetch_ohlcv(ETH_SYM, timeframe=interval, limit=lookback)
    b = exchange.fetch_ohlcv(BTC_SYM, timeframe=interval, limit=lookback)
    eth = pd.DataFrame(e, columns=cols); btc = pd.DataFrame(b, columns=cols)
    eth['timestamp'] = pd.to_datetime(eth['timestamp'], unit='ms')
    btc['timestamp'] = pd.to_datetime(btc['timestamp'], unit='ms')
    return eth, btc

def align_candles(eth: pd.DataFrame, btc: pd.DataFrame) -> pd.DataFrame:
    m = pd.merge(
        eth[['timestamp','Close']].rename(columns={'Close':'ETH'}),
        btc[['timestamp','Close']].rename(columns={'Close':'BTC'}),
        on='timestamp', how='inner'
    )
    return m

def get_price(symbol: str) -> float:
    t = exchange.fetch_ticker(symbol)
    return float(t['last'])

def get_spot_balances():
    bal = exchange.fetch_balance({'type': 'spot'})
    f = bal.get('free', {})
    usdc = float(f.get('USDC', 0) or 0)
    ueth = float(f.get('UETH', 0) or 0)
    ubtc = float(f.get('UBTC', 0) or 0)
    return usdc, ueth, ubtc

def get_account_value_usdc():
    usdc, ueth, ubtc = get_spot_balances()
    val = usdc
    if ueth > 0: val += ueth * get_price(ETH_SYM)
    if ubtc > 0: val += ubtc * get_price(BTC_SYM)
    assets = {'USDC': usdc, 'UETH': ueth, 'UBTC': ubtc}
    return val, assets

# ---------------------- Fees / precision ----------------------
def taker_fee(symbol: str, default=0.0007) -> float:
    mkt = exchange.markets.get(symbol, {})
    fee = mkt.get('taker', None)
    try: return float(fee) if fee is not None else default
    except Exception: return default

def price_to_precision(symbol: str, price: float) -> float:
    return float(exchange.price_to_precision(symbol, price))

def amount_to_precision(symbol: str, amount: float) -> float:
    return float(exchange.amount_to_precision(symbol, amount))

# ---------------------- Orderbook planning ----------------------
def _book(symbol: str):
    ob = exchange.fetch_order_book(symbol, limit=ORDERBOOK_LIMIT)
    bids = ob.get('bids', []) or []; asks = ob.get('asks', []) or []
    best_bid = bids[0][0] if bids else None
    best_ask = asks[0][0] if asks else None
    return bids, asks, best_bid, best_ask

def plan_buy(symbol: str, usdc_budget: float, max_slippage_bps: int, fee_rate: float):
    if usdc_budget <= MIN_QUOTE_TO_TRADE: return None
    bids, asks, best_bid, best_ask = _book(symbol)
    if not asks: return None
    max_price = best_ask * (1 + max_slippage_bps / 10_000.0)
    notional_budget = usdc_budget / (1 + fee_rate)
    quote = 0.0; base = 0.0; levels = 0
    for price, qty in asks:
        if price > max_price or notional_budget - quote <= 1e-12: break
        take_base = min(qty, (notional_budget - quote) / price)
        quote += take_base * price
        base  += take_base
        levels += 1
    if base <= 0: return None
    avg_px = quote / base
    fee    = quote * fee_rate
    spend  = quote + fee
    slip   = (avg_px / best_ask) - 1.0
    return {
        "symbol": symbol, "side": "buy",
        "best_bid": best_bid, "best_ask": best_ask,
        "max_px": max_price, "min_px": None,
        "est_base": base, "est_avg_px": avg_px, "est_fee": fee,
        "est_quote": spend, "levels": levels, "est_slippage_pct": slip * 100.0
    }

def plan_sell(symbol: str, base_available: float, max_slippage_bps: int, fee_rate: float):
    if base_available <= 0: return None
    bids, asks, best_bid, best_ask = _book(symbol)
    if not bids: return None
    min_price = best_bid * (1 - max_slippage_bps / 10_000.0)
    base = 0.0; quote = 0.0; levels = 0
    for price, qty in bids:
        if price < min_price or base_available - base <= 1e-12: break
        take = min(qty, base_available - base)
        base += take; quote += take * price; levels += 1
    if base <= 0: return None
    avg_px = quote / base
    fee    = quote * fee_rate
    proceeds = quote - fee
    slip   = 1.0 - (avg_px / best_bid)
    return {
        "symbol": symbol, "side": "sell",
        "best_bid": best_bid, "best_ask": best_ask,
        "max_px": None, "min_px": min_price,
        "est_base": base, "est_avg_px": avg_px, "est_fee": fee,
        "est_quote": proceeds, "levels": levels, "est_slippage_pct": slip * 100.0
    }

# ---------------------- IOC bounded execution ----------------------



def place_ioc_limit(symbol, side, base_amount, limit_price):
    """
    Place a spot IOC limit using the SDK 'order' interface, always on spot.
    """
    sdk_symbol = symbol     # e.g. 'UETH/USDC'
    qty  = float(amount_to_precision(symbol, base_amount))
    px   = float(price_to_precision(symbol, limit_price))
    tif  = "Ioc"
    cloid = f"sdk-{uuid.uuid4().hex}"   # for your logging only

    if qty <= 0:
        return {"id": None, "status": "skip_qty"}, cloid

    if DRY_RUN:
        logger.info(f"DRY_RUN {side.upper()} {sdk_symbol} qty={qty} px={px} tif={tif}")
        return {"id": None, "status": "dry_run", "filled": 0.0, "average": None, "info": {}}, cloid

    try:
        # SDK spot order (per the docs snippet you pasted)
        resp = hl_exch.order(sdk_symbol, (side == 'buy'), qty, px, {"limit": {"tif": tif}})
    except Exception as e:
        logger.error(f"[ORDER ERROR] {side.upper()} {sdk_symbol} failed: {e}")
        send_message(f"ORDER ERROR: {e}")
        raise

    status = resp.get("status", "ok") if isinstance(resp, dict) else "ok"
    if status not in ("ok", "success"):
        logger.error(f"[ORDER ERROR] status={status} resp={resp}")
        send_message(f"ORDER ERROR: status={status}")
        raise RuntimeError(f"Order rejected: {resp}")

    # Normalize for logging; IOC may return fill info in response['response']['data']['statuses'][0]
    order = {
        "id":      resp.get("order_id") if isinstance(resp, dict) else None,
        "status":  status,
        "filled":  resp.get("filled", 0.0) if isinstance(resp, dict) else 0.0,
        "average": resp.get("avgPx", px)   if isinstance(resp, dict) else px,
        "info":    resp,
    }
    return order, cloid





def market_sell_all(symbol: str, max_slippage_bps: int):
    """
    Sell BASE->USDC with bounded slippage. Slices (micro-TWAP).
    Returns a dict summary of slices and totals for reporting.
    """
    fee = taker_fee(symbol)
    base_asset = symbol.split('/')[0]  # 'UETH' or 'UBTC'
    slices = []
    total_base_filled = 0.0
    total_usdc_proceeds = 0.0

    for s in range(MAX_SLICES):
        # balances BEFORE slice
        usdc_b, ueth_b, ubtc_b = get_spot_balances()
        have_b = ueth_b if base_asset == 'UETH' else ubtc_b
        if have_b <= 0:
            break

        plan = plan_sell(symbol, have_b, max_slippage_bps, fee)
        if not plan:
            break
        notional_plan = plan["est_avg_px"] * plan["est_base"]
        if notional_plan < MIN_QUOTE_TO_TRADE:
            break

        order, cloid = place_ioc_limit(symbol, 'sell', plan["est_base"], plan["min_px"])

        # balances AFTER slice
        usdc_a, ueth_a, ubtc_a = get_spot_balances()
        have_a = ueth_a if base_asset == 'UETH' else ubtc_a

        base_filled = max(0.0, have_b - have_a)
        usdc_delta = max(0.0, usdc_a - usdc_b)
        avg_px_realized = (usdc_delta / base_filled) if base_filled > 0 else plan["est_avg_px"]

        slices.append({
            "side": "SELL",
            "symbol": symbol,
            "req_base": plan["est_base"],
            "limit_px": plan["min_px"],
            "base_filled": base_filled,
            "usdc_proceeds": usdc_delta,
            "avg_px_realized": avg_px_realized,
            "resp": order,
        })

        total_base_filled += base_filled
        total_usdc_proceeds += usdc_delta

        if base_filled <= 1e-12:
            break
        if SLICE_DELAY_SEC > 0:
            time.sleep(SLICE_DELAY_SEC)

    return {
        "action": "SELL",
        "symbol": symbol,
        "slices": slices,
        "total_base": total_base_filled,
        "total_usdc": total_usdc_proceeds,
    }


def market_buy_with_usdc(symbol: str, usdc_to_spend: float, max_slippage_bps: int):
    """
    Buy BASE with USDC within slippage bound. Slices (micro-TWAP).
    Returns a dict summary of slices and totals for reporting.
    """
    fee = taker_fee(symbol)
    slices = []
    total_base_bought = 0.0
    total_usdc_spent = 0.0

    for s in range(MAX_SLICES):
        # balances BEFORE slice
        usdc_b, ueth_b, ubtc_b = get_spot_balances()
        remaining = min(usdc_b, usdc_to_spend) if s == 0 else usdc_b
        if remaining < MIN_QUOTE_TO_TRADE:
            break

        plan = plan_buy(symbol, remaining, max_slippage_bps, fee)
        if not plan:
            break
        if plan["est_quote"] < MIN_QUOTE_TO_TRADE:
            break

        order, cloid = place_ioc_limit(symbol, 'buy', plan["est_base"], plan["max_px"])

        # balances AFTER slice
        usdc_a, ueth_a, ubtc_a = get_spot_balances()
        # detect which base we bought
        base_asset = symbol.split('/')[0]  # 'UETH' or 'UBTC'
        base_b = ueth_b if base_asset == 'UETH' else ubtc_b
        base_a = ueth_a if base_asset == 'UETH' else ubtc_a

        base_filled = max(0.0, base_a - base_b)
        usdc_delta = max(0.0, usdc_b - usdc_a)  # spent
        avg_px_realized = (usdc_delta / base_filled) if base_filled > 0 else plan["est_avg_px"]

        slices.append({
            "side": "BUY",
            "symbol": symbol,
            "req_base": plan["est_base"],
            "limit_px": plan["max_px"],
            "base_filled": base_filled,
            "usdc_spent": usdc_delta,
            "avg_px_realized": avg_px_realized,
            "resp": order,
        })

        total_base_bought += base_filled
        total_usdc_spent += usdc_delta

        if base_filled <= 1e-12:
            break
        if SLICE_DELAY_SEC > 0:
            time.sleep(SLICE_DELAY_SEC)

    return {
        "action": "BUY",
        "symbol": symbol,
        "slices": slices,
        "total_base": total_base_bought,
        "total_usdc": total_usdc_spent,
    }


# ---------------------- State / rebalancing ----------------------
def get_state_and_percents():
    total, a = get_account_value_usdc()
    total = max(total, 1e-9)
    pct_eth = (a['UETH'] * get_price(ETH_SYM) / total) if a['UETH'] > 0 else 0.0
    pct_btc = (a['UBTC'] * get_price(BTC_SYM) / total) if a['UBTC'] > 0 else 0.0
    pct_usd = a['USDC'] / total
    if pct_eth >= TARGET_TOL_PCT: state = 'ETH'
    elif pct_btc >= TARGET_TOL_PCT: state = 'BTC'
    elif pct_usd >= TARGET_TOL_PCT: state = 'CASH'
    else: state = 'MIXED'
    return state, {'ETH': pct_eth, 'BTC': pct_btc, 'USDC': pct_usd}, total, a

def rebalance_to_target(target: str) -> tuple[bool, str]:
    """
    Rebalance to target and return (changed, execution_message).
    Message includes per-leg totals and the new position split.
    """
    changed = False
    leg_reports = []

    for _ in range(MAX_REBALANCE_STEPS):
        state, pct, total, a = get_state_and_percents()
        if target == 'ETH' and pct['ETH'] >= TARGET_TOL_PCT: break
        if target == 'BTC' and pct['BTC'] >= TARGET_TOL_PCT: break
        if target == 'CASH' and pct['USDC'] >= TARGET_TOL_PCT: break

        before = a.copy()

        if target == 'ETH':
            if a['UBTC'] > 0.0:
                rep = market_sell_all(BTC_SYM, MAX_SLIPPAGE_BPS)
                leg_reports.append(rep)
            usdc, _, _ = get_spot_balances()
            if usdc > MIN_QUOTE_TO_TRADE:
                rep = market_buy_with_usdc(ETH_SYM, usdc, MAX_SLIPPAGE_BPS)
                leg_reports.append(rep)

        elif target == 'BTC':
            if a['UETH'] > 0.0:
                rep = market_sell_all(ETH_SYM, MAX_SLIPPAGE_BPS)
                leg_reports.append(rep)
            usdc, _, _ = get_spot_balances()
            if usdc > MIN_QUOTE_TO_TRADE:
                rep = market_buy_with_usdc(BTC_SYM, usdc, MAX_SLIPPAGE_BPS)
                leg_reports.append(rep)

        else:  # CASH
            if a['UETH'] > 0.0:
                rep = market_sell_all(ETH_SYM, MAX_SLIPPAGE_BPS)
                leg_reports.append(rep)
            if a['UBTC'] > 0.0:
                rep = market_sell_all(BTC_SYM, MAX_SLIPPAGE_BPS)
                leg_reports.append(rep)

        _, after = get_account_value_usdc()
        if any(abs(after[k] - before[k]) > 1e-12 for k in ('UETH','UBTC','USDC')):
            changed = True

    # Compose report
    leg_lines = [ln for ln in map(_leg_line, leg_reports) if ln]

    # New position split
    state, pct, total, a = get_state_and_percents()
    pos_line = _position_line(pct, a)

    if changed and leg_lines:
        msg = " • ".join(leg_lines) + " | " + pos_line
    elif changed:
        msg = "Rebalanced with minimal deltas. " + pos_line
    else:
        msg = "No fills this cycle. " + pos_line

    logger.info(f"[EXECUTION] {msg}")
    return changed, msg







# ---------------------- Signals ----------------------
_last_trade_bar_ts = None  # cooldown by bar time

def compute_signal(df: pd.DataFrame):
    """
    Returns ('ETH' | 'BTC' | 'CASH', diagnostics_dict, bar_ts)

    Strategy (best-performing stats):
    - If EITHER coin has z > Z_STRONG (default 0.5), be invested.
    - Use ratio z-score with neutral band RATIO_NEUTRAL (default 0.25):
        * z_ratio > +band -> ETH
        * z_ratio < -band -> BTC
        * |z_ratio| <= band -> keep current coin; if flat, fall back to sign of z_ratio.
    - Only go to cash if BOTH are weak (z <= Z_STRONG).
    """
    global _last_trade_bar_ts
    EPS = 1e-12

    df = df.copy()
    df['ETH_MA'] = df['ETH'].rolling(AVG).mean()
    df['ETH_SD'] = df['ETH'].rolling(AVG).std().clip(lower=EPS)
    df['BTC_MA'] = df['BTC'].rolling(AVG).mean()
    df['BTC_SD'] = df['BTC'].rolling(AVG).std().clip(lower=EPS)
    df['RATIO']  = df['ETH'] / df['BTC']
    df['R_MA']   = df['RATIO'].rolling(AVG).mean()
    df['R_SD']   = df['RATIO'].rolling(AVG).std().clip(lower=EPS)

    df['Z_ETH']   = (df['ETH'] - df['ETH_MA']) / df['ETH_SD']
    df['Z_BTC']   = (df['BTC'] - df['BTC_MA']) / df['BTC_SD']
    df['Z_RATIO'] = (df['RATIO'] - df['R_MA']) / df['R_SD']

    df = df.dropna()
    if len(df) < 2:
        return 'CASH', {"reason": "insufficient_bars"}, None

    cur = df.iloc[-1]
    bar_ts = pd.to_datetime(cur.name if df.index.name=='timestamp' else cur['timestamp'])

    eth_strong = cur['Z_ETH'] > Z_STRONG
    btc_strong = cur['Z_BTC'] > Z_STRONG
    either_strong = eth_strong or btc_strong
    both_weak = not either_strong

    # Cooldown: prevent multiple flips on the same bar; still allow risk-off
    cool = (_last_trade_bar_ts is not None) and (bar_ts == _last_trade_bar_ts)

    state, *_ = get_state_and_percents()
    target, reason = 'CASH', ""

    if both_weak:
        target, reason = 'CASH', "both_weak → risk_off"
    else:
        # Decide leader with neutral band; keep current if within band
        zr = float(cur['Z_RATIO'])
        if abs(zr) <= RATIO_NEUTRAL and state in ('ETH','BTC'):
            target, reason = state, f"either_strong, |z_ratio|<={RATIO_NEUTRAL} keep {state}"
        else:
            if zr > 0:
                target, reason = 'ETH', f"either_strong, ratio favors ETH (z_ratio={zr:.2f})"
            elif zr < 0:
                target, reason = 'BTC', f"either_strong, ratio favors BTC (z_ratio={zr:.2f})"
            else:
                # Exactly 0: break tie by higher individual z
                target = 'ETH' if cur['Z_ETH'] >= cur['Z_BTC'] else 'BTC'
                reason = f"either_strong, z_ratio=0 -> higher z: {target}"

    # Apply cooldown only for switching coin (not for moving to cash)
    if cool and target != 'CASH' and target != state:
        target, reason = state, f"cooldown active, maintaining {state}"

    diag = {
        "bar_ts": str(bar_ts),
        "Z_ETH": float(cur['Z_ETH']),
        "Z_BTC": float(cur['Z_BTC']),
        "Z_RATIO": float(cur['Z_RATIO']),
        "eth_strong": bool(eth_strong),
        "btc_strong": bool(btc_strong),
        "either_strong": bool(either_strong),
        "cooldown": bool(cool),
        "chosen": target,
        "reason": reason
    }
    return target, diag, bar_ts

# ---------------------- Main logic ----------------------
def trade_logic():
    eth, btc = fetch_data()
    m = align_candles(eth, btc).set_index('timestamp')  # aligned by time

    m['ETH_MA'] = m['ETH'].rolling(AVG).mean()
    m['ETH_SD'] = m['ETH'].rolling(AVG).std().clip(lower=1e-12)
    m['BTC_MA'] = m['BTC'].rolling(AVG).mean()
    m['BTC_SD'] = m['BTC'].rolling(AVG).std().clip(lower=1e-12)
    m['RATIO']  = m['ETH'] / m['BTC']
    m['R_MA']   = m['RATIO'].rolling(AVG).mean()
    m['R_SD']   = m['RATIO'].rolling(AVG).std().clip(lower=1e-12)

    target, diag, bar_ts = compute_signal(m)

    state, pct, total, a = get_state_and_percents()

    latest_row = m.iloc[-1]
    triggers = compute_triggers(latest_row)
    trig_line = format_triggers_msg(triggers)
    logger.info(f"[TRIGGERS] {trig_line}")

    ret = round((total - STARTING_VALUE) / max(STARTING_VALUE, 1e-9) * 100, 2)
    msg_prefix = (f"Equity ${total:,.2f} (ret {ret:+.2f}%) | "
                  f"USDC {a['USDC']:.2f} | UETH {a['UETH']:.6f} | UBTC {a['UBTC']:.6f}")

    log_event("signal", f"Decision: {target} ({diag['reason']})",
              {**diag, "state_before": state, "pct": pct})

    if target != state:
        changed, exec_msg = rebalance_to_target(target)
        global _last_trade_bar_ts
        _last_trade_bar_ts = bar_ts
        if changed:
            send_message(f"{msg_prefix}\n{exec_msg}\n{trig_line}")
        else:
            send_message(f"{msg_prefix}\nAttempted rebalance to {target}, but no fills. {exec_msg}\n{trig_line}")
    else:
        send_message(msg_prefix + f"\nNo change (state={state}).\n{trig_line}")



    last_run["time"] = time.time()
    last_run["status"] = "ok"

# ---------------------- Entrypoint ----------------------
if __name__ == "__main__":
    start_health_server()
    log_event("start", f"Bot starting. DRY_RUN={DRY_RUN}, MAX_SLIPPAGE_BPS={MAX_SLIPPAGE_BPS}, "
                       f"MIN_QUOTE_TO_TRADE={MIN_QUOTE_TO_TRADE}, AVG={AVG}, Z_STRONG={Z_STRONG}, "
                       f"RATIO_NEUTRAL={RATIO_NEUTRAL}, TARGET_TOL_PCT={TARGET_TOL_PCT}, "
                       f"MAX_SLICES={MAX_SLICES}, SLICE_DELAY_SEC={SLICE_DELAY_SEC}")
    while True:
        try:
            trade_logic()
        except Exception as e:
            logger.exception(f"Top-level error: {e}")
            send_message(f"Error: {e}")
            last_run["status"] = "error"
        time.sleep(SLEEP_SEC)
