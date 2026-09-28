
"""
GMI SENSEX Smart Money Flow v3 - engine
----------------------------------------
Live 1-minute decision-support engine for SENSEX option buying.

Data architecture:
- FYERS API v3 Option Chain REST polling: option-chain/OI snapshots.
- FYERS API v3 Data WebSocket: live LTP/OHLCV/volume for SENSEX, futures,
  and selected option contracts.
- 1-minute bars are built locally from live ticks.
- Option-chain snapshots are compared over time to calculate rolling
  intraday price/OI flow.
- No automatic order placement.

Important:
The engine infers positioning from price + OI behaviour. It does not
identify the actual institutional identity of any order.
"""

from __future__ import annotations
import json, math, os, threading, time, traceback
from collections import deque
from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

try:
    from fyers_apiv3 import fyersModel
    from fyers_apiv3.FyersWebsocket import data_ws
except Exception:
    fyersModel = None
    data_ws = None


IST = "Asia/Kolkata"


def now_ist():
    return pd.Timestamp.now(tz=IST)


def safe_float(x, default=np.nan):
    try:
        if x is None or x == "":
            return default
        return float(x)
    except Exception:
        return default


def nested_records(obj):
    out = []
    if isinstance(obj, list):
        if obj and all(isinstance(x, dict) for x in obj):
            out.append(obj)
        for x in obj:
            out.extend(nested_records(x))
    elif isinstance(obj, dict):
        for v in obj.values():
            out.extend(nested_records(v))
    return out


def first(d, keys, default=np.nan):
    if not isinstance(d, dict):
        return default
    for k in keys:
        if k in d and d[k] not in (None, ""):
            return d[k]
    return default


def parse_chain(raw: dict) -> pd.DataFrame:
    rows = []
    for arr in nested_records(raw):
        for d in arr:
            strike = safe_float(first(d, [
                "strike_price", "strikePrice", "strike", "strikePriceValue"
            ]))
            if pd.isna(strike):
                continue

            symbol = first(d, ["symbol", "option_symbol", "optionSymbol"], "")
            typ = first(d, ["option_type", "optionType", "type", "op_type"], "")
            if not typ and isinstance(symbol, str):
                u = symbol.upper()
                if u.endswith("CE"):
                    typ = "CE"
                elif u.endswith("PE"):
                    typ = "PE"

            typ = str(typ).upper()
            if typ not in ("CE", "PE"):
                continue

            rows.append({
                "symbol": symbol,
                "strike": strike,
                "type": typ,
                "ltp": safe_float(first(d, ["ltp", "LTP", "last_price", "lastPrice"])),
                "oi": safe_float(first(d, ["oi", "OI", "open_interest", "openInterest"])),
                "oich": safe_float(first(d, [
                    "oich", "oi_change", "oiChange", "OIChg"
                ])),
                "oipct": safe_float(first(d, [
                    "oipercent", "oi_percent", "oiChangePercent",
                    "oichp", "oich_percent"
                ])),
                "volume": safe_float(first(d, ["volume", "v", "vol"]), 0),
                "delta": safe_float(first(d, ["delta", "Delta"])),
                "iv": safe_float(first(d, ["iv", "IV"])),
                "bid": safe_float(first(d, ["bid", "bid_price", "bidPrice"])),
                "ask": safe_float(first(d, ["ask", "ask_price", "askPrice"])),
                "expiry": first(d, ["expiry", "expiryDate", "expiry_date"], ""),
            })

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    df = df.drop_duplicates(["symbol", "strike", "type"], keep="last")

    # Rebuild OI change where the API provides previous OI.
    for c in ("oi", "oich", "oipct"):
        if c not in df:
            df[c] = np.nan

    return df.reset_index(drop=True)


def option_flow(price_change, oi_change, typ):
    if pd.isna(price_change) or pd.isna(oi_change):
        return "N/A", 0

    pu = price_change > 0
    pdn = price_change < 0
    ou = oi_change > 0
    od = oi_change < 0

    if typ == "CE":
        if pu and ou: return "CE LONG BUILD", 3
        if pdn and ou: return "CE SHORT BUILD", -3
        if pu and od: return "CE SHORT COVER", 2
        if pdn and od: return "CE LONG UNWIND", -2
    else:
        if pu and ou: return "PE LONG BUILD", -3
        if pdn and ou: return "PE SHORT BUILD", 3
        if pu and od: return "PE SHORT COVER", -2
        if pdn and od: return "PE LONG UNWIND", 2

    return "FLAT", 0


@dataclass
class Config:
    client_id: str
    access_token: str
    underlying: str = "BSE:SENSEX-INDEX"
    futures_symbol: str = ""
    strikecount: int = 20
    wing: int = 5

    chain_poll_seconds: float = 3.0
    dashboard_seconds: float = 1.0

    # Signal window for 1-minute option buying.
    start_minute: int = 20       # 09:20
    end_hour: int = 15
    end_minute: int = 0          # 15:00

    bull_threshold: int = 65
    bear_threshold: int = -65

    relvol_threshold: float = 1.20
    rsi_bull: float = 55.0
    rsi_bear: float = 45.0

    # Option selection.
    min_option_volume: float = 1000
    min_delta_abs: float = 0.35
    max_delta_abs: float = 0.65

    # User-controlled risk display; NOT an auto-order engine.
    stop_loss_pct: float = 20.0
    target_pct: float = 30.0
    trailing_pct: float = 15.0

    telegram_bot_token: str = ""
    telegram_chat_id: str = ""

    state_file: str = "gmi_v3_state.json"
    log_file: str = "gmi_v3_signals.csv"


class OneMinuteBars:
    def __init__(self, maxlen=600):
        self.bars = deque(maxlen=maxlen)
        self.current = None
        self.lock = threading.Lock()

    def update(self, ts_ms, price, volume=0):
        if not price or pd.isna(price):
            return
        t = pd.Timestamp(ts_ms, unit="s", tz="UTC").tz_convert(IST)
        bucket = t.floor("min")

        with self.lock:
            if self.current is None or self.current["time"] != bucket:
                if self.current is not None:
                    self.bars.append(self.current.copy())
                self.current = {
                    "time": bucket,
                    "open": float(price),
                    "high": float(price),
                    "low": float(price),
                    "close": float(price),
                    "volume": float(volume or 0),
                }
            else:
                self.current["high"] = max(self.current["high"], float(price))
                self.current["low"] = min(self.current["low"], float(price))
                self.current["close"] = float(price)
                self.current["volume"] += float(volume or 0)

    def frame(self):
        with self.lock:
            rows = list(self.bars)
            if self.current:
                rows.append(self.current.copy())
        return pd.DataFrame(rows)


class GMIEngine:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.running = False
        self.lock = threading.RLock()

        self.rest = None
        self.ws = None
        self.ws_connected = False

        self.ticks = {}
        self.bars = OneMinuteBars()

        self.chain = pd.DataFrame()
        self.chain_prev = {}
        self.chain_first = {}
        self.chain_history = deque(maxlen=300)  # ~15 min at 3 sec

        self.underlying_ltp = np.nan
        self.future_ltp = np.nan
        self.future_oi = np.nan
        self.future_prev_oi = np.nan
        self.future_oi_change = np.nan
        self._future_oi_last = np.nan
        self._subscribed_symbols = set()

        self.selected_contracts = []
        self.atm = np.nan
        self.signal = "WAIT"
        self.score = 0
        self.reasons = []
        self.selected_option = ""
        self.selected_option_ltp = np.nan

        self.last_signal_key = ""
        self.last_signal_time = ""
        self.signal_log = []

        self.error = ""
        self.last_chain_time = ""
        self.last_tick_time = ""

    # -------------------------
    # FYERS REST
    # -------------------------
    def start(self):
        if fyersModel is None:
            raise RuntimeError("Install fyers-apiv3 before starting the engine.")

        self.rest = fyersModel.FyersModel(
            client_id=self.cfg.client_id,
            token=self.cfg.access_token,
            is_async=False,
            log_path="",
        )

        self.running = True

        threading.Thread(target=self._chain_loop, daemon=True).start()
        threading.Thread(target=self._websocket_loop, daemon=True).start()
        threading.Thread(target=self._analysis_loop, daemon=True).start()

    def stop(self):
        self.running = False
        try:
            if self.ws:
                self.ws.close_connection()
        except Exception:
            pass

    def _chain_loop(self):
        while self.running:
            try:
                # Omitting timestamp asks FYERS for current expiry.
                raw = self.rest.optionchain({
                    "symbol": self.cfg.underlying,
                    "strikecount": self.cfg.strikecount,
                    "timestamp": "",
                })

                chain = parse_chain(raw)
                if not chain.empty:
                    self._update_chain(chain)
                    self._update_futures_depth()
                    if self.ws and self.ws_connected:
                        self._subscribe_all([self.cfg.underlying, self.cfg.futures_symbol])
                    self.error = ""
                self.last_chain_time = now_ist().strftime("%H:%M:%S")
            except Exception as e:
                self.error = f"Option-chain: {e}"
            time.sleep(self.cfg.chain_poll_seconds)

    def _update_chain(self, chain):
        with self.lock:
            self.chain = chain.copy()

            if pd.notna(self.underlying_ltp):
                self.atm = min(
                    chain["strike"].dropna().unique(),
                    key=lambda x: abs(x - self.underlying_ltp)
                )
            else:
                self.atm = float(chain["strike"].median())

            snap = {}
            for _, r in chain.iterrows():
                sym = str(r["symbol"])
                ltp = r["ltp"]
                oi = r["oi"]
                prev = self.chain_prev.get(sym, {})

                if pd.isna(ltp):
                    pchg = np.nan
                else:
                    pchg = (
                        ltp - prev.get("ltp", ltp)
                        if "ltp" in prev else np.nan
                    )

                if pd.isna(oi):
                    oich = r["oich"]
                else:
                    oich = (
                        oi - prev.get("oi", oi)
                        if "oi" in prev else r["oich"]
                    )

                state, fscore = option_flow(pchg, oich, r["type"])

                snap[sym] = {
                    "strike": r["strike"],
                    "type": r["type"],
                    "ltp": ltp,
                    "oi": oi,
                    "oich": oich,
                    "oipct": r["oipct"],
                    "volume": r["volume"],
                    "delta": r["delta"],
                    "iv": r["iv"],
                    "bid": r["bid"],
                    "ask": r["ask"],
                    "pchg": pchg,
                    "flow_state": state,
                    "flow_score": fscore,
                }

                if sym not in self.chain_first:
                    self.chain_first[sym] = snap[sym].copy()

            self.chain_prev = snap
            self.chain_history.append({
                "time": now_ist().isoformat(),
                "atm": self.atm,
                "data": snap,
            })

    def _update_futures_depth(self):
        """Read futures OI from FYERS depth REST endpoint when available."""
        if not self.cfg.futures_symbol or self.rest is None:
            return
        try:
            raw = self.rest.depth({
                "symbol": self.cfg.futures_symbol,
                "ohlcv_flag": 1,
            })
            found = None
            for arr in nested_records(raw):
                for d in arr:
                    sym = str(first(d, ["symbol", "s"], ""))
                    if sym == self.cfg.futures_symbol:
                        found = d
                        break
                if found:
                    break
            if found is None and isinstance(raw, dict):
                d = raw.get("d")
                if isinstance(d, dict):
                    found = d.get(self.cfg.futures_symbol, d)
            if isinstance(found, dict):
                oi = safe_float(first(found, ["oi", "OI", "open_interest", "openInterest"]))
                if pd.notna(oi):
                    with self.lock:
                        self.future_prev_oi = self._future_oi_last
                        self.future_oi = oi
                        self.future_oi_change = (
                            oi - self._future_oi_last
                            if pd.notna(self._future_oi_last) else np.nan
                        )
                        self._future_oi_last = oi
        except Exception as e:
            self.error = f"Futures OI: {e}"

    # -------------------------
    # WebSocket
    # -------------------------
    def _websocket_loop(self):
        if data_ws is None:
            self.error = "FYERS websocket SDK unavailable."
            return

        while self.running:
            try:
                # Subscribe to the underlying + future first. Option symbols are
                # refreshed dynamically after every chain snapshot.
                symbols = [self.cfg.underlying]
                if self.cfg.futures_symbol:
                    symbols.append(self.cfg.futures_symbol)

                def on_message(msg):
                    self._on_tick(msg)

                def on_error(msg):
                    self.error = f"WebSocket error: {msg}"

                def on_close(msg):
                    self.ws_connected = False

                def on_open():
                    self.ws_connected = True
                    self._subscribe_all(symbols)

                token = f"{self.cfg.client_id}:{self.cfg.access_token}"
                self.ws = data_ws.FyersDataSocket(
                    access_token=token,
                    log_path="",
                    litemode=False,
                    write_to_file=False,
                    reconnect=True,
                    on_connect=on_open,
                    on_close=on_close,
                    on_error=on_error,
                    on_message=on_message,
                )
                self.ws.connect()

                # SDK blocks internally; if it returns, retry.
                time.sleep(2)

            except Exception as e:
                self.error = f"WebSocket: {e}"
                self.ws_connected = False
                time.sleep(5)

    def _subscribe_all(self, base_symbols=None):
        syms = list(base_symbols or [])
        with self.lock:
            # Subscribe to the closest ATM ± wing option contracts.
            if not self.chain.empty and pd.notna(self.atm):
                strikes = sorted(self.chain["strike"].dropna().unique())
                idx = min(range(len(strikes)), key=lambda i: abs(strikes[i] - self.atm))
                lo = max(0, idx - self.cfg.wing)
                hi = min(len(strikes), idx + self.cfg.wing + 1)
                chosen = set(strikes[lo:hi])
                opt_syms = self.chain[self.chain["strike"].isin(chosen)]["symbol"].dropna().tolist()
                syms.extend(opt_syms)
                self.selected_contracts = opt_syms

        syms = list(dict.fromkeys([s for s in syms if isinstance(s, str) and s]))
        new_syms = [x for x in syms if x not in self._subscribed_symbols]
        if new_syms and self.ws:
            try:
                self.ws.subscribe(symbols=new_syms, data_type="SymbolUpdate")
                self._subscribed_symbols.update(new_syms)
            except Exception as e:
                self.error = f"WebSocket subscribe: {e}"

    def _on_tick(self, msg):
        if not isinstance(msg, dict):
            return
        symbol = msg.get("symbol")
        if not symbol:
            return

        ltp = safe_float(msg.get("ltp"))
        if pd.isna(ltp):
            return

        ts = msg.get("last_traded_time") or msg.get("timestamp") or time.time()
        if ts > 10_000_000_000:
            ts = ts / 1000.0

        with self.lock:
            self.ticks[symbol] = {
                "ltp": ltp,
                "ch": safe_float(msg.get("ch")),
                "chp": safe_float(msg.get("chp")),
                "vol": safe_float(msg.get("vol_traded_today"), 0),
                "bid": safe_float(msg.get("bid_price")),
                "ask": safe_float(msg.get("ask_price")),
                "bid_size": safe_float(msg.get("bid_size"), 0),
                "ask_size": safe_float(msg.get("ask_size"), 0),
                "ts": ts,
            }

            if symbol == self.cfg.underlying:
                self.underlying_ltp = ltp
                self.bars.update(ts, ltp, 0)

            if symbol == self.cfg.futures_symbol:
                self.future_ltp = ltp

            if symbol in self.selected_contracts:
                # Track selected option live premium.
                pass

            self.last_tick_time = now_ist().strftime("%H:%M:%S")

    # -------------------------
    # Analysis
    # -------------------------
    def _analysis_loop(self):
        while self.running:
            try:
                self._analyze()
            except Exception as e:
                self.error = f"Analysis: {e}"
            time.sleep(self.cfg.dashboard_seconds)

    def _analyze(self):
        with self.lock:
            bars = self.bars.frame()
            chain = self.chain.copy()
            atm = self.atm
            now = now_ist()

            if chain.empty:
                return

            # Update ATM from live underlying.
            if pd.notna(self.underlying_ltp):
                atm = min(
                    chain["strike"].dropna().unique(),
                    key=lambda x: abs(x - self.underlying_ltp)
                )
                self.atm = atm

            tech = self._technical_score(bars)
            option = self._option_score(chain, atm)
            futures = self._futures_score()

            score = (
                tech["score"] +
                option["score"] +
                futures["score"]
            )
            score = int(max(-100, min(100, score)))

            reasons = tech["reasons"] + option["reasons"] + futures["reasons"]

            allowed_time = self._signal_window(now)
            signal = "WAIT"

            # Require both sides of the option chain to agree, plus underlying
            # structure and futures direction.
            if (
                allowed_time and
                score >= self.cfg.bull_threshold and
                option["bull_confluence"] and
                futures["bull"] and
                tech["bull"]
            ):
                signal = "BUY CE"

            elif (
                allowed_time and
                score <= self.cfg.bear_threshold and
                option["bear_confluence"] and
                futures["bear"] and
                tech["bear"]
            ):
                signal = "BUY PE"

            # Avoid repeating the same signal every second.
            key = f"{signal}|{atm}|{now.floor('min')}"
            if signal in ("BUY CE", "BUY PE") and key != self.last_signal_key:
                self.last_signal_key = key
                self.last_signal_time = now.strftime("%H:%M:%S")
                selected = option["preferred_ce"] if signal == "BUY CE" else option["preferred_pe"]
                self.selected_option = selected or ""
                self.selected_option_ltp = self._live_ltp(selected)
                self._record_signal(signal, score, reasons, selected)

            self.signal = signal
            self.score = score
            self.reasons = reasons

    def _signal_window(self, now):
        mins = now.hour * 60 + now.minute
        return (9 * 60 + self.cfg.start_minute) <= mins <= (
            self.cfg.end_hour * 60 + self.cfg.end_minute
        )

    def _technical_score(self, bars):
        if len(bars) < 25:
            return {
                "score": 0, "bull": False, "bear": False,
                "reasons": ["Waiting for 25 one-minute bars"]
            }

        d = bars.copy()
        c = d["close"].astype(float)
        h = d["high"].astype(float)
        l = d["low"].astype(float)
        v = d["volume"].astype(float)

        ema9 = c.ewm(span=9, adjust=False).mean()
        ema21 = c.ewm(span=21, adjust=False).mean()

        delta = c.diff()
        gain = delta.clip(lower=0).rolling(14).mean()
        loss = (-delta.clip(upper=0)).rolling(14).mean()
        rs = gain / loss.replace(0, np.nan)
        rsi = (100 - 100 / (1 + rs)).iloc[-1]

        typical = (h + l + c) / 3
        vol_cum = v.cumsum().replace(0, np.nan)
        vwap = (typical * v).cumsum() / vol_cum
        vwap_last = vwap.iloc[-1]

        volavg = v.rolling(20).mean().iloc[-1]
        relvol = v.iloc[-1] / volavg if volavg and not pd.isna(volavg) else 1.0

        score = 0
        reasons = []

        if c.iloc[-1] > vwap_last:
            score += 10; reasons.append("SENSEX > VWAP")
        else:
            score -= 10; reasons.append("SENSEX < VWAP")

        if ema9.iloc[-1] > ema21.iloc[-1]:
            score += 10; reasons.append("EMA9 > EMA21")
        else:
            score -= 10; reasons.append("EMA9 < EMA21")

        if rsi >= self.cfg.rsi_bull:
            score += 5; reasons.append(f"RSI {rsi:.1f} bullish")
        elif rsi <= self.cfg.rsi_bear:
            score -= 5; reasons.append(f"RSI {rsi:.1f} bearish")

        if relvol >= self.cfg.relvol_threshold:
            score += 5 if c.iloc[-1] > c.iloc[-2] else -5
            reasons.append(f"RelVol {relvol:.2f}x")

        # One-minute breakout confirmation.
        if c.iloc[-1] > h.iloc[-2]:
            score += 5; reasons.append("1m high breakout")
        elif c.iloc[-1] < l.iloc[-2]:
            score -= 5; reasons.append("1m low breakdown")

        return {
            "score": max(-30, min(30, score)),
            "bull": (
                c.iloc[-1] > vwap_last and
                ema9.iloc[-1] > ema21.iloc[-1] and
                rsi >= self.cfg.rsi_bull
            ),
            "bear": (
                c.iloc[-1] < vwap_last and
                ema9.iloc[-1] < ema21.iloc[-1] and
                rsi <= self.cfg.rsi_bear
            ),
            "reasons": reasons,
            "rsi": float(rsi),
            "vwap": float(vwap_last),
            "relvol": float(relvol),
            "ema9": float(ema9.iloc[-1]),
            "ema21": float(ema21.iloc[-1]),
        }

    def _option_score(self, chain, atm):
        strikes = sorted(chain["strike"].dropna().unique())
        if not strikes:
            return {"score": 0, "bull_confluence": False, "bear_confluence": False,
                    "reasons": ["No strikes"]}

        idx = min(range(len(strikes)), key=lambda i: abs(strikes[i] - atm))
        lo = max(0, idx - self.cfg.wing)
        hi = min(len(strikes), idx + self.cfg.wing + 1)
        chosen = strikes[lo:hi]
        d = chain[chain["strike"].isin(chosen)].copy()

        # Recalculate flow using the rolling snapshot state.
        ce_scores, pe_scores = [], []
        ce_vol, pe_vol = 0, 0

        for _, r in d.iterrows():
            sym = str(r["symbol"])
            prev = self.chain_prev.get(sym, {})
            pchg = np.nan
            oich = np.nan

            # chain_prev is latest snapshot. chain history gives previous.
            if len(self.chain_history) >= 2:
                prev_snap = self.chain_history[-2]["data"].get(sym)
                if prev_snap:
                    if pd.notna(r["ltp"]) and pd.notna(prev_snap["ltp"]):
                        pchg = r["ltp"] - prev_snap["ltp"]
                    if pd.notna(r["oi"]) and pd.notna(prev_snap["oi"]):
                        oich = r["oi"] - prev_snap["oi"]

            state, fs = option_flow(pchg, oich, r["type"])

            # More weight to ATM; taper by strike distance.
            dist = abs(r["strike"] - atm)
            step = min([abs(strikes[i] - strikes[i-1]) for i in range(1, len(strikes))] or [100])
            weight = 1 / (1 + dist / max(1, step))

            # Volume intensity. Cap to prevent one contract dominating.
            vol_factor = 1.0
            if pd.notna(r["volume"]) and r["volume"] > 0:
                vol_factor += min(1.0, math.log10(max(1, r["volume"])) / 10)

            weighted = fs * weight * vol_factor

            if r["type"] == "CE":
                ce_scores.append(weighted)
                ce_vol += 0 if pd.isna(r["volume"]) else r["volume"]
            else:
                pe_scores.append(weighted)
                pe_vol += 0 if pd.isna(r["volume"]) else r["volume"]

        ce = float(np.sum(ce_scores)) if ce_scores else 0
        pe = float(np.sum(pe_scores)) if pe_scores else 0

        # Normalize each side approximately to +/-15.
        ce_norm = max(-15, min(15, ce * 2))
        pe_norm = max(-15, min(15, pe * 2))

        # ΔOI-PCR over current ATM ±5.
        ce_oi_ch = d.loc[d["type"] == "CE", "oich"].sum(skipna=True)
        pe_oi_ch = d.loc[d["type"] == "PE", "oich"].sum(skipna=True)
        doi_pcr = (
            pe_oi_ch / ce_oi_ch
            if ce_oi_ch and ce_oi_ch > 0 else np.nan
        )

        reasons = [
            f"CE flow {ce_norm:+.1f}",
            f"PE flow {pe_norm:+.1f}",
        ]

        if pd.notna(doi_pcr):
            if doi_pcr >= 1.20:
                reasons.append(f"ΔOI-PCR {doi_pcr:.2f} bullish")
            elif doi_pcr <= 0.80:
                reasons.append(f"ΔOI-PCR {doi_pcr:.2f} bearish")
            else:
                reasons.append(f"ΔOI-PCR {doi_pcr:.2f} neutral")

        # Preferred contract: closest available delta to 0.50 and liquid.
        def preferred(typ):
            x = d[d["type"] == typ].copy()
            if x.empty:
                return ""
            x["absdelta"] = x["delta"].abs()
            x = x[
                x["volume"].fillna(0) >= self.cfg.min_option_volume
            ]
            if x.empty:
                x = d[d["type"] == typ].copy()
            x["delta_distance"] = (x["absdelta"] - 0.50).abs()
            x = x[
                (x["absdelta"] >= self.cfg.min_delta_abs) &
                (x["absdelta"] <= self.cfg.max_delta_abs)
            ]
            if x.empty:
                return str(d[d["type"] == typ].sort_values(
                    "strike", key=lambda s: abs(s - atm)
                ).iloc[0]["symbol"])
            return str(x.sort_values("delta_distance").iloc[0]["symbol"])

        # Bullish confluence: call-side positive AND put-side positive
        # (PE writing/put long-unwind contributes positive directional score).
        bull_conf = ce_norm > 0 and pe_norm > 0
        bear_conf = ce_norm < 0 and pe_norm < 0

        return {
            "score": int(round(ce_norm + pe_norm)),
            "ce_flow": ce_norm,
            "pe_flow": pe_norm,
            "bull_confluence": bull_conf,
            "bear_confluence": bear_conf,
            "reasons": reasons,
            "doi_pcr": doi_pcr,
            "preferred_ce": preferred("CE"),
            "preferred_pe": preferred("PE"),
        }

    def _futures_score(self):
        if not self.cfg.futures_symbol:
            return {"score": 0, "bull": False, "bear": False,
                    "reasons": ["Futures symbol not configured"]}

        ch = self.ticks.get(self.cfg.futures_symbol, {}).get("ch", np.nan)
        if pd.isna(ch):
            return {"score": 0, "bull": False, "bear": False,
                    "reasons": ["Waiting for futures tick"]}

        oich = self.future_oi_change

        if pd.notna(oich):
            if ch > 0 and oich > 0:
                return {"score": 20, "bull": True, "bear": False,
                        "reasons": [f"Futures LONG BUILDUP (OI {oich:+,.0f})"],
                        "oi": self.future_oi, "oich": oich}
            if ch < 0 and oich > 0:
                return {"score": -20, "bull": False, "bear": True,
                        "reasons": [f"Futures SHORT BUILDUP (OI {oich:+,.0f})"],
                        "oi": self.future_oi, "oich": oich}
            if ch > 0 and oich < 0:
                return {"score": 10, "bull": True, "bear": False,
                        "reasons": [f"Futures SHORT COVER (OI {oich:+,.0f})"],
                        "oi": self.future_oi, "oich": oich}
            if ch < 0 and oich < 0:
                return {"score": -10, "bull": False, "bear": True,
                        "reasons": [f"Futures LONG UNWIND (OI {oich:+,.0f})"],
                        "oi": self.future_oi, "oich": oich}

        if ch > 0:
            return {"score": 5, "bull": True, "bear": False,
                    "reasons": ["Futures price positive; OI unavailable"],
                    "oi": self.future_oi, "oich": self.future_oi_change}
        if ch < 0:
            return {"score": -5, "bull": False, "bear": True,
                    "reasons": ["Futures price negative; OI unavailable"],
                    "oi": self.future_oi, "oich": self.future_oi_change}

        return {"score": 0, "bull": False, "bear": False,
                "reasons": ["Futures neutral"],
                "oi": self.future_oi, "oich": self.future_oi_change}

    def _live_ltp(self, symbol):
        if not symbol:
            return np.nan
        return self.ticks.get(symbol, {}).get("ltp", np.nan)

    def _record_signal(self, signal, score, reasons, selected):
        rec = {
            "time": now_ist().strftime("%Y-%m-%d %H:%M:%S"),
            "signal": signal,
            "score": score,
            "underlying": self.underlying_ltp,
            "atm": self.atm,
            "contract": selected,
            "option_ltp": self._live_ltp(selected),
            "reasons": " | ".join(reasons),
        }
        self.signal_log.append(rec)

        p = Path(self.cfg.log_file)
        df = pd.DataFrame(self.signal_log)
        df.to_csv(p, index=False)

        self._telegram(
            f"GMI SENSEX v3\n{signal}\n"
            f"Score: {score}\nATM: {self.atm}\n"
            f"Contract: {selected}\n"
            f"Time: {rec['time']}"
        )

    def _telegram(self, text):
        if not self.cfg.telegram_bot_token or not self.cfg.telegram_chat_id:
            return
        try:
            import requests
            requests.post(
                f"https://api.telegram.org/bot{self.cfg.telegram_bot_token}/sendMessage",
                json={"chat_id": self.cfg.telegram_chat_id, "text": text},
                timeout=5,
            )
        except Exception:
            pass

    def snapshot(self):
        with self.lock:
            chain = self.chain.copy()

            if not chain.empty and pd.notna(self.atm):
                strikes = sorted(chain["strike"].unique())
                idx = min(range(len(strikes)), key=lambda i: abs(strikes[i] - self.atm))
                lo = max(0, idx - self.cfg.wing)
                hi = min(len(strikes), idx + self.cfg.wing + 1)
                chain = chain[chain["strike"].isin(strikes[lo:hi])].copy()

                # Attach rolling live flow fields.
                states = []
                for _, r in chain.iterrows():
                    sym = str(r["symbol"])
                    latest = self.chain_prev.get(sym, {})
                    states.append({
                        "symbol": sym,
                        "strike": r["strike"],
                        "type": r["type"],
                        "ltp": r["ltp"],
                        "oi": r["oi"],
                        "oich": latest.get("oich", r["oich"]),
                        "volume": r["volume"],
                        "delta": r["delta"],
                        "iv": r["iv"],
                        "flow_state": latest.get("flow_state", ""),
                        "flow_score": latest.get("flow_score", 0),
                    })
                chain = pd.DataFrame(states)

            oi_pcr = doi_pcr = vol_pcr = np.nan
            if not chain.empty:
                ce = chain[chain["type"] == "CE"]
                pe = chain[chain["type"] == "PE"]
                ce_oi = ce["oi"].sum(skipna=True)
                pe_oi = pe["oi"].sum(skipna=True)
                ce_doi = ce["oich"].sum(skipna=True)
                pe_doi = pe["oich"].sum(skipna=True)
                ce_vol = ce["volume"].sum(skipna=True)
                pe_vol = pe["volume"].sum(skipna=True)
                if ce_oi:
                    oi_pcr = pe_oi / ce_oi
                if ce_doi and ce_doi > 0:
                    doi_pcr = pe_doi / ce_doi
                if ce_vol:
                    vol_pcr = pe_vol / ce_vol

            return {
                "time": now_ist().strftime("%H:%M:%S"),
                "underlying": self.underlying_ltp,
                "future": self.future_ltp,
                "future_oi": self.future_oi,
                "future_oich": self.future_oi_change,
                "oi_pcr": oi_pcr,
                "doi_pcr": doi_pcr,
                "vol_pcr": vol_pcr,
                "atm": self.atm,
                "signal": self.signal,
                "score": self.score,
                "selected_option": self.selected_option,
                "selected_option_ltp": self.selected_option_ltp,
                "last_signal_time": self.last_signal_time,
                "ws_connected": self.ws_connected,
                "last_chain": self.last_chain_time,
                "last_tick": self.last_tick_time,
                "error": self.error,
                "reasons": list(self.reasons),
                "chain": chain,
                "bars": self.bars.frame(),
            }


_ENGINE = None


def get_engine(cfg: Config):
    global _ENGINE
    if _ENGINE is None:
        _ENGINE = GMIEngine(cfg)
        _ENGINE.start()
    return _ENGINE
