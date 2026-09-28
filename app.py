
import os, time
import numpy as np
import pandas as pd
import streamlit as st
from engine import Config, get_engine


st.set_page_config(
    page_title="GMI SENSEX Smart Money Flow v3",
    page_icon="📈",
    layout="wide",
)

st.markdown("""
<style>
.block {border:1px solid #ddd;border-radius:12px;padding:10px;margin-bottom:8px}
.sig {font-size:28px;font-weight:800;text-align:center;padding:14px;border-radius:12px}
.ce {background:#e8f7e8;color:#08752f;border:2px solid #149447}
.pe {background:#fdeaea;color:#a10d0d;border:2px solid #d43c3c}
.wait {background:#f3f3f3;color:#555;border:2px solid #aaa}
.warn {background:#fff6df;color:#8a6100;border:2px solid #d99b00}
</style>
""", unsafe_allow_html=True)

st.title("📈 GMI SENSEX Smart Money Flow v3")
st.caption("1-minute SENSEX option-buying decision dashboard • FYERS API v3")

def secret(name, default=""):
    try:
        value = st.secrets.get(name, None)
        if value not in (None, ""):
            return str(value)
    except Exception:
        pass
    return os.getenv(name, default)


with st.sidebar:
    st.header("FYERS")
    cloud_client_id = secret("FYERS_CLIENT_ID")
    cloud_token = secret("FYERS_ACCESS_TOKEN")
    cloud_underlying = secret("FYERS_SENSEX_SYMBOL", "BSE:SENSEX-INDEX")
    cloud_future = secret("FYERS_SENSEX_FUTURE", "")

    if cloud_client_id and cloud_token:
        st.success("FYERS credentials loaded securely from Cloud Secrets.")
        client_id = cloud_client_id
        token = cloud_token
    else:
        st.info("Enter FYERS credentials here, or add them later under Streamlit Cloud Secrets.")
        client_id = st.text_input("FYERS App ID", type="password")
        token = st.text_input("FYERS Access Token", type="password")

    underlying = st.text_input("SENSEX symbol", value=cloud_underlying)

    future = st.text_input(
        "SENSEX Futures symbol",
        value=cloud_future,
        help="Enter the active SENSEX futures symbol exactly as FYERS shows it."
    )

    st.divider()
    st.header("1-minute engine")
    chain_poll = st.slider("Option-chain refresh", 2, 10, 3)
    wing = st.slider("ATM wing", 3, 10, 5)
    strikecount = st.slider("FYERS strikecount", 5, 50, 20, 5)

    st.header("Signal")
    bull = st.slider("BUY CE score", 50, 90, 65)
    bear = st.slider("BUY PE score", -90, -50, -65)
    relvol = st.slider("Relative volume", 1.0, 2.5, 1.2, 0.1)
    rsi_bull = st.slider("RSI bullish", 52, 65, 55)
    rsi_bear = st.slider("RSI bearish", 35, 48, 45)

    st.header("Contract selection")
    min_vol = st.number_input("Minimum option volume", 0, 100000, 1000, 100)
    min_delta = st.slider("Minimum |Delta|", 0.20, 0.60, 0.35, 0.05)
    max_delta = st.slider("Maximum |Delta|", 0.40, 0.80, 0.65, 0.05)

    st.header("Risk display")
    sl = st.number_input("Option SL %", 1.0, 80.0, 20.0, 1.0)
    target = st.number_input("Option target %", 1.0, 200.0, 30.0, 1.0)
    trail = st.number_input("Trailing %", 1.0, 50.0, 15.0, 1.0)

    st.header("Optional Telegram")
    tg_token = secret("TELEGRAM_BOT_TOKEN")
    tg_chat = secret("TELEGRAM_CHAT_ID")

if not client_id or not token:
    st.warning("Enter your FYERS App ID and current access token in the sidebar.")
    st.info("Do not send your access token, secret key, PIN or TOTP to anyone.")
    st.stop()

cfg = Config(
    client_id=client_id,
    access_token=token,
    underlying=underlying,
    futures_symbol=future,
    strikecount=strikecount,
    wing=wing,
    chain_poll_seconds=chain_poll,
    bull_threshold=bull,
    bear_threshold=bear,
    relvol_threshold=relvol,
    rsi_bull=rsi_bull,
    rsi_bear=rsi_bear,
    min_option_volume=min_vol,
    min_delta_abs=min_delta,
    max_delta_abs=max_delta,
    stop_loss_pct=sl,
    target_pct=target,
    trailing_pct=trail,
    telegram_bot_token=tg_token,
    telegram_chat_id=tg_chat,
)

engine = get_engine(cfg)
snap = engine.snapshot()

c1, c2, c3, c4, c5, c6 = st.columns(6)
c1.metric("SENSEX", f"{snap['underlying']:,.2f}" if pd.notna(snap["underlying"]) else "—")
c2.metric("ATM", f"{snap['atm']:,.0f}" if pd.notna(snap["atm"]) else "—")
c3.metric("FLOW SCORE", f"{snap['score']:+d}")
c4.metric("FUTURE", f"{snap['future']:,.2f}" if pd.notna(snap["future"]) else "—")
c5.metric("FUT OI", f"{snap['future_oi']:,.0f}" if pd.notna(snap["future_oi"]) else "—")
c6.metric("WS", "CONNECTED" if snap["ws_connected"] else "OFFLINE")

p1, p2, p3, p4 = st.columns(4)
p1.metric("OI PCR", f"{snap['oi_pcr']:.2f}" if pd.notna(snap["oi_pcr"]) else "—")
p2.metric("ΔOI PCR", f"{snap['doi_pcr']:.2f}" if pd.notna(snap["doi_pcr"]) else "—")
p3.metric("Volume PCR", f"{snap['vol_pcr']:.2f}" if pd.notna(snap["vol_pcr"]) else "—")
p4.metric("Futures ΔOI", f"{snap['future_oich']:+,.0f}" if pd.notna(snap["future_oich"]) else "—")

sig = snap["signal"]
if sig == "BUY CE":
    css, title = "ce", "🟢 BUY CE"
elif sig == "BUY PE":
    css, title = "pe", "🔴 BUY PE"
elif abs(snap["score"]) >= 40:
    css, title = "warn", f"🟡 {('BULLISH' if snap['score'] > 0 else 'BEARISH')} — WAIT"
else:
    css, title = "wait", "⚪ WAIT"

st.markdown(f'<div class="sig {css}">{title}<br><small>Score {snap["score"]:+d}/100</small></div>', unsafe_allow_html=True)

if snap["signal"] in ("BUY CE", "BUY PE"):
    st.success(
        f"Preferred contract: **{snap['selected_option'] or 'not available'}**  |  "
        f"Live LTP: **{snap['selected_option_ltp'] if pd.notna(snap['selected_option_ltp']) else '—'}**  |  "
        f"Signal time: **{snap['last_signal_time']}**"
    )

st.subheader("🔥 ATM ±5 Smart Money Heatmap")

chain = snap["chain"].copy()
if chain.empty:
    st.info("Waiting for FYERS option-chain data...")
else:
    rows = []
    for strike in sorted(chain["strike"].unique()):
        ce = chain[(chain["strike"] == strike) & (chain["type"] == "CE")]
        pe = chain[(chain["strike"] == strike) & (chain["type"] == "PE")]
        cr = ce.iloc[0] if not ce.empty else None
        pr = pe.iloc[0] if not pe.empty else None

        rows.append({
            "Strike": int(strike),
            "CE Flow": cr["flow_state"] if cr is not None else "",
            "CE Score": cr["flow_score"] if cr is not None else 0,
            "CE LTP": cr["ltp"] if cr is not None else None,
            "CE OI": cr["oi"] if cr is not None else None,
            "CE ΔOI": cr["oich"] if cr is not None else None,
            "CE Vol": cr["volume"] if cr is not None else None,
            "PE LTP": pr["ltp"] if pr is not None else None,
            "PE OI": pr["oi"] if pr is not None else None,
            "PE ΔOI": pr["oich"] if pr is not None else None,
            "PE Vol": pr["volume"] if pr is not None else None,
            "PE Score": pr["flow_score"] if pr is not None else 0,
            "PE Flow": pr["flow_state"] if pr is not None else "",
        })

    heat = pd.DataFrame(rows)
    st.dataframe(
        heat.style
        .format({
            "CE LTP": "{:,.2f}", "PE LTP": "{:,.2f}",
            "CE OI": "{:,.0f}", "PE OI": "{:,.0f}",
            "CE ΔOI": "{:+,.0f}", "PE ΔOI": "{:+,.0f}",
            "CE Vol": "{:,.0f}", "PE Vol": "{:,.0f}",
        })
        .background_gradient(
            subset=["CE Score", "PE Score"],
            cmap="RdYlGn", vmin=-3, vmax=3
        ),
        use_container_width=True,
        hide_index=True,
    )

col1, col2 = st.columns(2)
with col1:
    st.subheader("🧠 Signal reasons")
    for r in snap["reasons"]:
        st.write("• " + r)

with col2:
    st.subheader("🎯 Risk display")
    entry = snap["selected_option_ltp"]
    if pd.notna(entry):
        st.write(f"Reference option LTP: **{entry:.2f}**")
        st.write(f"Configured SL ({sl:.0f}%): **{entry*(1-sl/100):.2f}**")
        st.write(f"Configured target ({target:.0f}%): **{entry*(1+target/100):.2f}**")
        st.write(f"Configured trailing ({trail:.0f}%): **{entry*(1-trail/100):.2f}**")
    else:
        st.write("No active signal/contract yet.")

st.subheader("📈 1-minute SENSEX bars")
bars = snap["bars"]
if not bars.empty:
    chart = bars.set_index("time")[["open","high","low","close"]].tail(120)
    st.line_chart(chart["close"])
else:
    st.info("Waiting for 1-minute tick aggregation.")

with st.expander("Current normalized chain data"):
    st.dataframe(chain, use_container_width=True, hide_index=True)

with st.expander("Engine status / errors"):
    st.write({
        "last_tick": snap["last_tick"],
        "last_chain": snap["last_chain"],
        "websocket": snap["ws_connected"],
        "error": snap["error"],
    })

st.caption(
    "Decision-support only. The score is an inference from observable market data; "
    "it is not a direct institutional-order identifier and does not guarantee an option move. "
    "Risk levels are user-configurable reference levels, not trade recommendations."
)

time.sleep(1)
st.rerun()
