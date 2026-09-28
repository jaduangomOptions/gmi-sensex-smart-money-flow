# GMI SENSEX Smart Money Flow — Web Tool

Browser-based Streamlit dashboard for SENSEX intraday decision support using FYERS API v3 market data.

## Dashboard
- SENSEX and ATM
- Futures price and futures OI / OI change when available
- OI PCR, ΔOI PCR and Volume PCR
- ATM ±5 option heatmap
- Rolling option price/OI flow states
- 1-minute VWAP / EMA / RSI / relative-volume confirmation
- BUY CE / BUY PE / WAIT decision banner
- Preferred option contract based on delta/liquidity filters
- Reference SL/target/trailing calculations

## Important
This tool does **not** place orders. “Smart money/institutional flow” is an inference from observable market data; it does not identify the actual institution behind an order and does not guarantee a profitable trade.

## Streamlit Cloud Secrets
In Streamlit Community Cloud, open the app settings and add these secrets:

```toml
FYERS_CLIENT_ID = "YOUR_FYERS_APP_ID"
FYERS_ACCESS_TOKEN = "YOUR_CURRENT_FYERS_ACCESS_TOKEN"
FYERS_SENSEX_SYMBOL = "BSE:SENSEX-INDEX"
FYERS_SENSEX_FUTURE = "YOUR_ACTIVE_FYERS_SENSEX_FUTURES_SYMBOL"
```

Optional Telegram:

```toml
TELEGRAM_BOT_TOKEN = ""
TELEGRAM_CHAT_ID = ""
```

Never commit a secrets file or access token to GitHub.
