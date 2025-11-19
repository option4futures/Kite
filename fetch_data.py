#!/usr/bin/env python
# -----------------------------
# SENSEX Option Chain Live Update → Google Sheets
# Works perfectly on GitHub Actions (Nov 2025)
# -----------------------------

import os
import sys
import json
import pytz
from datetime import datetime, time

import gspread
from google.oauth2.service_account import Credentials
from kiteconnect import KiteConnect
from gspread.exceptions import WorksheetNotFound


# -----------------------------
# 0. CONFIG
# -----------------------------
SHEET_ID = os.getenv("SHEET_ID")
GOOGLE_CREDS_JSON = os.getenv("GOOGLE_CREDENTIALS")  # Full JSON string from secret
API_KEY = os.getenv("API_KEY")
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")

# Define SENSEX expiries → tab names
EXPIRIES = [
    ("2025-11-20", "SENSEX_Exp_1"),
    # Add more as needed: ("2025-11-27", "SENSEX_Exp_2"), etc.
]


# -----------------------------
# 1. Market Open Check (IST)
# -----------------------------
ist = pytz.timezone("Asia/Kolkata")
now = datetime.now(ist)
current_time = now.time()
current_weekday = now.weekday()  # 0=Mon, 5=Sat, 6=Sun

market_open = time(9, 15)   # 9:15 AM IST
market_close = time(17, 30) # 3:30 PM IST (BSE Equity close)

if current_weekday >= 5 or not (market_open <= current_time <= market_close):
    print(f"Market closed or weekend. Current: {now.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    sys.exit(0)

print(f"Market is open. Time: {now.strftime('%H:%M:%S %Z')}")


# -----------------------------
# 2. Setup KiteConnect
# -----------------------------
if not API_KEY or not ACCESS_TOKEN:
    raise Exception("Missing API_KEY or ACCESS_TOKEN!")

kite = KiteConnect(api_key=API_KEY)
kite.set_access_token(ACCESS_TOKEN)


# -----------------------------
# 3. Setup Google Sheets (Modern google-auth)
# -----------------------------
if not SHEET_ID:
    raise Exception("Missing SHEET_ID!")
if not GOOGLE_CREDS_JSON:
    raise Exception("Missing GOOGLE_CREDENTIALS secret!")

creds_dict = json.loads(GOOGLE_CREDS_JSON)

scopes = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
client = gspread.authorize(creds)
spreadsheet = client.open_by_key(SHEET_ID)

print("Google Sheets connected successfully!")


# -----------------------------
# 4. Process Each Expiry
# -----------------------------
for expiry, sheet_name in EXPIRIES:
    print(f"\nProcessing {expiry} → Tab: {sheet_name}")

    try:
        # Get or create worksheet
        try:
            sheet = spreadsheet.worksheet(sheet_name)
            print(f"Found existing tab: {sheet_name}")
        except WorksheetNotFound:
            sheet = spreadsheet.add_worksheet(title=sheet_name, rows=1000, cols=20)
            print(f"Created new tab: {sheet_name}")

        # Read previous OI to calculate change
        prev_oi_dict = {}
        existing = sheet.get_all_values()
        if len(existing) > 1:
            headers = existing[0]
            try:
                strike_idx = headers.index("Strike")
                call_oi_idx = headers.index("Call OI")
                put_oi_idx = headers.index("Put OI")
                for row in existing[1:]:
                    if len(row) <= max(strike_idx, call_oi_idx, put_oi_idx):
                        continue
                    try:
                        strike = float(row[strike_idx])
                        call_oi = int(row[call_oi_idx] or 0)
                        put_oi = int(row[put_oi_idx] or 0)
                        prev_oi_dict[strike] = {"call": call_oi, "put": put_oi}
                    except:
                        continue
            except ValueError:
                pass  # Headers not found yet

        # Fetch all SENSEX options for this expiry
        instruments = kite.instruments("BFO")
        sensex_opts = [
            i for i in instruments
            if i["name"] == "SENSEX" and i["expiry"].strftime("%Y-%m-%d") == expiry
        ]

        print(f"Found {len(sensex_opts)} contracts for {expiry}")

        option_chain = {}
        quotes = kite.quote([i["instrument_token"] for i in sensex_opts])

        for inst in sensex_opts:
            token = str(inst["instrument_token"])
            if token not in quotes:
                continue
            data = quotes[token]

            strike = inst["strike"]
            typ = inst["instrument_type"]  # CE or PE

            if strike not in option_chain:
                option_chain[strike] = {"call": {}, "put": {}}

            prev_oi = prev_oi_dict.get(strike, {"call": 0, "put": 0})
            base = {
                "ltp": data.get("last_price", 0),
                "oi": data.get("oi", 0),
                "vol": data.get("volume", 0),
                "chg_oi": 0
            }

            if typ == "CE":
                base["chg_oi"] = base["oi"] - prev_oi["call"]
                option_chain[strike]["call"] = base
            elif typ == "PE":
                base["chg_oi"] = base["oi"] - prev_oi["put"]
                option_chain[strike]["put"] = base

        # Prepare rows for Google Sheets
        header = [
            "Call LTP", "Call OI", "Call Chg OI", "Call Vol",
            "Strike", "Expiry",
            "Put LTP", "Put OI", "Put Chg OI", "Put Vol",
            "PCR (OI)", "Net Chg OI"
        ]

        rows = [header]

        for strike in sorted(option_chain.keys()):
            c = option_chain[strike]["call"]
            p = option_chain[strike]["put"]

            call_oi = c.get("oi", 0)
            put_oi = p.get("oi", 0)
            pcr = round(put_oi / call_oi, 2) if call_oi > 0 else 0
            net_chg = (c.get("chg_oi", 0) or 0) - (p.get("chg_oi", 0) or 0)

            row = [
                c.get("ltp", ""),
                call_oi,
                c.get("chg_oi", ""),
                c.get("vol", ""),
                strike,
                expiry,
                p.get("ltp", ""),
                put_oi,
                p.get("chg_oi", ""),
                p.get("vol", ""),
                pcr,
                net_chg
            ]
            rows.append(row)

        # Write to sheet
        sheet.clear()
        sheet.update("A1", rows)  # Fastest method
        print(f"Updated {len(rows)-1} strikes in '{sheet_name}' | Time: {now.strftime('%H:%M:%S')}")

    except Exception as e:
        print(f"Error processing {expiry}: {e}")
        raise

print("\nAll expiries updated successfully!")
