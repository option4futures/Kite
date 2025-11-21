import time
from datetime import datetime
import pytz
import subprocess

IST = pytz.timezone("Asia/Kolkata")

def is_market_open():
    now = datetime.now(IST)
    start = now.replace(hour=9, minute=10, second=0, microsecond=0)
    end = now.replace(hour=15, minute=35, second=0, microsecond=0)
    return start <= now <= end

while True:
    if not is_market_open():
        print("🛑 Market closed. Stopping program.")
        break

    print("▶ Running fetch_data.py")
    subprocess.run(["python", "fetch_data.py"])

    print("⏳ Waiting 60 seconds...")
    time.sleep(60)
