import time
import subprocess
from datetime import datetime
import pytz

IST = pytz.timezone("Asia/Kolkata")

def is_market_open():
    now = datetime.now(IST)
    return (
        now.weekday() < 5 and
        now.time() >= datetime.strptime("09:07", "%H:%M").time() and
        now.time() <= datetime.strptime("15:35", "%H:%M").time()
    )

while True:
    if not is_market_open():
        print("🛑 Market is closed. Continuous runner stopping.")
        break

    print("▶ Running fetch_data.py ...")
    subprocess.run(["python", "fetch_data.py"])

    print("⏳ Sleeping for 60 seconds...\n")
    time.sleep(60)
