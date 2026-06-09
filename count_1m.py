import csv, os
from pathlib import Path
from collections import Counter
_HERE = os.path.dirname(os.path.abspath(__file__))
_WORKSPACE = os.path.dirname(_HERE)

datadir = Path("D:\\qmt_data")
out = os.path.join(_HERE, "stock_1m_count.csv")

stocks = []
with open(os.path.join(_HERE, "stock_list.csv"), encoding="utf-8") as f:
    for row in csv.DictReader(f):
        stocks.append((row["stock_code"], row["market"]))

market_dir = {"SH": "SH", "SZ": "SZ", "BJ": "BJ"}
results = []
for code, market in stocks:
    d = datadir / market_dir[market] / "60"
    num = code.split(".")[0]
    f = d / f"{num}.DAT"
    size = f.stat().st_size if f.exists() else 0
    est_bars = size // 36 if size else 0
    results.append((code, market, "Y" if size > 0 else "", size, est_bars))

with open(out, "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["stock_code", "market", "has_1m", "file_size", "est_bars"])
    w.writerows(results)

has = sum(1 for _, _, h, _, _ in results if h == "Y")
no = sum(1 for _, _, h, _, _ in results if h != "Y")
buckets = Counter()
for _, _, _, _, b in results:
    if b == 0: buckets["0"] += 1
    elif b < 50000: buckets["1-5万"] += 1
    elif b < 100000: buckets["5-10万"] += 1
    elif b < 104210: buckets["10万-104210"] += 1
    else: buckets[">=104210"] += 1

print(f"有1m: {has}, 无: {no}")
for k in sorted(buckets.keys()):
    print(f"  {k}: {buckets[k]}")
