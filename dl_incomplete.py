import sys, csv, os, json
_HERE = os.path.dirname(os.path.abspath(__file__))
_WORKSPACE = os.path.dirname(_HERE)
sys.path.insert(0, _WORKSPACE)
import xtquant.xtdata as xtdata
from tqdm import tqdm

xtdata.data_dir = "G:\\qmt\\userdata_mini\\datadir"

need = []
with open(os.path.join(_HERE, "stock_1m_count.csv"), encoding="utf-8") as f:
    for row in csv.DictReader(f):
        b = int(row["est_bars"])
        if 100000 <= b < 104210:
            need.append(row["stock_code"])

# 加载断点
cp_file = os.path.join(_HERE, "dl_checkpoint.json")
done = set()
if os.path.exists(cp_file):
    with open(cp_file) as f:
        done = set(json.load(f))

remaining = [s for s in need if s not in done]
print(f"总数: {len(need)}, 已完成: {len(done)}, 剩余: {len(remaining)}")

ok = len(done)
fail = 0
pbar = tqdm(total=len(need), initial=len(done), desc="逐只下载1m", unit="只")

for stock in remaining:
    for period, start, end in [("1m","20250101","20251231"), ("1m","20260101","20260608")]:
        try:
            r = xtdata.download_history_data(stock, period, start, end)
            if r is not None:
                fail += 1
        except Exception:
            fail += 1
    ok += 1
    done.add(stock)
    with open(cp_file, "w") as f:
        json.dump(list(done), f)
    pbar.update(1)
    pbar.set_postfix(ok=ok, fail=fail)

pbar.close()
print(f"\n完成! 成功={ok} 失败={fail}")
if os.path.exists(cp_file):
    os.remove(cp_file)
