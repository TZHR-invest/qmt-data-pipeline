import sys, os, time
_HERE = os.path.dirname(os.path.abspath(__file__))
_WORKSPACE = os.path.dirname(_HERE)
sys.path.insert(0, _WORKSPACE)
import xtquant.xtdata as xtdata

xtdata.data_dir = "G:\\qmt\\userdata_mini\\datadir"

for period in ["1m", "5m"]:
    print(f"下载 {period} 20230601-20260608...", flush=True)
    t0 = time.time()
    r = xtdata.download_history_data("000001.SZ", period, "20230601", "20260608")
    t = time.time() - t0
    print(f"{period} 完成, 耗时{t:.0f}秒, return={r}", flush=True)

print("全部完成")
