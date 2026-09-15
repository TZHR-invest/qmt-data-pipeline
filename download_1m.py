import sys, os, time
_HERE = os.path.dirname(os.path.abspath(__file__))
_WORKSPACE = os.path.dirname(_HERE)
sys.path.insert(0, _WORKSPACE)
# 2026-09-15: mini 退役后 miniquote(58610) 消失，行情改走大QMT 桥。
# 必须插在 _WORKSPACE 之后：G:\qmt_projects\xtquant 是旧 SDK 副本(与 venv 内那份
# 逐字节相同)，会遮蔽桥的影子包；spawn 子进程会重跑本模块体，故对子进程同样生效。
sys.path.insert(0, r"C:\bridge-client")
sys.path.insert(0, r"C:\bridge-client\bridge\src")
import xtquant.xtdata as xtdata

xtdata.data_dir = "D:\\qmt_data"

for period in ["1m", "5m"]:
    print(f"下载 {period} 20230601-20260608...", flush=True)
    t0 = time.time()
    r = xtdata.download_history_data("000001.SZ", period, "20230601", "20260608")
    t = time.time() - t0
    print(f"{period} 完成, 耗时{t:.0f}秒, return={r}", flush=True)

print("全部完成")
