import sys, os, time, csv
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

today = "20260608"
stock_file = os.path.join(_HERE, "stock_list.csv")

stocks = []
with open(stock_file, encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        stocks.append(row["stock_code"])

print(f"共 {len(stocks)} 只股票，下载今日({today}) 1m 数据", flush=True)

ok = fail = 0
t0 = time.time()

for i, code in enumerate(stocks, 1):
    try:
        r = xtdata.download_history_data(code, "1m", today, today)
        if r == 0:
            ok += 1
        else:
            fail += 1
    except Exception as e:
        fail += 1

    if i % 500 == 0 or i == len(stocks):
        elapsed = time.time() - t0
        print(f"[{i}/{len(stocks)}] OK={ok} Fail={fail} 耗时{elapsed:.0f}s", flush=True)

t = time.time() - t0
print(f"\n完成! OK={ok} Fail={fail} 总耗时{t:.0f}秒", flush=True)
