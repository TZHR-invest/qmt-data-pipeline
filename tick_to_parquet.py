import sys, os, json, time, csv, random
_HERE = os.path.dirname(os.path.abspath(__file__))
_WORKSPACE = os.path.dirname(_HERE)
sys.path.insert(0, _WORKSPACE)
# 2026-09-15: mini 退役后 miniquote(58610) 消失，行情改走大QMT 桥。
# 必须插在 _WORKSPACE 之后：G:\qmt_projects\xtquant 是旧 SDK 副本(与 venv 内那份
# 逐字节相同)，会遮蔽桥的影子包；spawn 子进程会重跑本模块体，故对子进程同样生效。
sys.path.insert(0, r"C:\bridge-client")
sys.path.insert(0, r"C:\bridge-client\bridge\src")
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import xtquant.xtdata as xtdata

xtdata.data_dir = "D:\\qmt_data"
out_dir = r"D:\qmt_data_parquet\tick_parquet"
os.makedirs(out_dir, exist_ok=True)
cp_file = os.path.join(out_dir, "_checkpoint.json")

# 从stock_list.csv随机取100只
all_stocks = []
with open(os.path.join(_HERE, "stock_list.csv"), encoding="utf-8") as f:
    for row in csv.DictReader(f):
        all_stocks.append(row["stock_code"])

random.seed(42)
stocks = random.sample(all_stocks, 100)
print(f"随机抽取 100 只: {stocks[:5]}...")

# 加载断点
done = {}
if os.path.exists(cp_file):
    with open(cp_file) as f:
        done = json.load(f)

remaining = [s for s in stocks if s not in done]
print(f"已完成: {len(done)}, 剩余: {len(remaining)}")

t0 = time.time()
for i, code in enumerate(remaining):
    try:
        xtdata.download_history_data(code, "tick", "20260508", "20260608")
    except Exception:
        done[code] = "fail"
        continue

    try:
        # 2026-09-15: 桥的终端进程内没有 numpy，旧 get_market_data() 会抛
        # RpcServerRepliedError(ModuleNotFoundError: numpy)；改用 get_market_data_ex。
        # 注意：桥下 tick 只有 18 列（比 mini 少 tickvol/pe；pe 在 mini 时代恒 0）。
        raw = xtdata.get_market_data_ex(field_list=[], stock_list=[code], period="tick", start_time="", end_time="", count=-1)
        rows = raw.get(code) if raw else None
    except Exception:
        done[code] = "fail"
        continue

    if rows is None or len(rows) == 0:
        done[code] = "empty"
        continue

    df = pd.DataFrame(rows)
    table = pa.Table.from_pandas(df)
    out_file = os.path.join(out_dir, f"{code.replace('.', '_')}.parquet")
    pq.write_table(table, out_file, compression="zstd", compression_level=12)

    done[code] = str(len(rows))

    if (i + 1) % 10 == 0 or i == len(remaining) - 1:
        with open(cp_file, "w") as f:
            json.dump(done, f)
        elapsed = time.time() - t0
        speed = (i + 1) / max(elapsed, 0.1)
        eta = (len(remaining) - i - 1) / max(speed, 0.1)
        print(f"  [{i+1}/{len(remaining)}] {code}: {len(rows)}条, {elapsed:.0f}s, ETA {eta:.0f}s")

with open(cp_file, "w") as f:
    json.dump(done, f)

ok = sum(1 for v in done.values() if v not in ("fail", "empty"))
fail = sum(1 for v in done.values() if v == "fail")
empty = sum(1 for v in done.values() if v == "empty")
print(f"\n完成! 成功={ok}, 失败={fail}, 无数据={empty}")
print(f"总耗时: {time.time()-t0:.0f}秒")
