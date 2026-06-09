import sys, os, time
from datetime import datetime, timedelta
_HERE = os.path.dirname(os.path.abspath(__file__))
_WORKSPACE = os.path.dirname(_HERE)
sys.path.insert(0, _WORKSPACE)
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import xtquant.xtdata as xtdata
from tqdm import tqdm

xtdata.data_dir = "D:\\qmt_data"
out_dir_base = r"D:\qmt_data_parquet\tick_parquet"
os.makedirs(out_dir_base, exist_ok=True)

# 股票池：全部从 xtdata 实时拉取（SH+SZ+BJ，含新股）
sh_sz = xtdata.get_stock_list_in_sector("沪深A股") or []
bj = xtdata.get_stock_list_in_sector("BJ") or []
stocks = sorted(set(sh_sz) | set(bj))
print(f"股票池: {len(stocks)} 只（SH+SZ={len(sh_sz)}, BJ={len(bj)}）")

# 盘后运行：增量下载只读当天，不需扫 90 天
today = datetime.now().strftime("%Y%m%d")
start_date = today
end = today

ok = fail = skip = 0
t0 = time.time()
pbar = tqdm(stocks, desc="Updating tick", unit="stock")

for code in pbar:
    code_dir = os.path.join(out_dir_base, code.replace(".", "_"))
    day_file = os.path.join(code_dir, f"{today[:4]}-{today[4:6]}-{today[6:]}.parquet")

    # 如果今日文件已存在且不为空，跳过
    if os.path.exists(day_file) and os.path.getsize(day_file) > 0:
        skip += 1
        continue

    # 第1步：增量下载（xtdata 自己判断缺失了哪些，只补缺失的）
    try:
        xtdata.download_history_data(code, "tick", start_date, end, incrementally=True)
    except Exception as e:
        pbar.write(f"{code}: download failed - {e}")
        fail += 1
        continue

    # 第2步：只读今天的数据（不再用 count=-1 读全部）
    try:
        raw = xtdata.get_market_data(
            field_list=[], stock_list=[code], period="tick",
            start_time=today, end_time=today, count=-1,
        )
        arr = raw.get(code) if raw else None
    except Exception as e:
        pbar.write(f"{code}: get_data failed - {e}")
        fail += 1
        continue

    if arr is None or len(arr) == 0:
        skip += 1
        continue

    df_new = pd.DataFrame(arr).sort_values("time").reset_index(drop=True)
    os.makedirs(code_dir, exist_ok=True)

    # 第3步：追加写今日 parquet（文件已存在则合并去重，但只针对今日文件）
    if os.path.exists(day_file):
        df_old = pd.read_parquet(day_file)
        df = pd.concat([df_old, df_new], ignore_index=True)
        df = df.drop_duplicates(subset=["time"], keep="last").sort_values("time").reset_index(drop=True)
    else:
        df = df_new

    pq.write_table(
        pa.Table.from_pandas(df, preserve_index=False),
        day_file,
        compression="zstd",
        compression_level=6,  # 从 12 降到 6，写入快 3-5 倍
    )
    ok += 1

pbar.close()
tqdm.write(f"\nDone! OK={ok} Skip={skip} Fail={fail} Elapsed={time.time()-t0:.0f}s")
tqdm.write(f"Output: {out_dir_base}/{code.replace('.', '_')}/{today[:4]}-{today[4:6]}-{today[6:]}.parquet")
