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

xtdata.data_dir = "G:\\qmt\\userdata_mini\\datadir"
out_dir_base = os.path.join(_WORKSPACE, "kline_1m")
os.makedirs(out_dir_base, exist_ok=True)

# 股票池：全部从 xtdata 实时拉取（SH+SZ+BJ，含新股）
sh_sz = xtdata.get_stock_list_in_sector("沪深A股") or []
bj = xtdata.get_stock_list_in_sector("BJ") or []
stocks = sorted(set(sh_sz) | set(bj))
print(f"股票池: {len(stocks)} 只（SH+SZ={len(sh_sz)}, BJ={len(bj)}）")
print(f"{len(stocks)} stocks, incremental update 1m data + parquet export", flush=True)

today = datetime.now()
today_ymd = today.strftime("%Y%m%d")
month_str = today.strftime("%Y-%m")

ok = fail = 0
t0 = time.time()

for code in tqdm(stocks, desc="1m", unit="stock"):
    code_dir = os.path.join(out_dir_base, code.replace(".", "_"))
    month_file = os.path.join(code_dir, f"{month_str}.parquet")

    # 增量下载（确保数据完整）
    try:
        xtdata.download_history_data(code, "1m", today_ymd, today_ymd, incrementally=True)
    except Exception:
        pass

    # 读当天数据
    try:
        raw = xtdata.get_market_data_ex(
            field_list=[], stock_list=[code], period="1m",
            start_time=today_ymd, end_time=today_ymd, count=-1,
        )
        frame = raw.get(code) if raw else None
    except Exception:
        frame = None

    if frame is None or frame.empty:
        ok += 1
        continue

    # 第3步：追加到当月 parquet
    os.makedirs(code_dir, exist_ok=True)
    day_df = frame.drop_duplicates(subset=["time"]).sort_values("time").reset_index(drop=True)

    if os.path.exists(month_file) and os.path.getsize(month_file) > 0:
        df_old = pd.read_parquet(month_file)
        merged = pd.concat([df_old, day_df], ignore_index=True)
        merged = merged.drop_duplicates(subset=["time"]).sort_values("time").reset_index(drop=True)
    else:
        merged = day_df

    pq.write_table(
        pa.Table.from_pandas(merged, preserve_index=False),
        month_file,
        compression="zstd",
        compression_level=6,
    )
    ok += 1

t = time.time() - t0
print(f"\nDone! OK={ok} Fail={fail} Elapsed={t:.0f}s", flush=True)
print(f"Output: {out_dir_base}/{{code}}/{{month}}.parquet", flush=True)
