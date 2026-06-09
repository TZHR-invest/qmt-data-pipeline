import sys, os, time
_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(_HERE))

from xtquant import xtdata

stock = '000001.SZ'

print(f"[{time.strftime('%H:%M:%S')}] Reading 1m data for {stock}...")
print(f"Data dir: {xtdata.get_data_dir()}")
t0 = time.time()

data = xtdata.get_local_data(
    stock_list=[stock],
    period='1m',
    start_time='',
    end_time='',
    count=-1,
    dividend_type='none',
    fill_data=True,
)

t1 = time.time()
print(f"[{time.strftime('%H:%M:%S')}] Read done in {t1-t0:.2f}s")

if data is None:
    print("No data returned!")
    sys.exit(1)

print(f"Keys in data dict: {list(data.keys())}")

for code, df in data.items():
    print(f"\n--- {code} ---")
    print(f"Shape: {df.shape}")
    print(f"Columns: {df.columns.tolist()}")
    print(f"dtypes:\n{df.dtypes}")
    if df.shape[0] > 0:
        print(f"\nFirst 3 rows:\n{df.head(3)}")
        print(f"\nLast 3 rows:\n{df.tail(3)}")
    else:
        print("EMPTY DATAFRAME!")
        sys.exit(1)

import datetime
times = df['time'].values
print(f"\nTime range: {times[0]} ~ {times[-1]}")
print(f"  -> {datetime.datetime.fromtimestamp(times[0]/1000)} ~ {datetime.datetime.fromtimestamp(times[-1]/1000)}")
print(f"Total bars: {len(times)}")

# Memory
mem_bytes = df.memory_usage(deep=True).sum()
print(f"\nDataFrame memory: {mem_bytes/1024/1024:.2f} MB")

# Save parquet
parquet_path = r'G:\qmt_projects\000001_1m.parquet'
t2 = time.time()
df.to_parquet(parquet_path, index=False, compression='snappy')
t3 = time.time()
print(f"Parquet saved to {parquet_path} in {t3-t2:.2f}s")

parquet_size = os.path.getsize(parquet_path)
print(f"Parquet file size: {parquet_size/1024/1024:.2f} MB ({parquet_size:,} bytes)")

# Check original data file sizes for 000001 only
print(f"\n--- Original data file sizes for {stock} ---")
data_dir = xtdata.get_data_dir()
total_orig = 0
for period_name, period_dir in [('tick', '0'), ('1m', '60'), ('5m', '300'), ('1d', '86400')]:
    fpath = os.path.join(data_dir, 'SZ', period_dir, '000001.DAT')
    if os.path.exists(fpath):
        sz = os.path.getsize(fpath)
        total_orig += sz
        print(f"  SZ/{period_dir}/000001.DAT ({period_name}): {sz/1024/1024:.2f} MB ({sz:,} bytes)")
    else:
        # check for dir (tick is a directory)
        dpath = os.path.join(data_dir, 'SZ', period_dir, '000001')
        if os.path.isdir(dpath):
            dir_size = sum(os.path.getsize(os.path.join(dpath, f)) for f in os.listdir(dpath))
            total_orig += dir_size
            print(f"  SZ/{period_dir}/000001/ (tick dir): {dir_size/1024/1024:.2f} MB ({dir_size:,} bytes)")

# Also check 86400 for extra files
for extra in ['000001_4002.DAT']:
    fpath = os.path.join(data_dir, 'SZ', '86400', extra)
    if os.path.exists(fpath):
        sz = os.path.getsize(fpath)
        total_orig += sz
        print(f"  SZ/86400/{extra}: {sz/1024/1024:.2f} MB ({sz:,} bytes)")

print(f"\nTotal original size for {stock} (all periods): {total_orig/1024/1024:.2f} MB ({total_orig:,} bytes)")
print(f"1m parquet size: {parquet_size/1024/1024:.2f} MB")
