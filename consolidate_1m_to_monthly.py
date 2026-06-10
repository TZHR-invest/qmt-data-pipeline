"""
1m 日级 parquet → 月度 parquet 合并

将 D:\qmt_data_parquet\kline_1m\{code}\ 下的日级文件 (YYYY-MM-DD.parquet)
合并为月度文件 (YYYY-MM.parquet)，使用高压缩比。

用法： python consolidate_1m_to_monthly.py
      python consolidate_1m_to_monthly.py --month 2026-06
      python consolidate_1m_to_monthly.py --delete  # 合并后删除日级文件
"""
import sys, os, argparse, glob
from datetime import datetime

_HERE = os.path.dirname(os.path.abspath(__file__))
_WORKSPACE = os.path.dirname(_HERE)
sys.path.insert(0, _WORKSPACE)
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

out_dir_base = r"D:\qmt_data_parquet\kline_1m"


def consolidate_stock(code, month, delete_daily):
    """将单只股票指定月份的日级 parquet 合并为月度文件"""
    code_dir = os.path.join(out_dir_base, code.replace(".", "_"))
    month_file = os.path.join(code_dir, f"{month}.parquet")

    if not os.path.isdir(code_dir):
        return ("skip", code, "dir not found")

    # 扫描当月日级文件
    daily_glob = os.path.join(code_dir, f"{month}-??.parquet")
    daily_files = sorted(glob.glob(daily_glob))

    try:
        chunks = []

        # 如果已有月度 parquet，读入作为基础（兼容旧数据 + 首次切换）
        if os.path.exists(month_file) and os.path.getsize(month_file) > 0:
            try:
                df_month = pd.read_parquet(month_file)
                if not df_month.empty:
                    chunks.append(df_month)
            except Exception:
                pass

        # 追加当月日级文件
        for f in daily_files:
            df = pd.read_parquet(f)
            if not df.empty:
                chunks.append(df)

        if not chunks:
            return ("skip", code, "no data")

        merged = pd.concat(chunks, ignore_index=True)
        merged = merged.drop_duplicates(subset=["time"]).sort_values("time").reset_index(drop=True)

        # 写入月度 parquet（高压缩）
        os.makedirs(code_dir, exist_ok=True)
        pq.write_table(
            pa.Table.from_pandas(merged, preserve_index=False),
            month_file,
            compression="zstd",
            compression_level=6,
        )
    except Exception as e:
        return ("fail", code, str(e))

    # 可选删除日级文件
    if delete_daily:
        for f in daily_files:
            try:
                os.remove(f)
            except Exception:
                pass

    return ("ok", code, f"{len(daily_files)} days, {len(chunks)} chunks, {len(merged)} rows")


def main():
    parser = argparse.ArgumentParser(description="1m 日级 parquet → 月度合并")
    parser.add_argument("--month", type=str, default="",
                        help="要合并的月份 (YYYY-MM)，默认当前月")
    parser.add_argument("--delete", action="store_true",
                        help="合并后删除日级文件")
    parser.add_argument("--limit", type=int, default=0,
                        help="限制处理股票数量（测试用）")
    args = parser.parse_args()

    month = args.month or datetime.now().strftime("%Y-%m")

    # 扫描所有股票目录
    if not os.path.isdir(out_dir_base):
        print(f"错误：输出目录不存在 {out_dir_base}")
        sys.exit(1)

    codes = sorted(os.listdir(out_dir_base))
    if args.limit:
        codes = codes[:args.limit]
        print(f"限制处理 {args.limit} 只")

    print(f"合并 {month} 月度 parquet，共 {len(codes)} 只股票")
    if args.delete:
        print("（合并后将删除日级文件）")

    t0 = datetime.now()
    ok = fail = skip = 0

    for i, code_dir in enumerate(codes, 1):
        # code_dir 是 "000001_SZ" 格式
        status, code, msg = consolidate_stock(code_dir, month, args.delete)
        if status == "ok":
            ok += 1
        elif status == "fail":
            fail += 1
            print(f"  [{i}/{len(codes)}] ✗ {code}: {msg}")
        else:
            skip += 1

        if i % 1000 == 0 or status == "ok":
            elapsed = (datetime.now() - t0).total_seconds()
            print(f"  [{i}/{len(codes)}] {status.upper():4s} {code}  {msg}  ({elapsed:.0f}s)")

    elapsed = (datetime.now() - t0).total_seconds()
    print(f"\n完成! OK={ok} Skip={skip} Fail={fail} 耗时={elapsed:.0f}s ({elapsed/60:.1f}min)")


if __name__ == "__main__":
    main()
