"""
一次性任务：将旧的 flat parquet 拆分为按日分片
输出: D:\qmt_data_parquet\tick_parquet\{code}\{date}.parquet

用法： python migrate_tick_to_daily.py [--dry-run] [--workers 8] [--limit N]
      --dry-run 只统计不实际拆分
      --workers  并行进程数 (default: CPU 核心数)
      --limit    限制处理数量（用于测试）
"""
import sys, os, argparse
from multiprocessing import Pool
_HERE = os.path.dirname(os.path.abspath(__file__))
_WORKSPACE = os.path.dirname(_HERE)
sys.path.insert(0, _WORKSPACE)
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

out_dir = r"D:\qmt_data_parquet\tick_parquet"


def migrate_file(code, flat_file, dry_run=False):
    """将单个 flat parquet 拆分为按日分片（数据已按 time 有序，无需 sort_values）"""
    code_dir = os.path.join(out_dir, code.replace(".", "_"))
    os.makedirs(code_dir, exist_ok=True)

    df = pd.read_parquet(flat_file)

    if "time" not in df.columns:
        return ("skip", code, "no time column")

    # 提取日期（数据已全局有序，下游无需排序）
    df["_date"] = pd.to_datetime(df["time"], unit="ms").dt.strftime("%Y-%m-%d")

    # 缓存已存在的日文件列表，避免每日期一次 os.path.exists
    existing = set(os.listdir(code_dir)) if os.path.isdir(code_dir) else set()

    written = 0
    date_strs = []
    for date_str, day_df in df.groupby("_date", sort=False):
        date_strs.append(date_str)
        day_df = day_df.drop(columns=["_date"]).reset_index(drop=True)
        day_file = os.path.join(code_dir, f"{date_str}.parquet")

        if f"{date_str}.parquet" in existing:
            # 合并去重（新旧数据各自有序，合并后只需去重，无需排序）
            df_old = pd.read_parquet(day_file)
            merged = pd.concat([df_old, day_df], ignore_index=True)
            merged = merged.drop_duplicates(subset=["time"], keep="last").reset_index(drop=True)
            if not dry_run:
                pq.write_table(pa.Table.from_pandas(merged, preserve_index=False), day_file,
                               compression="zstd", compression_level=6)
        else:
            if not dry_run:
                pq.write_table(pa.Table.from_pandas(day_df, preserve_index=False), day_file,
                               compression="zstd", compression_level=6)
        written += 1

    return ("ok", code, f"{len(df)} rows -> {written} daily files ({min(date_strs)} ~ {max(date_strs)})")


def _worker(args):
    """Pool.imap_unordered 的包装（必须是模块级函数，Windows spawn 兼容）"""
    return migrate_file(*args)


def main():
    parser = argparse.ArgumentParser(description="迁移旧 flat parquet 到按日分片")
    parser.add_argument("--dry-run", action="store_true", help="只统计不写入")
    parser.add_argument("--limit", type=int, default=0, help="限制处理数量（用于测试）")
    parser.add_argument("--workers", type=int, default=0,
                        help="并行进程数 (default: CPU 核心数)")
    args = parser.parse_args()

    # 扫描需要迁移的 flat parquet（排除已有子目录的）
    flat_files = []
    for f in sorted(os.listdir(out_dir)):
        if not f.endswith(".parquet"):
            continue
        code_key = f.replace(".parquet", "")
        code_dir = os.path.join(out_dir, code_key)
        code = code_key.replace("_", ".")
        if os.path.isdir(code_dir) and any(f.endswith(".parquet") for f in os.listdir(code_dir)):
            continue  # 已有日文件，跳过
        flat_files.append((code, os.path.join(out_dir, f)))

    print(f"找到 {len(flat_files)} 个待迁移的 flat parquet 文件")
    if args.limit:
        flat_files = flat_files[:args.limit]
        print(f"限制处理 {args.limit} 个")

    if args.dry_run:
        print("[DRY RUN] 仅统计，不实际写入")

    workers = args.workers or os.cpu_count() or 4
    print(f"使用 {workers} 个 worker 并行处理")

    ok = skip = 0
    t0 = __import__("time").time()

    from tqdm import tqdm

    if workers > 1:
        # 多进程模式 —— 每个 worker 处理不同的 stock，天然无竞态
        with Pool(processes=workers) as pool:
            for status, code, msg in tqdm(
                pool.imap_unordered(_worker, ((c, f, args.dry_run) for c, f in flat_files)),
                total=len(flat_files), desc="Migrating", unit="file"
            ):
                if status == "ok":
                    ok += 1
                else:
                    skip += 1
                if not args.dry_run:
                    tqdm.write(f"  [{code}] {msg}")
    else:
        # 单进程模式（调试用）
        for code, fpath in tqdm(flat_files, desc="Migrating", unit="file"):
            status, code2, msg = migrate_file(code, fpath, dry_run=args.dry_run)
            if status == "ok":
                ok += 1
            else:
                skip += 1
            if not args.dry_run:
                tqdm.write(f"  [{code2}] {msg}")

    elapsed = __import__("time").time() - t0
    print(f"\n完成! OK={ok} Skip={skip} 耗时={elapsed:.0f}s ({elapsed/60:.1f}min)")

    if not args.dry_run and ok > 0:
        print("\n迁移完成。旧的 flat parquet 文件还在，确认无误后可手动删除：")
        print(f"  cd {out_dir} && del *.parquet")


if __name__ == "__main__":
    main()
