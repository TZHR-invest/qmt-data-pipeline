"""
一次性任务：将旧的 flat parquet (tick_parquet/000001_SZ.parquet)
拆分为按日分片 (tick_parquet/000001_SZ/2026-03-09.parquet ...)

用法： python migrate_tick_to_daily.py [--dry-run]
      --dry-run 只统计不实际拆分
"""
import sys, os, argparse
_HERE = os.path.dirname(os.path.abspath(__file__))
_WORKSPACE = os.path.dirname(_HERE)
sys.path.insert(0, _WORKSPACE)
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm

out_dir = os.path.join(_WORKSPACE, "tick_parquet")

def migrate_file(code, flat_file, dry_run=False):
    """将单个 flat parquet 拆分为按日分片"""
    code_dir = os.path.join(out_dir, code.replace(".", "_"))
    os.makedirs(code_dir, exist_ok=True)

    # 读旧文件
    df = pd.read_parquet(flat_file)

    # 如果没有 time 列，跳过
    if "time" not in df.columns:
        return ("skip", code, "no time column")

    # 按日期分组
    df["_date"] = pd.to_datetime(df["time"], unit="ms").dt.strftime("%Y-%m-%d")
    dates = sorted(df["_date"].unique())

    written = 0
    for date_str in dates:
        day_df = df[df["_date"] == date_str].drop(columns=["_date"]).sort_values("time").reset_index(drop=True)
        day_file = os.path.join(code_dir, f"{date_str}.parquet")

        if os.path.exists(day_file):
            # 新旧合并去重
            df_old = pd.read_parquet(day_file)
            merged = pd.concat([df_old, day_df], ignore_index=True)
            merged = merged.drop_duplicates(subset=["time"], keep="last").sort_values("time").reset_index(drop=True)
            if not dry_run:
                pq.write_table(pa.Table.from_pandas(merged, preserve_index=False), day_file,
                               compression="zstd", compression_level=6)
        else:
            if not dry_run:
                pq.write_table(pa.Table.from_pandas(day_df, preserve_index=False), day_file,
                               compression="zstd", compression_level=6)
        written += 1

    return ("ok", code, f"{len(df)} rows -> {written} daily files ({dates[0]} ~ {dates[-1]})")


def main():
    parser = argparse.ArgumentParser(description="迁移旧 flat parquet 到按日分片")
    parser.add_argument("--dry-run", action="store_true", help="只统计不写入")
    parser.add_argument("--limit", type=int, default=0, help="限制处理数量（用于测试）")
    args = parser.parse_args()

    # 扫描需要迁移的 flat parquet（排除已有子目录的）
    flat_files = []
    for f in sorted(os.listdir(out_dir)):
        if not f.endswith(".parquet"):
            continue
        code_key = f.replace(".parquet", "")
        code_dir = os.path.join(out_dir, code_key)
        code = code_key.replace("_", ".")
        # 检查是否已有子目录
        if os.path.isdir(code_dir):
            continue  # 已迁移过
        flat_files.append((code, os.path.join(out_dir, f)))

    print(f"找到 {len(flat_files)} 个待迁移的 flat parquet 文件")
    if args.limit:
        flat_files = flat_files[:args.limit]
        print(f"限制处理 {args.limit} 个")

    if args.dry_run:
        print("[DRY RUN] 仅统计，不实际写入")

    ok = skip = 0
    t0 = __import__("time").time()

    for code, fpath in tqdm(flat_files, desc="Migrating", unit="file"):
        status, code2, msg = migrate_file(code, fpath, dry_run=args.dry_run)
        if status == "ok":
            ok += 1
        else:
            skip += 1

    elapsed = __import__("time").time() - t0
    print(f"\n完成! OK={ok} Skip={skip} 耗时={elapsed:.0f}s")

    if not args.dry_run and ok > 0:
        print("\n迁移完成。旧的 flat parquet 文件还在，确认无误后可手动删除：")
        print(f"  cd {out_dir} && del *.parquet")

if __name__ == "__main__":
    main()
