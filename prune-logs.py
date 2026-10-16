#!/usr/bin/env python3
# coding: utf-8
"""删除 logs/ 下超过 N 天（默认 60）的日志文件；--dry-run 只列不删。

为什么放这里：归档目录是日常运行的自然收口点（每天 16:00 跑一次），
把保留期挂在这里就不需要额外的计划任务。失败绝不影响归档退出码。
"""
import argparse, os, time

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", default=r"G:\qmt_projects\qmt-data-pipeline\logs")
    ap.add_argument("--days", type=int, default=60)
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    if not os.path.isdir(a.dir):
        print("logs 目录不存在: %s" % a.dir); return 0
    cutoff = time.time() - a.days * 86400
    gone = kept = 0; freed = 0
    for name in os.listdir(a.dir):
        p = os.path.join(a.dir, name)
        if not os.path.isfile(p):
            continue
        try:
            st = os.stat(p)
        except Exception:
            continue
        if st.st_mtime < cutoff:
            gone += 1; freed += st.st_size
            if a.dry_run:
                print("  将删 %s (%.1f KB)" % (name, st.st_size / 1024.0))
            else:
                try:
                    os.remove(p)
                except Exception as e:
                    print("  删除失败 %s: %s" % (name, e)); gone -= 1
        else:
            kept += 1
    print("日志保留期 %d 天：%s %d 个（释放 %.1f MB），保留 %d 个"
          % (a.days, "将删除" if a.dry_run else "已删除", gone, freed / 1048576.0, kept))
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
