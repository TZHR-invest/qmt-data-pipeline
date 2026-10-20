# -*- coding: utf-8 -*-
"""补 1m 历史缺口（2026-06~09 那次事故留下的运维工具，2026-10-20 落地）。

用法（trade-pc，Windows python）：
    set BIGQMT_ACCOUNT_ID=666810082889
    set BIGQMT_FORMULA_ENABLED=0
    G:\\qmt_projects\\quant-qmt-proxy\\.venv\\Scripts\\python.exe repair_1m_gaps.py

它做什么
--------
1. 扫 `D:\\qmt_data_parquet\\kline_1m\\<code>\\<YYYY-MM>.parquet`，找出"当月行数明显少于
   交易日×241"的票（用 tick 归档的**存在性**当参考天数，新上市/退市不会误判）；
2. 对缺的日子**批量下载**（见下面"为什么必须批量"），再逐只读 + 合并进月文件；
3. 安全：每个被改文件先备份到 `_backup_gapfill5_<ts>\\`；只追加缺失日期；已有日期行数
   必须逐一不变；先写 `.tmp` 再 `os.replace`；`volume` 全零视为无数据、不写。

为什么必须批量（这是这个工具存在的唯一理由）
------------------------------------------
`download_history_data` 只是 `download_history_data2([code])` 的壳，对**没有数据的票**会干等
`data_wait_seconds=60s` 才返回 ⇒ 逐只补"无源票"每票白烧 30~60 s（实测 46 个任务 23 分钟、
0 成功）。而**模块级** `xtquant.xtdata.download_history_data2` 不接受 `chunk_size` /
`data_wait_seconds`（shim 与 compat 类签名不一致，传了直接 TypeError）⇒ 必须取 compat 单例：
`CLIENT = xtquant.xtdata._compat.xtdata`。用批量 + `data_wait_seconds=5`：123 个任务 107 秒。

⚠️ 三条纪律
-----------
1. **避开 16:00–17:50（归档窗口）与 20:50（客户端定时自重启）**；运行中每月检查一次
   `get_full_tick(['SH','SZ'])` 是否仍 ~5226，掉线就等恢复、连续不恢复则中止。
2. 单进程串行读（桥客户端是进程内单例，多线程并发会踩 `not authenticated`）。
3. 客户端对 4 个月前的**冷日期**多半已经没有数据（2026-10-20 实测：download 返回 finished 后
   连读 48 s 仍是 0 行）⇒ 这个工具会把它们计为"无源"并跳过，**这是正常结果，不是 bug**。
"""
import sys
for _st in (sys.stdout, sys.stderr):
    try:
        _st.reconfigure(encoding='utf-8', errors='replace')
    except Exception:
        pass
sys.path.insert(0, r'C:\bridge-client')
sys.path.insert(0, r'C:\bridge-client\bridge\src')

import collections, datetime as dt, os, shutil, statistics, time
import pandas as pd
import chinese_calendar as cc
import xtquant.xtdata as xtdata

BASE = r'D:\qmt_data_parquet'
ONE = os.path.join(BASE, 'kline_1m')
TICK = os.path.join(BASE, 'tick_parquet')
MONTHS = ['2026-06', '2026-07', '2026-08', '2026-09']
STAMP = time.strftime('%Y%m%d-%H%M%S')
BACKUP = os.path.join(BASE, '_backup_gapfill5_' + STAMP)
LOGPATH = os.path.join(r'C:\Users\wbaif', 'repair_gaps5_%s.log' % STAMP)
CLIENT = xtdata._compat.xtdata          # 兼容层单例（带完整 kwargs）


def trading(ym):
    y, mo = map(int, ym.split('-'))
    out, d = [], dt.date(y, mo, 1)
    while d.month == mo:
        if d.weekday() < 5 and not cc.is_holiday(d):
            out.append(d.strftime('%Y-%m-%d'))
        d += dt.timedelta(days=1)
    return out


def pump_ok():
    try:
        n = len(xtdata.get_full_tick(['SH', 'SZ']) or {})
    except Exception:
        n = -1
    return n


def wait_pump(log, need=1000, tries=12):
    for i in range(tries):
        n = pump_ok()
        if n >= need:
            return True
        log('    pump=%s，等 10s 再试（%d/%d）' % (n, i + 1, tries))
        time.sleep(10)
    return False


def main():
    logf = open(LOGPATH, 'w', encoding='utf-8')

    def log(*a):
        m = ' '.join(str(x) for x in a)
        print(m); sys.stdout.flush(); logf.write(m + '\n'); logf.flush()

    t0 = time.time()
    os.makedirs(BACKUP, exist_ok=True)
    tick = collections.defaultdict(set)
    for c in os.listdir(TICK):
        p = os.path.join(TICK, c)
        if not os.path.isdir(p):
            continue
        with os.scandir(p) as it:
            for e in it:
                if e.name.endswith('.parquet') and len(e.name) == 18:
                    tick[c].add(e.name[:-8])
    groups = {}
    for ym in MONTHS:
        sizes = {}
        for c in os.listdir(ONE):
            p = os.path.join(ONE, c, ym + '.parquet')
            if os.path.exists(p):
                sizes[c] = os.path.getsize(p)
        med = statistics.median(sizes.values()) if sizes else 0
        tdays = trading(ym)
        tasks = []
        for c, s in sizes.items():
            if s >= med * 0.9 or c not in tick:
                continue
            ref = [d for d in tdays if d in tick[c]]
            if not ref:
                continue
            p = os.path.join(ONE, c, ym + '.parquet')
            try:
                df = pd.read_parquet(p, columns=['time'])
            except Exception:
                continue
            have = set(pd.to_datetime(df['time'], unit='ms').dt.strftime('%Y-%m-%d'))
            miss = [d for d in ref if d not in have]
            if miss:
                tasks.append((c, miss))
        groups[ym] = tasks
        log('[G5] %s 待补 code=%d（%.0fs）' % (ym, len(tasks), time.time() - t0))

    filled = skipped = checkfail = 0
    days_added = 0
    for ym, tasks in groups.items():
        if not tasks:
            continue
        codes = [c for c, _ in tasks]
        d0 = min(min(m) for _, m in tasks).replace('-', '')
        d1 = max(max(m) for _, m in tasks).replace('-', '')
        dotted = [c.replace('_', '.') for c in codes]
        t1 = time.time()
        try:
            ret = CLIENT.download_history_data2(dotted, '1m', start_time=d0, end_time=d1,
                                                incrementally=True, chunk_size=100,
                                                download_timeout_seconds=150.0,
                                                data_wait_seconds=5.0)
            log('[G5] %s 批量下载 %d 只 %s..%s -> %s  %.1fs'
                % (ym, len(dotted), d0, d1, str(ret)[:80], time.time() - t1))
        except Exception as exc:
            log('[G5] %s 批量下载异常 %r（继续逐只读）' % (ym, exc))
        for i, (c, miss) in enumerate(tasks, 1):
            dotted1 = c.replace('_', '.')
            try:
                raw = xtdata.get_market_data_ex(field_list=[], stock_list=[dotted1], period='1m',
                                                start_time=miss[0].replace('-', ''),
                                                end_time=miss[-1].replace('-', ''),
                                                count=-1, fill_data=False)
                f = (raw or {}).get(dotted1)
            except Exception as exc:
                log('    ERR %s %s %r' % (c, ym, str(exc)[:100]))
                skipped += 1
                continue
            if f is None or len(f) == 0:
                skipped += 1
                continue
            f = f.copy()
            f['__d'] = pd.to_datetime(f['time'], unit='ms').dt.strftime('%Y-%m-%d')
            add = f[f['__d'].isin(miss)].drop(columns=['__d'])
            if len(add) == 0 or int(add['volume'].sum()) == 0:
                skipped += 1
                continue
            p = os.path.join(ONE, c, ym + '.parquet')
            old = pd.read_parquet(p)
            for col in old.columns:
                if col in add.columns:
                    add[col] = add[col].astype(old[col].dtype)
            add = add[list(old.columns)]
            newt = set(add['time']) - set(old['time'])
            merged = (pd.concat([old, add], ignore_index=True)
                        .drop_duplicates('time').sort_values('time').reset_index(drop=True))
            o = pd.to_datetime(old['time'], unit='ms').dt.strftime('%Y-%m-%d').value_counts().to_dict()
            m = pd.to_datetime(merged['time'], unit='ms').dt.strftime('%Y-%m-%d').value_counts().to_dict()
            if len(merged) != len(old) + len(newt) or any(m.get(d) != o[d] for d in o):
                checkfail += 1
                log('    CHECKFAIL %s %s' % (c, ym))
                continue
            bdir = os.path.join(BACKUP, ym, c)
            os.makedirs(bdir, exist_ok=True)
            shutil.copy2(p, os.path.join(bdir, ym + '.parquet'))
            tmp = p + '.tmp'
            merged.to_parquet(tmp, index=False)
            os.replace(tmp, p)
            filled += 1
            days_added += len(newt)
            log('    [%d/%d] filled %s %s +%d 天' % (i, len(tasks), c, ym, len(newt)))
        log('[G5] %s 段完成：累计 filled=%d skipped=%d checkfail=%d（%.0fs）pump=%s'
            % (ym, filled, skipped, checkfail, time.time() - t0, pump_ok()))
        if not wait_pump(log):
            log('[G5] ⚠ pump 连续掉线，中止（已补的保留）')
            break
    log('[G5] 结束：补齐 %d 个文件 / %d 个 code-day；无源 %d；校验失败 %d；%.0fs'
        % (filled, days_added, skipped, checkfail, time.time() - t0))
    logf.close()


if __name__ == '__main__':
    main()
