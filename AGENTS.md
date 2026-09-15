# AGENTS.md

QMT data pipeline — tick/1m data download, incremental update, and parquet export.

**Location**: This repo lives inside `G:\qmt_projects\qmt-data-pipeline\`, the broader workspace root.

Key adjacent projects (independent repos):
- `../quant-qmt-proxy/` — gRPC/REST/WS proxy wrapping xtquant for remote access
- `../memory_recall/` — Memory Recall plugin

`xtquant/` — QMT's official Python SDK (also installable via `pip install xtquant`). Key modules:
- `xtdata.py` — data API (K线, tick, L2, 财务数据, 订阅)
- `xttrader.py` — trading API (下单, 撤单, 查询)
- `xtconstant.py` — constants (order types, sides)
- `xttype.py` — data types (StockAccount, etc.)

**IMPORTANT: `xtquant/doc/` has API docs (`xtdata.md`, `xttrader.md`). Always check these docs first when writing code that uses xtdata/xttrader APIs.**

`quant-qmt-proxy/` — gRPC/REST/WS proxy wrapping `xtquant` for remote access. See its own `AGENTS.md`.

## Python environment

The project's Python venv is at `./quant-qmt-proxy/.venv`. Use it for any Python scripts in this workspace:

- Windows: `.\quant-qmt-proxy\.venv\Scripts\python`
- Unix: `./quant-qmt-proxy/.venv/bin/python`

Packages like `xtquant`, `pandas`, `pyarrow` are available inside this venv.

## 行情数据源：mini 已退役，改走大QMT 桥（2026-09-15）

**背景**：16:00 `QMT-Tick-Daily` / 16:30 `QMT-1m-Daily` 原先吃的是 **miniQMT 的行情服务**
（`xtdata` 连 `127.0.0.1:58610` = `miniquote`）。2026-09-15 mini 退役（停 `XtMiniQmt`+`miniquote`）
⇒ 该端口消失 ⇒ venv 里真 `xtquant` 的 `connect()` 直接失败（"无法连接xtquant服务"）。

**现在必须这样跑**（两个 .bat 已经内置；手工跑请照抄这两行 env）：
```
set BIGQMT_ACCOUNT_ID=666810082889
set BIGQMT_FORMULA_ENABLED=0
G:\qmt_projects\quant-qmt-proxy\.venv\Scripts\python.exe <script>.py
```

**⚠️ 光设 PYTHONPATH 没用**：本目录每个脚本都会 `sys.path.insert(0, _WORKSPACE)`
（= `G:\qmt_projects`），而那里有一份官方 SDK 副本 `xtquant/` ⇒ 它排在 PYTHONPATH 之前，
`import xtquant` 命中的是旧 SDK。所以 `update_all_1m_mp.py` / `update_all_tick_mp.py`
在模块体里**显式**插了两行把桥的影子包放到最前：
```
sys.path.insert(0, r"C:\bridge-client")
sys.path.insert(0, r"C:\bridge-client\bridge\src")
```
multiprocessing 的 spawn 子进程会重跑模块体，所以这行对子进程同样生效。

**为什么关 FormulaServer**：开着时它对「带 start/end 的 1m 读取」返回 0 行且不回落；
关掉后走 RPC 返回正确的 241 行/天（代价：每次读 ~5.8s，FormulaServer 的 6 列 OHLCV 快通道
只要 0.015s，但那条路只有 6 列，改了会让 parquet 少 5 列 —— **不要动 schema**）。

**板块名**：桥下 `get_stock_list_in_sector("BJ")` = 0（`get_sector_list` 未实现）；
北交所的真名是 **`京市A股`**（343 只），与 `沪深A股`（5220）相加 = `沪深京A股`（5563）。

**⚠️⚠️ tick 必须用 `get_market_data_ex`（2026-09-15 实测，血坑）**：桥的 RPC 在**大QMT 终端
进程内**执行，而终端自带 **Python 3.6 没有 numpy** ⇒ 旧 API `xtdata.get_market_data()` 直接
`RpcServerRepliedError: ModuleNotFoundError: No module named 'numpy'`（tick/1m 都一样）。
而 `process_stock()` 把这个异常吞成 **`return ("skip", code)`** ⇒ **任务照样 exit 0、日志照样
"Skip=5563 看着很正常"，实际当天一个 parquet 都没写**（当天那批文件是 mini 还活着时写的）。
四种读法实测：`get_market_data(tick)` ❌ / `get_market_data(1m)` ❌ /
`get_market_data_ex(tick)` ✅ 4880 行 / `get_local_data(tick)` ⚠️ 列名齐全但 7 列全 NaN。
**判断"任务是不是真的在干活"只能看盘后落盘的文件数**（不是退出码、不是日志）：

```
find /mnt/d/qmt_data_parquet/tick_parquet -maxdepth 2 -name "$(date +%Y-%m-%d).parquet" | wc -l
```

**⚠️ 桥下 tick 只有 18 列（mini 时代 20 列）**：16 列逐值相同；`pvolume`/`stockStatus`
**反而有真值了**（mini 全 0）；真正丢的是 **`tickvol`**（每笔成交量）与 `pe`
（`pe` 在 mini 时代恒为 0，无损失）。`tickvol` **不可重建**（`tickvol.sum()` 339978 股 vs
`volume` 末值 13762 手，口径不同，`volume.diff()` 只对上 392/4880 行）⇒ **不写 NaN 列**。
⚠️ 下游 `small_cap_flow`（devbox）的 `qmt_loader.py` 把 `tickvol`→`tick_count`、
`l3_features.py` 取 `df["tick_count"]` ⇒ 那里会 KeyError（**响亮**优于 NaN 静默毒化）。

**已知小坑**：①下载是异步的，个别股票第一次读可能为空而被记成 `skip`
（2026-09-15 有 2 只）⇒ 重跑一次作业即可（已完成的会 skip）；②手工从 WSL 调这两个 .bat 时
`%DATE%` = `周二 2026/09/15`（带星期前缀）⇒ 日志会写成 `1m_周二 22609.log`；
计划任务上下文没有前缀，所以正式日志名正常；③**全部 11 个 `import xtquant` 的脚本都已打补丁**
（2026-09-15 晚，含 4 个 tick 脚本的 `_ex` 改写；`read_pingan_1m.py` 没有 `_WORKSPACE` 锚点，
桥路径插在首个顶层 `import xtquant` 之前）—— 新增脚本请自己照抄或跑补丁器 `--check` 复核。

补丁可重放（均在 meshdeck 仓库）：
`scripts/qmt-data-pipeline-bridge-patch.py --check|--apply`（桥路径 + 板块名 + .bat env）、
`scripts/qmt-data-pipeline-tick-api-patch.py --check|--apply`（tick 的 `_ex` 改写）；
回归验证：`scripts/qmt-data-pipeline-parity.py`（在 trade-pc 上跑）。

**监控兜底**：`meshdeck/scripts/qmt-preopen-check.sh`（每交易日 08:40 发正报）已含
「**上一交易日** tick_parquet / kline_1m 落盘数 ≥5300」两项 —— 数据没落盘会当天早上就发红邮件。
