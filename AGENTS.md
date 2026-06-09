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
