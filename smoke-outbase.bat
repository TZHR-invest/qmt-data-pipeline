@echo off
rem ============================================================================
rem  smoke-outbase.bat -- verify PIPE_OUT_BASE isolation BEFORE any load test
rem  (added 2026-10-16 after the incident in which a pre-open load test wrote
rem   648 half-day tick parquet files into the PRODUCTION dir; the 16:00 archive
rem   then skipped them as "already done" -- Skip=648 -- and every count-based
rem   check stayed green. See meshdeck docs/machines/trade-pc-data-pipeline.md 13.33)
rem
rem  Expect: the "Output:" line below points at the TEST dir, and 3 parquet files
rem          appear under it. If it points at D:\qmt_data_parquet\... -> STOP,
rem          isolation is broken; do not run the load test.
rem ============================================================================
setlocal
set TESTBASE=D:\qmt_data_parquet_test
set PYTHONPATH=C:\bridge-client\bridge\src;C:\bridge-client
set BIGQMT_ACCOUNT_ID=666810082889
set BIGQMT_FORMULA_ENABLED=0
set PIPE_READ_FIRST=0
set PIPE_OUT_BASE=%TESTBASE%\tick_parquet
echo [smoke] PIPE_OUT_BASE=%PIPE_OUT_BASE%
G:\qmt_projects\quant-qmt-proxy\.venv\Scripts\python.exe G:\qmt_projects\qmt-data-pipeline\update_all_tick_mp.py --limit 3 --workers 2
echo [smoke] SMOKE_EXIT=%ERRORLEVEL%
echo [smoke] ---- test dir contents (expect 3 files) ----
dir /b /s "%TESTBASE%\tick_parquet\*.parquet"
endlocal
