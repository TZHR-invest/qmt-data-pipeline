@echo off
set LOGDIR=G:\qmt_projects\qmt-data-pipeline\logs
if not exist "%LOGDIR%" mkdir "%LOGDIR%"
set LOGFILE=%LOGDIR%\tick_%DATE:~0,4%%DATE:~5,2%%DATE:~8,2%.log
rem === 2026-09-15: data source moved from mini miniquote(58610) to the BigQMT bridge ===
set PYTHONPATH=C:\bridge-client\bridge\src;C:\bridge-client
set BIGQMT_ACCOUNT_ID=666810082889
set BIGQMT_FORMULA_ENABLED=0
rem === 2026-10-15: enable the bridge's 3-segment timing (wait/handle/return). ===
rem     Purely additive instrumentation (xtquant_compat.py:1106 reads this env);
rem     it is the only way to attribute the large run-to-run variance of tick cost.
if not exist "G:\qmt_projects\qmt-data-pipeline\logs" mkdir "G:\qmt_projects\qmt-data-pipeline\logs"
set BRIDGE_TIMING_LOG=G:\qmt_projects\qmt-data-pipeline\logs\timing_tick_%DATE:~0,4%%DATE:~5,2%%DATE:~8,2%.log
rem === 2026-10-15: read-first OFF for tick (measured hit rate 505/5577=9% at 16:00,
rem     and only 2/5577=0.04% even after a prior download -> the day's tick is never
rem     local at 16:00, so the pre-read is pure overhead ~0.77s/stock (~17min/day).
rem     Keep PIPE_READ_FIRST=1 for 1m: it runs 16:49 when data IS local (720s->463s). ===
set PIPE_READ_FIRST=0
echo [%DATE% %TIME%] tick update started >> "%LOGFILE%"
G:\qmt_projects\quant-qmt-proxy\.venv\Scripts\python.exe G:\qmt_projects\qmt-data-pipeline\check_trading_day.py >> "%LOGFILE%" 2>&1
if errorlevel 1 (
    echo [%DATE% %TIME%] not a trading day, skip >> "%LOGFILE%"
    exit /b 0
)
G:\qmt_projects\quant-qmt-proxy\.venv\Scripts\python.exe G:\qmt_projects\qmt-data-pipeline\update_all_tick_mp.py --workers 4 >> "%LOGFILE%" 2>&1
echo [%DATE% %TIME%] tick update finished exit=%ERRORLEVEL% >> "%LOGFILE%"