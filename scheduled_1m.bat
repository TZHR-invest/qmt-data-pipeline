@echo off
set LOGDIR=G:\qmt_projects\qmt-data-pipeline\logs
if not exist "%LOGDIR%" mkdir "%LOGDIR%"
set LOGFILE=%LOGDIR%\1m_%DATE:~0,4%%DATE:~5,2%%DATE:~8,2%.log
echo [%DATE% %TIME%] 1m daily update started >> "%LOGFILE%"
G:\qmt_projects\quant-qmt-proxy\.venv\Scripts\python.exe G:\qmt_projects\qmt-data-pipeline\update_all_1m_mp.py --workers 4 >> "%LOGFILE%" 2>&1
echo [%DATE% %TIME%] 1m daily update finished exit=%ERRORLEVEL% >> "%LOGFILE%"

REM 合并当月日级文件→月度 parquet（首次较慢，后续增量）
echo [%DATE% %TIME%] 1m consolidation started >> "%LOGFILE%"
G:\qmt_projects\quant-qmt-proxy\.venv\Scripts\python.exe G:\qmt_projects\qmt-data-pipeline\consolidate_1m_to_monthly.py >> "%LOGFILE%" 2>&1
echo [%DATE% %TIME%] 1m consolidation finished exit=%ERRORLEVEL% >> "%LOGFILE%"
