@echo off
set LOGDIR=G:\qmt_projects\qmt-data-pipeline\logs
if not exist "%LOGDIR%" mkdir "%LOGDIR%"
set LOGFILE=%LOGDIR%\1m_%DATE:~0,4%%DATE:~5,2%%DATE:~8,2%.log
echo [%DATE% %TIME%] 1m update started >> "%LOGFILE%"
G:\qmt_projects\quant-qmt-proxy\.venv\Scripts\python.exe G:\qmt_projects\qmt-data-pipeline\update_all_1m_mp.py --workers 4 >> "%LOGFILE%" 2>&1
echo [%DATE% %TIME%] 1m update finished exit=%ERRORLEVEL% >> "%LOGFILE%"
