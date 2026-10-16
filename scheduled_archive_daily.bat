@echo off
rem ============================================================================
rem scheduled_archive_daily.bat  (2026-10-12)
rem
rem Why this wrapper exists:
rem   scheduled_tick.bat (16:00) and scheduled_1m.bat (16:30) BOTH drive the same
rem   QMT client (BIGQMT_ACCOUNT_ID=666810082889 -> proxy 8002 -> G:\qmt1).
rem   Running them in parallel (16:30-17:05, 8 workers total) doubled the request
rem   queue inside that one client, which pushed the 16:30 NAV job's reads to
rem   2.3-4.4 s and once crossed the client's 10 s read timeout (2026-10-12).
rem   Running them one after the other keeps only 4 workers in flight at any time.
rem
rem Contract kept unchanged:
rem   - each stage keeps its own log file  (logs\tick_<date>.log / logs\1m_<date>.log)
rem   - each stage keeps its own trading-day guard (non-trading day -> skip, rc 0)
rem   - this wrapper adds only logs\archive_<date>.log (stage boundaries)
rem
rem Rollback: delete task QMT-Archive-Daily, re-enable QMT-Tick-Daily + QMT-1m-Daily.
rem ============================================================================
set LOGDIR=G:\qmt_projects\qmt-data-pipeline\logs
if not exist "%LOGDIR%" mkdir "%LOGDIR%"
set ALOG=%LOGDIR%\archive_%DATE:~0,4%%DATE:~5,2%%DATE:~8,2%.log
echo [%DATE% %TIME%] serial archive started (tick then 1m) >> "%ALOG%"
call G:\qmt_projects\qmt-data-pipeline\scheduled_tick.bat
set RC_TICK=%ERRORLEVEL%
echo [%DATE% %TIME%] stage tick returned rc=%RC_TICK% >> "%ALOG%"
call G:\qmt_projects\qmt-data-pipeline\scheduled_1m.bat
set RC_1M=%ERRORLEVEL%
echo [%DATE% %TIME%] stage 1m returned rc=%RC_1M% >> "%ALOG%"
rem --- 2026-10-16: 日志保留期（60 天）。失败不影响归档退出码 ---
G:\qmt_projects\quant-qmt-proxy\.venv\Scripts\python.exe G:\qmt_projects\qmt-data-pipeline\prune-logs.py >> "%ALOG%" 2>&1
echo [%DATE% %TIME%] serial archive finished (tick=%RC_TICK% 1m=%RC_1M%) >> "%ALOG%"
rem 2026-10-16: 原为 exit /b 0 —— 把两阶段的失败全都吞掉了（10-15 tick exit=1
rem             而任务"上次结果"仍是 0）。现在返回最差码：tick 非 0 优先。
if not "%RC_TICK%"=="0" exit /b %RC_TICK%
exit /b %RC_1M%
