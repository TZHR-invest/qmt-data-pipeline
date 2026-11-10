@echo off
setlocal EnableExtensions
rem =====================================================================
rem backfill_day.bat -- re-run the tick + 1m archive for ONE given date.
rem
rem WHY (2026-11-10): the 2026-11-09 QMT-Archive-Daily started at 16:00:01
rem   and was killed at 87% (4835/5579) because the machine was powered off
rem   by hand (clean Windows reboot at 17:29:57). Outcome: tick 4831/5563
rem   files, 1m 0/5563. Both worker scripts used to hardcode datetime.now(),
rem   so there was NO entry point able to refill that day. The date is now
rem   passed via PIPE_TARGET_DATE (unset = today => production unchanged).
rem
rem USAGE (equivalent):
rem   1) scheduled task: echo 20261109 > backfill_date.txt
rem                      schtasks /run /tn QMT-Backfill-Day
rem   2) manual:         backfill_day.bat 20261109
rem   meshdeck wrapper:  bash scripts/qmt-backfill-day.sh 20261109
rem
rem GUARDS:
rem   1) No date => exit. Never defaults to "today" (that is the 16:00
rem      archive's job; running both would fight over the same bridge).
rem   2) Time window: only 15:00-15:59 or >= 17:55 (stays out of the
rem      16:00-17:50 archive slot and the 09:15-15:00 session). The 2nd
rem      argument --force overrides it.
rem   3) Truncated files are NOT auto-repaired: the skip rule is
rem      "exists && size>0". Move suspicious files into
rem      D:\qmt_data_parquet\_quarantine\ first (repo AGENTS.md rule 3).
rem
rem NOTE: this file is deliberately PURE ASCII -- cmd.exe decodes .bat as
rem   cp936, and UTF-8 Chinese comments get executed as commands.
rem =====================================================================

set "REPO=G:\qmt_projects\qmt-data-pipeline"
set "PY=G:\qmt_projects\quant-qmt-proxy\.venv\Scripts\python.exe"
set "LOGDIR=%REPO%\logs"
set "DFILE=%REPO%\backfill_date.txt"
set "ELOG=%LOGDIR%\backfill_errors.log"
if not exist "%LOGDIR%" mkdir "%LOGDIR%"

set "DAY=%~1"
if not defined DAY if exist "%DFILE%" set /p DAY=<"%DFILE%"
if not defined DAY goto :no_date

rem ---- validate: real YYYYMMDD calendar date (python does the work) ----
"%PY%" -c "import sys,datetime;datetime.datetime.strptime(sys.argv[1],'%%Y%%m%%d')" "%DAY%" >nul 2>&1
if errorlevel 1 goto :bad_date

rem ---- time-window guard (string compare on zero-padded HHMM) ----
set "HHMM=?"
for /f "tokens=1,2 delims=:" %%a in ("%TIME: =0%") do set "HHMM=%%a%%b"
if "%~2"=="--force" goto :guard_ok
if not "%HHMM:~3,1%"=="" if not "%HHMM:~4,1%"=="" if "%HHMM%" geq "1500" if "%HHMM%" leq "1559" goto :guard_ok
if not "%HHMM:~3,1%"=="" if not "%HHMM:~4,1%"=="" if "%HHMM%" geq "1755" goto :guard_ok
echo [%DATE% %TIME%] REFUSED day=%DAY%: outside window (allowed 15:00-15:59 or 17:55+; override with --force)>>"%LOGDIR%\backfill_archive_%DAY%.log"
echo REFUSED: now %TIME% -- allowed 15:00-15:59 or 17:55+, override with --force
exit /b 3
:guard_ok

set "BLOG=%LOGDIR%\backfill_archive_%DAY%.log"
set "TLOG=%LOGDIR%\backfill_tick_%DAY%.log"
set "OLOG=%LOGDIR%\backfill_1m_%DAY%.log"

set "PYTHONPATH=C:\bridge-client\bridge\src;C:\bridge-client"
set "BIGQMT_ACCOUNT_ID=666810082889"
set "BIGQMT_FORMULA_ENABLED=0"
set "PIPE_TARGET_DATE=%DAY%"

echo [%DATE% %TIME%] backfill started day=%DAY%>>"%BLOG%"

rem ---- stage 1: tick ----
rem read-first OFF, same as production scheduled_tick.bat.
set "PIPE_READ_FIRST=0"
echo [%DATE% %TIME%] stage tick start>>"%BLOG%"
"%PY%" "%REPO%\update_all_tick_mp.py" --workers 4>>"%TLOG%" 2>&1
set "RC_TICK=%ERRORLEVEL%"
echo [%DATE% %TIME%] stage tick returned rc=%RC_TICK%>>"%BLOG%"

rem ---- stage 2: 1m ----
rem read-first OFF too: this day's 1m was never archived, local hit rate is
rem unknown and one miss costs ~5.8s/code (~9h for 5563). download+read is
rem the measured 720s baseline and is deterministic.
set "PIPE_READ_FIRST=0"
echo [%DATE% %TIME%] stage 1m start>>"%BLOG%"
"%PY%" "%REPO%\update_all_1m_mp.py" --workers 4>>"%OLOG%" 2>&1
set "RC_1M=%ERRORLEVEL%"
echo [%DATE% %TIME%] stage 1m returned rc=%RC_1M%>>"%BLOG%"

echo [%DATE% %TIME%] backfill finished (tick=%RC_TICK% 1m=%RC_1M%)>>"%BLOG%"
if not "%RC_TICK%"=="0" exit /b %RC_TICK%
exit /b %RC_1M%

:no_date
echo [%DATE% %TIME%] ERROR no date given - pass YYYYMMDD as arg1 or write backfill_date.txt>>"%ELOG%"
echo ERROR: no date given - pass YYYYMMDD as arg1 or write backfill_date.txt
exit /b 2

:bad_date
echo [%DATE% %TIME%] ERROR bad date "%DAY%">>"%ELOG%"
echo ERROR: not a valid YYYYMMDD date: "%DAY%"
exit /b 2
