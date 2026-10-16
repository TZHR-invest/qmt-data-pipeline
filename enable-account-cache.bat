@echo off
rem Enable account-read cache: restart QMTProxy-666 only (020/2986 untouched)
echo === restart QMTProxy-666 ===
sc stop QMTProxy-666 > nul
ping -n 5 127.0.0.1 > nul
for /f tokens=5 %%p in ('netstat -ano ^| findstr ":8002" ^| findstr LISTENING') do taskkill /F /T /PID %%p
ping -n 3 127.0.0.1 > nul

sc start QMTProxy-666 > nul
ping -n 10 127.0.0.1 > nul
netstat -ano | findstr LISTENING | findstr ":8002"
