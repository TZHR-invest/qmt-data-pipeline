# 2026-10-19 temp: wait for the manual tick backfill to finish, then run the 1m
# stage. Keeps the serial design (both stages share ONE QMT client / proxy 8002).
$ErrorActionPreference = 'SilentlyContinue'
$log = 'G:\qmt_projects\qmt-data-pipeline\logs\backfill_chain_20261019.log'
function Log([string]$m) { Add-Content -Path $log -Value ('[' + (Get-Date -Format 'HH:mm:ss') + '] ' + $m) -Encoding UTF8 }

Log 'watcher started'
$deadline = (Get-Date).AddMinutes(150)
$sawTick = $false
while ((Get-Date) -lt $deadline) {
    $n = @(Get-CimInstance Win32_Process -Filter "Name='python.exe'" |
            Where-Object { $_.CommandLine -like '*update_all_tick*' }).Count
    if ($n -gt 0) { $sawTick = $true }
    if ($n -eq 0 -and $sawTick) { break }
    if ($n -eq 0 -and -not $sawTick) { $sawTick = $true }   # tick may end between polls
    Start-Sleep -Seconds 30
}
$n = @(Get-CimInstance Win32_Process -Filter "Name='python.exe'" |
        Where-Object { $_.CommandLine -like '*update_all_tick*' }).Count
if ($n -gt 0) { Log 'GAVE UP: tick workers still running at deadline'; exit 9 }

Log 'tick workers gone -> starting 1m stage'
$p = Start-Process -FilePath 'cmd.exe' -ArgumentList '/c G:\qmt_projects\qmt-data-pipeline\scheduled_1m.bat' -Wait -PassThru -NoNewWindow
Log ('1m stage rc=' + $p.ExitCode)
Log 'watcher finished'
exit $p.ExitCode
