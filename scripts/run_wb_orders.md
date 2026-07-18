@echo off
setlocal EnableExtensions EnableDelayedExpansion
chcp 65001 >nul
set PYTHONUTF8=1

set ROOT=%~dp0
if "%ROOT:~-1%"=="\" set ROOT=%ROOT:~0,-1%

set LOG=%ROOT%\logs\wb_orders.txt
set COMPOSEFILE=%ROOT%\infra\docker-compose.yml
set ENVFILE=%ROOT%\.env
set PY=%ROOT%\.venv\Scripts\python.exe
set JOB=%ROOT%\app\jobs_wb_orders_raw_norm.py
set CONTAINER=infra-db-1

if not exist "%ROOT%\logs\" mkdir "%ROOT%\logs\"

echo %date% %time% START >> "%LOG%"
echo ROOT=%ROOT% >> "%LOG%"

if not exist "%COMPOSEFILE%" (
    echo ERR: no compose file: %COMPOSEFILE% >> "%LOG%"
    echo %date% %time% END FAIL missing compose >> "%LOG%"
    exit /b 2
)
if not exist "%PY%" (
    echo ERR: no venv python: %PY% >> "%LOG%"
    echo %date% %time% END FAIL missing venv >> "%LOG%"
    exit /b 3
)
if not exist "%JOB%" (
    echo ERR: no job file: %JOB% >> "%LOG%"
    echo %date% %time% END FAIL missing job >> "%LOG%"
    exit /b 4
)

echo %date% %time% docker compose up -d >> "%LOG%"
docker compose --env-file "%ENVFILE%" -f "%COMPOSEFILE%" up -d >> "%LOG%" 2>&1

set /a tries=24
:WAITDOCKER
docker exec -e PGPASSWORD=%DB_PASS% %CONTAINER% psql -U app -d marketplace --no-password -c "select 1" >> "%LOG%" 2>&1
if !errorlevel!==0 goto DOJOB
set /a tries=!tries!-1
if !tries! LEQ 0 goto FAILDOCKER
timeout /t 5 /nobreak >nul
goto WAITDOCKER

:DOJOB
echo %date% %time% DB ready >> "%LOG%"
cd /d "%ROOT%"
"%PY%" "%JOB%" >> "%LOG%" 2>&1
set JOBEXIT=!errorlevel!
if not !JOBEXIT!==0 (
    echo %date% %time% END FAIL job exit=!JOBEXIT! >> "%LOG%"
    exit /b !JOBEXIT!
)
echo %date% %time% END OK >> "%LOG%"
exit /b 0

:FAILDOCKER
echo %date% %time% END FAIL db not ready after 120s >> "%LOG%"
exit /b 1
