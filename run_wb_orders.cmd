@echo off
setlocal EnableExtensions EnableDelayedExpansion

REM ============================================================
REM WB Orders runner (portable)
REM - ROOT вычисляется от расположения этого .cmd (работает из Планировщика)
REM - поднимает Postgres через docker compose
REM - ждёт готовность db через psql внутри контейнера
REM - запускает Python job в .venv
REM - всё пишет в logs_wb_orders.txt
REM ============================================================

REM Для нормальной UTF-8 печати из Python в лог (без кракозябр)
chcp 65001 >nul
set "PYTHONUTF8=1"

REM ROOT = папка, где лежит этот .cmd
set "ROOT=%~dp0"
if "%ROOT:~-1%"=="\" set "ROOT=%ROOT:~0,-1%"

set "LOG=%ROOT%\logs_wb_orders.txt"
set "COMPOSE_FILE=%ROOT%\infra\docker-compose.yml"
set "PY=%ROOT%\.venv\Scripts\python.exe"
set "JOB=%ROOT%\app\jobs_wb_orders_raw_norm.py"

echo ==== %date% %time% START ====>> "%LOG%"
echo ROOT="%ROOT%">> "%LOG%"

REM 0) Валидация файлов, чтобы ошибка была понятной
if not exist "%COMPOSE_FILE%" (
  echo [ERR] docker-compose file not found: "%COMPOSE_FILE%">> "%LOG%"
  echo ==== %date% %time% END FAIL: missing docker-compose ====>> "%LOG%"
  exit /b 2
)

if not exist "%PY%" (
  echo [ERR] venv python not found: "%PY%">> "%LOG%"
  echo Hint: create venv and install requirements in app\requirements.txt>> "%LOG%"
  echo ==== %date% %time% END FAIL: missing venv ====>> "%LOG%"
  exit /b 3
)

if not exist "%JOB%" (
  echo [ERR] job not found: "%JOB%">> "%LOG%"
  echo ==== %date% %time% END FAIL: missing job ====>> "%LOG%"
  exit /b 4
)

REM 1) Поднять docker compose (если Docker Engine не поднят, команда может упасть)
docker compose -f "%COMPOSE_FILE%" up -d >> "%LOG%" 2>&1

REM 2) Подождать готовность БД (до ~120 секунд)
set /a tries=24

:WAIT_DOCKER
docker compose -f "%COMPOSE_FILE%" ps >> "%LOG%" 2>&1
docker compose -f "%COMPOSE_FILE%" exec -T db psql -U app -d marketplace -P pager=off -c "select 1;" >> "%LOG%" 2>&1

if %errorlevel%==0 goto DO_JOB

set /a tries-=1
if %tries% LEQ 0 goto FAIL_DOCKER

timeout /t 5 /nobreak >nul
goto WAIT_DOCKER

:DO_JOB
cd /d "%ROOT%"

REM 3) Запуск основного джоба
"%PY%" "%JOB%" >> "%LOG%" 2>&1
set "JOB_EXIT=%errorlevel%"

if not "%JOB_EXIT%"=="0" (
  echo ==== %date% %time% END FAIL: job exit=%JOB_EXIT% ====>> "%LOG%"
  exit /b %JOB_EXIT%
)

echo ==== %date% %time% END OK ====>> "%LOG%"
exit /b 0

:FAIL_DOCKER
echo ==== %date% %time% END FAIL: docker/db not ready ====>> "%LOG%"
exit /b 1
