@echo off
setlocal
chcp 65001 >nul
set PYTHONUTF8=1
set ROOT=%~dp0..
set VENV=%ROOT%\.venv\Scripts\python.exe
set JOB=%ROOT%\app\jobs\job_source_statistics.py
set SOURCE_STATISTICS_ORDERS_LIST_PATH=\\tsclient\P\Список заказов\Список заказов VED.xlsx
set SOURCE_STATISTICS_1C_STOCKS_PATH=\\tsclient\S\МП\Остатки МП.txt
if not exist "%ROOT%\logs" mkdir "%ROOT%\logs"
"%VENV%" "%JOB%"
set JOBEXIT=%ERRORLEVEL%
endlocal & exit /b %JOBEXIT%
