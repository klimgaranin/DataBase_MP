@echo off
setlocal
chcp 65001 >nul
set PYTHONUTF8=1
set ROOT=%~dp0..
set VENV=%ROOT%\.venv\Scripts\python.exe
set JOB=%ROOT%\app\jobs\job_ozon_placement.py
if not exist "%ROOT%\logs" mkdir "%ROOT%\logs"
"%VENV%" "%JOB%"
endlocal
