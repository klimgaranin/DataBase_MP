@echo off
setlocal
set ROOT=%~dp0..
set VENV=%ROOT%\.venv\Scripts\python.exe
set JOB=%ROOT%\app\jobs\job_wb_stocks.py
if not exist "%ROOT%\logs" mkdir "%ROOT%\logs"
"%VENV%" "%JOB%"
endlocal