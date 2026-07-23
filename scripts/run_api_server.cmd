@echo off
setlocal
chcp 65001 >nul
set PYTHONUTF8=1
set ROOT=%~dp0..
set VENV=%ROOT%\.venv\Scripts\python.exe
cd /d "%ROOT%"
"%VENV%" -m uvicorn app.api_server:app --host 0.0.0.0 --port 8080 --workers 1
set JOBEXIT=%ERRORLEVEL%
endlocal & exit /b %JOBEXIT%
