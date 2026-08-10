@echo off
setlocal
chcp 65001 >nul
set PYTHONUTF8=1
set ROOT=%~dp0..
set VENV=%ROOT%\.venv\Scripts\python.exe
set SOURCE_JOB=%ROOT%\app\jobs\job_source_statistics.py
set SHEETS_JOB=%ROOT%\app\jobs\job_sheets_source_files_export.py
if not exist "%ROOT%\logs" mkdir "%ROOT%\logs"

set SOURCE_STATISTICS_NO_CHANGES_EXIT_CODE=3
"%VENV%" "%SOURCE_JOB%"
set SOURCE_EXIT=%ERRORLEVEL%
if "%SOURCE_EXIT%"=="3" (
    endlocal & exit /b 0
)
if not "%SOURCE_EXIT%"=="0" (
    endlocal & exit /b %SOURCE_EXIT%
)

"%VENV%" "%SHEETS_JOB%"
set SHEETS_EXIT=%ERRORLEVEL%
endlocal & exit /b %SHEETS_EXIT%
