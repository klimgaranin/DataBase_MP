@echo off
setlocal
chcp 65001 >nul
set ROOT=%~dp0..
call "%ROOT%\scripts\run_api_erp_tru_product_stats.cmd"
if errorlevel 1 (
  endlocal & exit /b 1
)
call "%ROOT%\scripts\run_sheets_api_erp_tru_sales_export.cmd"
set JOBEXIT=%ERRORLEVEL%
endlocal & exit /b %JOBEXIT%
