@echo off
cd /d %~dp0..
python -m uvicorn app.api_server:app --host 0.0.0.0 --port 8080 --workers 1
