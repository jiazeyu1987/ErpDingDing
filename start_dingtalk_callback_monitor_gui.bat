@echo off
chcp 65001 >nul
cd /d "%~dp0"
python code\dingtalk_callback_monitor_gui.py --env-file D:\ProjectPackage\demo\dingding_demo\code\.env %*
pause
