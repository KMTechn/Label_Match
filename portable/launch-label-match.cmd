@echo off
setlocal
set "LABEL_MATCH_PORTABLE_ROOT=%~dp0"
"%LABEL_MATCH_PORTABLE_ROOT%runtime\pythonw.exe" -B "%LABEL_MATCH_PORTABLE_ROOT%app\main.py" %*
exit /b %ERRORLEVEL%
