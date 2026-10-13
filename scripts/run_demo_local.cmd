@echo off
rem cmd.exe / Anaconda Prompt launcher for run_demo_local.ps1 (cmd cannot run .ps1 directly).
rem Usage: scripts\run_demo_local.cmd [-Files a.mp4,b.mp4] [-MaxWidth 960]
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0run_demo_local.ps1" %*
