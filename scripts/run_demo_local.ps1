<#
.SYNOPSIS
    Start one transmitter per video file, each in its own console window.
.DESCRIPTION
    Simulates a multi-camera venue: every file becomes a camera whose id is the
    file name without extension (cam1.mp4 -> cam1). Each transmitter runs as a
    separate process in a titled window, so you can stop one cleanly (Ctrl+C in
    its window) or "crash" it (close the window) during the failure drills.

    Start the cluster, create topics and start the switchboard first.
.EXAMPLE
    scripts\run_demo_local.ps1
.EXAMPLE
    scripts\run_demo_local.ps1 -Files sample_videos\a.mp4,sample_videos\b.mp4 -MaxWidth 960
#>
param(
    [string[]]$Files,
    [int]$MaxWidth = 1280,
    [string]$Brokers = $env:DRAGONFLY_BROKERS
)
$ErrorActionPreference = 'Stop'

$root = Split-Path -Parent $PSScriptRoot
if (-not $Files) {
    $Files = Get-ChildItem -Path (Join-Path $root 'sample_videos') -Filter 'cam*.mp4' |
        Sort-Object Name | ForEach-Object FullName
}
if (-not $Files) {
    Write-Error 'no videos found: put cam1.mp4, cam2.mp4, ... in sample_videos\ or pass -Files'
}

$venvPython = Join-Path $root '.venv\Scripts\python.exe'
$python = if (Test-Path $venvPython) { $venvPython } else { 'python' }
$shell = if (Get-Command pwsh -ErrorAction SilentlyContinue) { 'pwsh' } else { 'powershell' }

foreach ($file in $Files) {
    $path = (Resolve-Path -LiteralPath $file).Path
    $cameraId = [IO.Path]::GetFileNameWithoutExtension($path)
    $txArgs = "--camera-id '$cameraId' --file '$path' --max-width $MaxWidth"
    if ($Brokers) { $txArgs += " --brokers '$Brokers'" }
    $command = "`$Host.UI.RawUI.WindowTitle = 'Dragonfly transmitter: $cameraId'; " +
               "& '$python' transmitter.py $txArgs"
    Start-Process -FilePath $shell -WorkingDirectory $root `
        -ArgumentList '-NoExit', '-NoProfile', '-Command', $command | Out-Null
    Write-Host "started $cameraId <- $path"
}
Write-Host "`n$($Files.Count) transmitter(s) started. Ctrl+C in a window = clean stop; closing it = crash."
