<#
.SYNOPSIS
    Convert any phone/camera clip into a transmitter-friendly MP4.
.DESCRIPTION
    PowerShell twin of convert_video.sh. Produces H.264 (yuv420p) video only,
    at a constant frame rate, with rotation applied to the pixels (portrait
    stays portrait), a keyframe every second, and faststart.
.EXAMPLE
    scripts\convert_video.ps1 sample_videos\IMG_3247.MOV sample_videos\cam1.mp4
.EXAMPLE
    scripts\convert_video.ps1 in.mov out.mp4 -Fps 25
.PARAMETER Ffmpeg
    ffmpeg binary to use. Defaults to $env:DRAGONFLY_FFMPEG, then 'ffmpeg' on
    PATH. Needed when the first ffmpeg on PATH lacks libx264 (e.g. Anaconda's).
#>
param(
    [Parameter(Mandatory = $true, Position = 0)][string]$InputFile,
    [Parameter(Mandatory = $true, Position = 1)][string]$OutputFile,
    [int]$Fps = 30,
    [string]$Ffmpeg = $(if ($env:DRAGONFLY_FFMPEG) { $env:DRAGONFLY_FFMPEG } else { 'ffmpeg' })
)
$ErrorActionPreference = 'Stop'

if (-not (Test-Path -LiteralPath $InputFile -PathType Leaf)) {
    Write-Error "input file not found: $InputFile"
}
$cmd = Get-Command $Ffmpeg -ErrorAction SilentlyContinue
if (-not $cmd) {
    Write-Error "ffmpeg not found: $Ffmpeg"
}
if (-not (& $cmd.Source -hide_banner -encoders 2>$null | Select-String -SimpleMatch 'libx264' -Quiet)) {
    Write-Error "$($cmd.Source) has no libx264 encoder; install a full build or pass -Ffmpeg (see README)"
}

& $cmd.Source -hide_banner -loglevel error -stats -y `
    -i $InputFile `
    -map 0:v:0 -an -sn -dn `
    -vf "fps=$Fps,format=yuv420p" `
    -c:v libx264 -preset veryfast -crf 20 `
    -g $Fps -keyint_min $Fps `
    -movflags +faststart `
    $OutputFile
if ($LASTEXITCODE -ne 0) { Write-Error "ffmpeg failed with exit code $LASTEXITCODE" }

Write-Host "wrote $OutputFile"
