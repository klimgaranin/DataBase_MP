[CmdletBinding(SupportsShouldProcess = $true)]
param(
    [string]$TaskName = "Sheets_Orders_Export",
    [string]$TaskPath = "\DB_MP\",
    [int]$IntervalMinutes = 60,
    [int]$StartMinute = 12,
    [string]$Root = "",
    [switch]$SkipFileChecks,
    [switch]$RunNow
)

$ErrorActionPreference = "Stop"
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

if ($IntervalMinutes -lt 15) {
    throw "IntervalMinutes must be at least 15"
}
if ($StartMinute -lt 0 -or $StartMinute -gt 59) {
    throw "StartMinute must be between 0 and 59"
}

function Get-RootFromExistingTask {
    $existing = Get-ScheduledTask -TaskPath "\DB_MP\" -TaskName "Ozon_Orders_Sync" -ErrorAction SilentlyContinue
    if ($null -eq $existing) {
        $existing = Get-ScheduledTask -TaskPath "\DB_MP\" -TaskName "WB_Orders_Sync" -ErrorAction SilentlyContinue
    }
    if ($null -eq $existing) {
        return ""
    }
    $arguments = ($existing.Actions | Select-Object -First 1).Arguments
    if ($arguments -match '"([^"]*\\scripts\\run_[^"]+\.cmd)"') {
        return (Split-Path (Split-Path $Matches[1] -Parent) -Parent)
    }
    return ""
}

$root = $Root.Trim()
if (-not $root) {
    $root = Get-RootFromExistingTask
}
if (-not $root) {
    $root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
}

$jobScript = Join-Path $root "scripts\run_sheets_orders_export.cmd"
$hiddenRunner = Join-Path $root "scripts\run_hidden.vbs"
$venvPython = Join-Path $root ".venv\Scripts\python.exe"
$jobFile = Join-Path $root "app\jobs\job_sheets_orders_export.py"

if (-not $SkipFileChecks) {
    if (-not (Test-Path $jobScript)) {
        throw "Missing run script: $jobScript"
    }
    if (-not (Test-Path $hiddenRunner)) {
        throw "Missing hidden runner: $hiddenRunner"
    }
    if (-not (Test-Path $venvPython)) {
        throw "Missing venv python: $venvPython"
    }
    if (-not (Test-Path $jobFile)) {
        throw "Missing job file: $jobFile"
    }
}

$argument = "`"$hiddenRunner`" `"$jobScript`""
$today = Get-Date
$startAt = Get-Date `
    -Year $today.Year `
    -Month $today.Month `
    -Day $today.Day `
    -Hour 0 `
    -Minute $StartMinute `
    -Second 0 `
    -Millisecond 0

$action = New-ScheduledTaskAction `
    -Execute "wscript.exe" `
    -Argument $argument

$trigger = New-ScheduledTaskTrigger `
    -Once `
    -At $startAt `
    -RepetitionInterval (New-TimeSpan -Minutes $IntervalMinutes)

$settings = New-ScheduledTaskSettingsSet `
    -StartWhenAvailable `
    -MultipleInstances IgnoreNew `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 10) `
    -RestartCount 2 `
    -RestartInterval (New-TimeSpan -Minutes 2)

$description = "DataBase_MP: export Ozon and WB order aggregates from PostgreSQL to Google Sheets DATA."

if ($PSCmdlet.ShouldProcess("$TaskPath$TaskName", "Register scheduled task")) {
    Register-ScheduledTask `
        -TaskName $TaskName `
        -TaskPath $TaskPath `
        -Action $action `
        -Trigger $trigger `
        -Settings $settings `
        -Description $description `
        -Force | Out-Null

    Write-Host "OK: task $TaskPath$TaskName created or updated"
    Write-Host "Command: wscript.exe $argument"
    Write-Host "Interval: every $IntervalMinutes minutes"
    Write-Host "Start minute: $StartMinute"

    if ($RunNow) {
        Start-ScheduledTask -TaskName $TaskName -TaskPath $TaskPath
        Write-Host "OK: task started now"
    }
}
