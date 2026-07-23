[CmdletBinding(SupportsShouldProcess = $true)]
param(
    [string]$TaskName = "Ozon_Orders_Sync",
    [string]$TaskPath = "\DB_MP\",
    [int]$IntervalMinutes = 60,
    [string]$Root = "",
    [switch]$SkipFileChecks,
    [switch]$RunNow
)

$ErrorActionPreference = "Stop"
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

if ($IntervalMinutes -lt 15) {
    throw "IntervalMinutes must be at least 15"
}

function Get-RootFromWbOrdersTask {
    $existing = Get-ScheduledTask -TaskPath "\DB_MP\" -TaskName "WB_Orders_Sync" -ErrorAction SilentlyContinue
    if ($null -eq $existing) {
        return ""
    }
    $arguments = ($existing.Actions | Select-Object -First 1).Arguments
    if ($arguments -match '"([^"]*\\scripts\\run_wb_orders\.cmd)"') {
        return (Split-Path (Split-Path $Matches[1] -Parent) -Parent)
    }
    return ""
}

$root = $Root.Trim()
if (-not $root) {
    $root = Get-RootFromWbOrdersTask
}
if (-not $root) {
    $root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
}

$jobScript = Join-Path $root "scripts\run_ozon_orders.cmd"
$venvPython = Join-Path $root ".venv\Scripts\python.exe"
$jobFile = Join-Path $root "app\jobs\job_ozon_orders.py"

if (-not $SkipFileChecks) {
    if (-not (Test-Path $jobScript)) {
        throw "Missing run script: $jobScript"
    }
    if (-not (Test-Path $venvPython)) {
        throw "Missing venv python: $venvPython"
    }
    if (-not (Test-Path $jobFile)) {
        throw "Missing job file: $jobFile"
    }
}

$argument = "/c `"$jobScript`""
$startAt = (Get-Date -Hour 0 -Minute 10 -Second 0)

$action = New-ScheduledTaskAction `
    -Execute "cmd.exe" `
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

$description = "DataBase_MP: Ozon FBO orders hourly sync. Updates current order rows and stores posting change history."

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
    Write-Host "Command: cmd.exe $argument"
    Write-Host "Interval: every $IntervalMinutes minutes"

    if ($RunNow) {
        Start-ScheduledTask -TaskName $TaskName -TaskPath $TaskPath
        Write-Host "OK: task started now"
    }
}
