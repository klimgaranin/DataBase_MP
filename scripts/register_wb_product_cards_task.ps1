[CmdletBinding(SupportsShouldProcess = $true)]
param(
    [string]$TaskName = "WB_Product_Cards_Sync",
    [string]$TaskPath = "\DB_MP\",
    [int]$IntervalMinutes = 60,
    [string]$Root = "",
    [switch]$SkipFileChecks,
    [switch]$RunNow
)

$ErrorActionPreference = "Stop"
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
if ($IntervalMinutes -lt 15) { throw "IntervalMinutes must be at least 15" }

function Get-RootFromWbOrdersTask {
    $existing = Get-ScheduledTask -TaskPath "\DB_MP\" -TaskName "WB_Orders_Sync" -ErrorAction SilentlyContinue
    if ($null -eq $existing) { return "" }
    $arguments = ($existing.Actions | Select-Object -First 1).Arguments
    if ($arguments -match '"([^"]*\\scripts\\run_wb_orders\.cmd)"') {
        return (Split-Path (Split-Path $Matches[1] -Parent) -Parent)
    }
    return ""
}

$root = $Root.Trim()
if (-not $root) { $root = Get-RootFromWbOrdersTask }
if (-not $root) { $root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path }

$jobScript = Join-Path $root "scripts\run_wb_product_cards.cmd"
$hiddenRunner = Join-Path $root "scripts\run_hidden.vbs"
$venvPython = Join-Path $root ".venv\Scripts\python.exe"
$jobFile = Join-Path $root "app\jobs\job_wb_product_cards.py"

if (-not $SkipFileChecks) {
    foreach ($file in @($jobScript, $hiddenRunner, $venvPython, $jobFile)) {
        if (-not (Test-Path $file)) { throw "Missing required file: $file" }
    }
}

$argument = "`"$hiddenRunner`" `"$jobScript`""
$startAt = (Get-Date -Hour 0 -Minute 20 -Second 0)
$action = New-ScheduledTaskAction -Execute "wscript.exe" -Argument $argument
$trigger = New-ScheduledTaskTrigger -Once -At $startAt -RepetitionInterval (New-TimeSpan -Minutes $IntervalMinutes)
$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -MultipleInstances IgnoreNew -ExecutionTimeLimit (New-TimeSpan -Minutes 30) -RestartCount 2 -RestartInterval (New-TimeSpan -Minutes 2)
$description = "DataBase_MP: WB product cards hourly sync. Loads official photos from Content API."

if ($PSCmdlet.ShouldProcess("$TaskPath$TaskName", "Register scheduled task")) {
    Register-ScheduledTask -TaskName $TaskName -TaskPath $TaskPath -Action $action -Trigger $trigger -Settings $settings -Description $description -Force | Out-Null
    Write-Host "OK: task $TaskPath$TaskName created or updated"
    Write-Host "Interval: every $IntervalMinutes minutes"
    if ($RunNow) {
        Start-ScheduledTask -TaskName $TaskName -TaskPath $TaskPath
        Write-Host "OK: task started now"
    }
}
