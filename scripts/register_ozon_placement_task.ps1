[CmdletBinding(SupportsShouldProcess = $true)]
param(
    [string]$TaskName = "Ozon_Placement_Sync",
    [string]$TaskPath = "\DB_MP\",
    [string]$At = "07:00",
    [string]$Root = "",
    [switch]$SkipFileChecks,
    [switch]$RunNow
)

$ErrorActionPreference = "Stop"
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

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

$jobScript = Join-Path $root "scripts\run_ozon_placement.cmd"
$hiddenRunner = Join-Path $root "scripts\run_hidden.vbs"
$venvPython = Join-Path $root ".venv\Scripts\python.exe"
$jobFile = Join-Path $root "app\jobs\job_ozon_placement.py"

if (-not $SkipFileChecks) {
    foreach ($path in @($jobScript, $hiddenRunner, $venvPython, $jobFile)) {
        if (-not (Test-Path $path)) {
            throw "Missing file: $path"
        }
    }
}

$argument = "`"$hiddenRunner`" `"$jobScript`""
$action = New-ScheduledTaskAction -Execute "wscript.exe" -Argument $argument
$trigger = New-ScheduledTaskTrigger -Daily -At $At
$settings = New-ScheduledTaskSettingsSet `
    -StartWhenAvailable `
    -MultipleInstances IgnoreNew `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 30) `
    -RestartCount 2 `
    -RestartInterval (New-TimeSpan -Minutes 5)

$description = "DataBase_MP: daily Ozon placement/storage cost report sync to PostgreSQL."

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
    Write-Host "Daily at: $At"

    if ($RunNow) {
        Start-ScheduledTask -TaskName $TaskName -TaskPath $TaskPath
        Write-Host "OK: task started now"
    }
}
