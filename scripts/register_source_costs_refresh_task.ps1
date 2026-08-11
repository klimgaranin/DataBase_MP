[CmdletBinding(SupportsShouldProcess = $true)]
param(
    [string]$TaskPath = "\DB_MP\",
    [string]$TaskName = "Source_Costs_Refresh",
    [int]$EveryMinutes = 1,
    [string]$Root = "",
    [switch]$SkipFileChecks,
    [switch]$RunNow
)

$ErrorActionPreference = "Stop"
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

$root = $Root.Trim()
if (-not $root) {
    $root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
}

$hiddenRunner = Join-Path $root "scripts\run_hidden.vbs"
$venvPython = Join-Path $root ".venv\Scripts\python.exe"
$refreshScript = Join-Path $root "scripts\run_source_costs_refresh.cmd"
$sourceJob = Join-Path $root "app\jobs\job_source_costs.py"
$sheetsJob = Join-Path $root "app\jobs\job_sheets_source_costs_export.py"

if (-not $SkipFileChecks) {
    foreach ($path in @($hiddenRunner, $venvPython, $refreshScript, $sourceJob, $sheetsJob)) {
        if (-not (Test-Path $path)) {
            throw "Missing file: $path"
        }
    }
}

$argument = "`"$hiddenRunner`" `"$refreshScript`""
$action = New-ScheduledTaskAction -Execute "wscript.exe" -Argument $argument
$trigger = New-ScheduledTaskTrigger -Once -At (Get-Date).Date -RepetitionInterval (New-TimeSpan -Minutes $EveryMinutes)
$settings = New-ScheduledTaskSettingsSet `
    -StartWhenAvailable `
    -MultipleInstances IgnoreNew `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 20) `
    -RestartCount 1 `
    -RestartInterval (New-TimeSpan -Minutes 5)

if ($PSCmdlet.ShouldProcess("$TaskPath$TaskName", "Register scheduled task")) {
    Register-ScheduledTask `
        -TaskName $TaskName `
        -TaskPath $TaskPath `
        -Action $action `
        -Trigger $trigger `
        -Settings $settings `
        -Description "DataBase_MP: checks 1C source cost file and exports Ozon/WB costs to Google Sheets when changed." `
        -Force | Out-Null

    Write-Host "OK: task $TaskPath$TaskName created or updated"
    Write-Host "Command: wscript.exe $argument"
    Write-Host "Every minutes: $EveryMinutes"
}

if ($RunNow) {
    Start-ScheduledTask -TaskName $TaskName -TaskPath $TaskPath
    Write-Host "OK: task started now"
}
