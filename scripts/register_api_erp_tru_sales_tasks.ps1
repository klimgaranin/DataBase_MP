[CmdletBinding(SupportsShouldProcess = $true)]
param(
    [string]$TaskPath = "\DB_MP\",
    [string]$ApiTaskName = "API_ERP_TRU_Product_Stats",
    [string]$SheetsTaskName = "Sheets_API_ERP_TRU_Sales_Export",
    [string]$ApiAt = "06:20",
    [string]$SheetsAt = "06:22",
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
$apiScript = Join-Path $root "scripts\run_api_erp_tru_product_stats.cmd"
$apiJob = Join-Path $root "app\jobs\job_api_erp_tru_product_stats.py"
$sheetsScript = Join-Path $root "scripts\run_sheets_api_erp_tru_sales_export.cmd"
$sheetsJob = Join-Path $root "app\jobs\job_sheets_api_erp_tru_sales_export.py"

if (-not $SkipFileChecks) {
    foreach ($path in @($hiddenRunner, $venvPython, $apiScript, $apiJob, $sheetsScript, $sheetsJob)) {
        if (-not (Test-Path $path)) {
            throw "Missing file: $path"
        }
    }
}

function Register-HiddenCmdTask {
    param(
        [string]$Name,
        [string]$CmdPath,
        [string]$At,
        [string]$Description
    )
    $argument = "`"$hiddenRunner`" `"$CmdPath`""
    $action = New-ScheduledTaskAction -Execute "wscript.exe" -Argument $argument
    $trigger = New-ScheduledTaskTrigger -Daily -At $At
    $settings = New-ScheduledTaskSettingsSet `
        -StartWhenAvailable `
        -MultipleInstances IgnoreNew `
        -ExecutionTimeLimit (New-TimeSpan -Minutes 40) `
        -RestartCount 1 `
        -RestartInterval (New-TimeSpan -Minutes 5)

    if ($PSCmdlet.ShouldProcess("$TaskPath$Name", "Register scheduled task")) {
        Register-ScheduledTask `
            -TaskName $Name `
            -TaskPath $TaskPath `
            -Action $action `
            -Trigger $trigger `
            -Settings $settings `
            -Description $Description `
            -Force | Out-Null

        Write-Host "OK: task $TaskPath$Name created or updated"
        Write-Host "Command: wscript.exe $argument"
        Write-Host "Daily at: $At"
    }
}

Register-HiddenCmdTask `
    -Name $ApiTaskName `
    -CmdPath $apiScript `
    -At $ApiAt `
    -Description "DataBase_MP: daily ERP/TRU product sales statistics import into PostgreSQL."

Register-HiddenCmdTask `
    -Name $SheetsTaskName `
    -CmdPath $sheetsScript `
    -At $SheetsAt `
    -Description "DataBase_MP: daily ERP/TRU sales export from PostgreSQL to Google Sheets DATA!AE:AF."

if ($RunNow) {
    Start-ScheduledTask -TaskName $ApiTaskName -TaskPath $TaskPath
    Write-Host "OK: API task started now"
}
