$ErrorActionPreference = "Stop"
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

$taskNames = @("WB_Orders_Sync", "WB_Stocks_Sync", "Ozon_Orders_Sync")
foreach ($taskName in $taskNames) {
    $task = Get-ScheduledTask -TaskPath "\DB_MP\" -TaskName $taskName -ErrorAction SilentlyContinue
    if ($null -eq $task) {
        Write-Host "MISSING \DB_MP\$taskName"
        continue
    }

    Write-Host "TASK \DB_MP\$taskName"
    $action = $task.Actions | Select-Object -First 1
    Write-Host "Execute=$($action.Execute)"
    Write-Host "Arguments=$($action.Arguments)"
    Write-Host "WorkingDirectory=$($action.WorkingDirectory)"

    $xml = [xml](Export-ScheduledTask -TaskPath "\DB_MP\" -TaskName $taskName)
    foreach ($trigger in $xml.Task.Triggers.ChildNodes) {
        Write-Host "TriggerType=$($trigger.Name)"
        Write-Host "StartBoundary=$($trigger.StartBoundary)"
        Write-Host "Interval=$($trigger.Repetition.Interval)"
        Write-Host "Duration=$($trigger.Repetition.Duration)"
    }
    Write-Host "MultipleInstances=$($xml.Task.Settings.MultipleInstancesPolicy)"
    Write-Host "ExecutionTimeLimit=$($xml.Task.Settings.ExecutionTimeLimit)"
    Write-Host "StartWhenAvailable=$($xml.Task.Settings.StartWhenAvailable)"
    Write-Host "RestartCount=$($xml.Task.Settings.RestartOnFailure.Count)"
    Write-Host "RestartInterval=$($xml.Task.Settings.RestartOnFailure.Interval)"

    $arguments = [string]$action.Arguments
    $marker = "\scripts\"
    $markerIndex = $arguments.IndexOf($marker, [System.StringComparison]::OrdinalIgnoreCase)
    if ($markerIndex -ge 0) {
        $beforeScripts = $arguments.Substring(0, $markerIndex)
        $quoteIndex = $beforeScripts.LastIndexOf('"')
        if ($quoteIndex -ge 0) {
            $root = $beforeScripts.Substring($quoteIndex + 1)
            Write-Host "ProjectRoot=$root"
            $scriptsDir = Join-Path $root "scripts"
            if (Test-Path $scriptsDir) {
                Get-ChildItem $scriptsDir -Filter "*.cmd" | Select-Object Name,Length | Format-Table -AutoSize
            } else {
                Write-Host "ScriptsDirMissing=$scriptsDir"
            }
        }
    }
}
