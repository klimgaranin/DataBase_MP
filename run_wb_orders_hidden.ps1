# C:\marketplace-etl\run_wb_orders_hidden.ps1
$ErrorActionPreference = "Stop"

$notifyLog = "C:\marketplace-etl\notify_wb_orders.log"
$cmd = "C:\marketplace-etl\run_wb_orders.cmd"

function Write-NotifyLog {
  param([string]$Message)
  $ts = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")
  Add-Content -Path $notifyLog -Encoding UTF8 -Value "$ts $Message"
}

function Show-Toast {
  param(
    [string]$Title,
    [string]$Text
  )

  try {
    # Пример создания Toast через Windows Runtime типы [web:557][web:562]
    $xml = @"
<toast>
  <visual>
    <binding template="ToastGeneric">
      <text>$Title</text>
      <text>$Text</text>
    </binding>
  </visual>
</toast>
"@

    $doc = [Windows.Data.Xml.Dom.XmlDocument, Windows.Data.Xml.Dom.XmlDocument, ContentType = WindowsRuntime]::New()
    $doc.LoadXml($xml)

    # AppId для PowerShell, чтобы CreateToastNotifier мог показать уведомление [web:557][web:560]
    $appId = '{1AC14E77-02E7-4E5D-B744-2EB1AE5198B7}\WindowsPowerShell\v1.0\powershell.exe'

    [Windows.UI.Notifications.ToastNotificationManager, Windows.UI.Notifications, ContentType = WindowsRuntime]::CreateToastNotifier($appId).Show($doc)
  }
  catch {
    # Если Toast по какой-то причине не показался — это не должно ломать задачу
    Write-NotifyLog ("TOAST_FAIL: " + $_.Exception.Message)
  }
}

$sw = [System.Diagnostics.Stopwatch]::StartNew()

Write-NotifyLog "START"
Show-Toast -Title "WB Orders" -Text "Старт синхронизации"

try {
  & cmd.exe /c "`"$cmd`""
  $exit = $LASTEXITCODE

  $sw.Stop()

  if ($exit -eq 0) {
    Write-NotifyLog ("END OK in {0:n0}s" -f $sw.Elapsed.TotalSeconds)
    Show-Toast -Title "WB Orders" -Text ("Готово OK за {0:n0} сек." -f $sw.Elapsed.TotalSeconds)
    exit 0
  } else {
    Write-NotifyLog ("END FAIL exit={0} in {1:n0}s" -f $exit, $sw.Elapsed.TotalSeconds)
    Show-Toast -Title "WB Orders" -Text ("Ошибка. Код {0}. {1:n0} сек." -f $exit, $sw.Elapsed.TotalSeconds)
    exit $exit
  }
}
catch {
  $sw.Stop()
  Write-NotifyLog ("END EXCEPTION in {0:n0}s: {1}" -f $sw.Elapsed.TotalSeconds, $_.Exception.Message)
  Show-Toast -Title "WB Orders" -Text ("Исключение: " + $_.Exception.Message)
  exit 1
}
