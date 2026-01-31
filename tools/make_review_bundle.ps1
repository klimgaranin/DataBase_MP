param(
  [string]$Root = "",
  [string]$OutDir = "_review",
  [int64]$MaxFileBytes = 512KB
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function New-Dir([string]$path) {
  if (-not (Test-Path -LiteralPath $path)) {
    New-Item -ItemType Directory -Path $path -Force | Out-Null
  }
}

function Set-TextUtf8NoBom([string]$path, [string]$text) {
  $utf8NoBom = [System.Text.UTF8Encoding]::new($false)
  [System.IO.File]::WriteAllText($path, $text, $utf8NoBom)
}

function Add-TextUtf8NoBom([string]$path, [string]$text) {
  $utf8NoBom = [System.Text.UTF8Encoding]::new($false)
  [System.IO.File]::AppendAllText($path, $text, $utf8NoBom)
}

function Get-LangTag([string]$path) {
  switch ([IO.Path]::GetExtension($path).ToLowerInvariant()) {
    ".py"   { "python" }
    ".ps1"  { "powershell" }
    ".cmd"  { "bat" }
    ".bat"  { "bat" }
    ".vbs"  { "vbscript" }
    ".yml"  { "yaml" }
    ".yaml" { "yaml" }
    ".sql"  { "sql" }
    ".md"   { "markdown" }
    ".txt"  { "txt" }
    ".json" { "json" }
    default { "" }
  }
}

function Test-HasNulByte([byte[]]$bytes) {
  foreach ($b in $bytes) { if ($b -eq 0) { return $true } }
  return $false
}

function ConvertFrom-BytesSmart([byte[]]$bytes) {
  $utf8Strict = [System.Text.UTF8Encoding]::new($false, $true)
  try {
    return $utf8Strict.GetString($bytes)
  } catch {
    $cp1251 = [System.Text.Encoding]::GetEncoding(1251)
    return $cp1251.GetString($bytes)
  }
}

function ConvertTo-RelNorm([string]$rel) {
  return ($rel -replace "\\", "/").TrimStart("/")
}

# Root берём от местоположения скрипта, чтобы не зависеть от кириллицы в литералах
if ([string]::IsNullOrWhiteSpace($Root)) {
  $Root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
} else {
  $Root = (Resolve-Path $Root).Path
}

$excludeDirs = @(
  ".git", ".venv", "__pycache__", ".pytest_cache", ".mypy_cache",
  "node_modules", ".idea", ".vscode",
  "pgdata", "data", "logs", $OutDir,
  "migrations/__pycache__", "tools/__pycache__"
) | ForEach-Object { ConvertTo-RelNorm $_ }

$excludePathMasks = @(
  ".env", ".env.*",

  "*.pyc", "*.pyo", "*.pyd",

  "*.log", "*.stderr.txt",
  "logs_*.txt", "log*.txt", "notify_*.txt", "notify_*.log",
  "*для чата*.txt", "*после двух дней*.txt",

  "backup_*.sql", "*.dump", "*.bak",

  "*.zip", "*.7z", "*.rar",
  "*.png", "*.jpg", "*.jpeg", "*.gif", "*.ico",
  "*.pdf", "*.mp4", "*.mov",

  "PROJECT_REVIEW.md", "CHECKSUMS.sha256"
)

$allowExt = @(
  ".py", ".ps1", ".cmd", ".bat", ".vbs",
  ".yml", ".yaml", ".json",
  ".sql",
  ".md",
  ".txt"
)

function Get-RelPath([string]$fullPath) {
  $rel = $fullPath.Substring($Root.Length).TrimStart("\", "/")
  return ConvertTo-RelNorm $rel
}

function Test-ExcludedRelPath([string]$relNorm) {
  foreach ($d in $excludeDirs) {
    if ($relNorm -ieq $d -or $relNorm.StartsWith($d + "/")) { return $true }
  }
  return $false
}

function Test-ExcludedByMask([string]$relNorm) {
  $name = [IO.Path]::GetFileName($relNorm)
  foreach ($m in $excludePathMasks) {
    if ($name -like $m) { return $true }
    if ($relNorm -like $m) { return $true }
  }
  return $false
}

function Test-AllowedExt([string]$fullPath) {
  $ext = [IO.Path]::GetExtension($fullPath).ToLowerInvariant()
  return $allowExt -contains $ext
}

if (-not (Test-Path -LiteralPath $Root)) {
  throw "Root folder not found: $Root"
}

$absOutDir = Join-Path $Root $OutDir
New-Dir $absOutDir

$outBundle = Join-Path $absOutDir "PROJECT_REVIEW.md"
$outSums   = Join-Path $absOutDir "CHECKSUMS.sha256"

$files = Get-ChildItem -Path $Root -Recurse -File -Force | ForEach-Object {
  $relNorm = Get-RelPath $_.FullName
  [PSCustomObject]@{
    FullName = $_.FullName
    RelNorm  = $relNorm
    Length   = $_.Length
  }
} | Where-Object {
  -not (Test-ExcludedRelPath $_.RelNorm)
} | Where-Object {
  -not (Test-ExcludedByMask $_.RelNorm)
} | Where-Object {
  Test-AllowedExt $_.FullName
} | Sort-Object RelNorm

$fence = '```'

$header =
"# Project review bundle`n`n" +
"Root: $Root`n" +
"Generated: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')`n" +
"MaxFileBytes: $MaxFileBytes`n`n" +
"## File list`n`n"

Set-TextUtf8NoBom $outBundle $header

foreach ($f in $files) {
  Add-TextUtf8NoBom $outBundle ("- " + $f.RelNorm + "`n")
}

Add-TextUtf8NoBom $outBundle "`n## Files content`n"

Set-TextUtf8NoBom $outSums "# SHA256 checksums (relative_path  sha256)`n"
foreach ($f in $files) {
  $hash = (Get-FileHash -Algorithm SHA256 -LiteralPath $f.FullName).Hash.ToLowerInvariant()
  Add-TextUtf8NoBom $outSums ("{0}  {1}`n" -f $f.RelNorm, $hash)
}

foreach ($f in $files) {
  $lang = Get-LangTag $f.FullName

  Add-TextUtf8NoBom $outBundle ("`n---`n### " + $f.RelNorm + "`n`n")

  if ($f.Length -gt $MaxFileBytes) {
    Add-TextUtf8NoBom $outBundle ("SKIPPED (too large): " + $f.Length + " bytes (limit=" + $MaxFileBytes + ")`n")
    continue
  }

  $bytes = [System.IO.File]::ReadAllBytes($f.FullName)
  if (Test-HasNulByte $bytes) {
    Add-TextUtf8NoBom $outBundle "SKIPPED (binary file detected: NUL byte)`n"
    continue
  }

  Add-TextUtf8NoBom $outBundle ($fence + $lang + "`n")
  $text = ConvertFrom-BytesSmart $bytes
  $text = ($text -replace "`r`n", "`n").TrimEnd()
  Add-TextUtf8NoBom $outBundle ($text + "`n")
  Add-TextUtf8NoBom $outBundle ($fence + "`n")
}

Write-Host "OK generated:"
Write-Host $outBundle
Write-Host $outSums
Write-Host ("Files: " + $files.Count)
