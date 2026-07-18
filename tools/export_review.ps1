$ErrorActionPreference = "Stop"

$root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$out = Join-Path $PSScriptRoot "PROJECT_REVIEW.md"

$excludeDirs = @(
  ".git", ".venv", "__pycache__", ".pytest_cache", ".mypy_cache",
  "node_modules", ".idea", ".vscode",
  "infra\pg_data", "infra\pgdata", "infra\data", "pg_data", "data",
  "logs"
)

$allowExt   = @(".py",".ps1",".bat",".cmd",".vbs",".yml",".yaml",".json",".md",".txt",".ini",".cfg",".toml",".dockerignore",".gitignore")
$allowNames = @("Dockerfile","docker-compose.yml","docker-compose.yaml","requirements.txt","README.md")

$excludeFileMasks = @(
  ".env", ".env.*",
  "PROJECT_REVIEW.md", "export_review_v2.ps1",
  "*.log","*.zip","*.7z","*.rar",
  "*.exe","*.dll","*.pdb",
  "*.png","*.jpg","*.jpeg","*.gif","*.ico",
  "*.pdf","*.mp4","*.mov",
  "*.db","*.sqlite","*.bak",
  "*.sql"
)

function Is-ExcludedPath([string]$fullPath) {
  $rel = $fullPath.Substring($root.Length).TrimStart('\')
  foreach ($d in $excludeDirs) {
    if ($rel -like "$d\*" -or $rel -eq $d) { return $true }
  }
  return $false
}

function Is-AllowedFile([System.IO.FileInfo]$f) {
  $name = $f.Name
  foreach ($mask in $excludeFileMasks) { if ($name -like $mask) { return $false } }
  if ($allowNames -contains $name) { return $true }
  $ext = [IO.Path]::GetExtension($name).ToLowerInvariant()
  return ($allowExt -contains $ext)
}

function Get-LangByExt([string]$path) {
  switch ([IO.Path]::GetExtension($path).ToLowerInvariant()) {
    ".py"   { "python" }
    ".ps1"  { "powershell" }
    ".bat"  { "bat" }
    ".cmd"  { "bat" }
    ".vbs"  { "vbscript" }
    ".json" { "json" }
    ".yml"  { "yaml" }
    ".yaml" { "yaml" }
    ".md"   { "markdown" }
    default { "" }
  }
}

function Read-TextSmart([string]$path) {
  $bytes = [System.IO.File]::ReadAllBytes($path)
  if ($bytes.Length -eq 0) { return @{ Ok=$true; Text="" } }
  $sample = $bytes[0..([Math]::Min($bytes.Length-1,4095))]
  if ($sample -contains 0) { return @{ Ok=$false; Reason="binary-NUL"; Text=$null } }
  if ($bytes.Length -ge 2 -and $bytes[0] -eq 0xFF -and $bytes[1] -eq 0xFE) {
    return @{ Ok=$true; Text=([System.Text.Encoding]::Unicode).GetString($bytes) }
  }
  if ($bytes.Length -ge 2 -and $bytes[0] -eq 0xFE -and $bytes[1] -eq 0xFF) {
    return @{ Ok=$true; Text=([System.Text.Encoding]::BigEndianUnicode).GetString($bytes) }
  }
  try {
    $utf8 = New-Object System.Text.UTF8Encoding($false,$true)
    return @{ Ok=$true; Text=$utf8.GetString($bytes) }
  } catch {
    $cp1251 = [System.Text.Encoding]::GetEncoding(1251)
    return @{ Ok=$true; Text=$cp1251.GetString($bytes) }
  }
}

@(
  "# Project review bundle"
  ""
  "Root: $root"
  "Generated: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')"
  ""
  "## File list"
  ""
) | Set-Content -Path $out -Encoding UTF8

$files = Get-ChildItem -Path $root -Recurse -File -Force -ErrorAction SilentlyContinue |
  Where-Object { -not (Is-ExcludedPath $_.FullName) } |
  Where-Object { Is-AllowedFile $_ } |
  Sort-Object FullName

$files |
  ForEach-Object { "- " + $_.FullName.Substring($root.Length).TrimStart('\') } |
  Add-Content -Path $out -Encoding UTF8

Add-Content -Path $out -Encoding UTF8 -Value "`n## Files content`n"

foreach ($f in $files) {
  $rel  = $f.FullName.Substring($root.Length).TrimStart('\')
  $lang = Get-LangByExt $f.FullName
  Add-Content -Path $out -Encoding UTF8 -Value "`n---`n### $rel`n"
  $r = Read-TextSmart $f.FullName
  if (-not $r.Ok) {
    Add-Content -Path $out -Encoding UTF8 -Value "_Skipped: $($r.Reason)_`n"
    continue
  }
  Add-Content -Path $out -Encoding UTF8 -Value ('```' + $lang)
  Add-Content -Path $out -Encoding UTF8 -Value $r.Text
  Add-Content -Path $out -Encoding UTF8 -Value '```'
}

Write-Host "OK: files=$($files.Count) out=$out"
