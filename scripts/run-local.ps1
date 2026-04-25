param(
  [string]$EnvFile = ".env",
  [string]$PgDsn = "",
  [string]$PgReadOnlyDsn = "",
  [string]$LlmProvider = "",
  [switch]$AllowPortFallback,
  [switch]$StopExisting
)

$root = Split-Path -Parent $PSScriptRoot
$resolvedEnvFile = Join-Path $root $EnvFile
$binDir = Join-Path $root ".bin"
$hostArch = if ($env:PROCESSOR_ARCHITECTURE -eq "ARM64") { "arm64" } else { "amd64" }

function Read-DotEnv([string]$Path) {
  $result = @{}
  if (-not (Test-Path -LiteralPath $Path)) {
    return $result
  }

  foreach ($line in Get-Content -LiteralPath $Path -Encoding UTF8) {
    $trimmed = $line.Trim()
    if ([string]::IsNullOrWhiteSpace($trimmed) -or $trimmed.StartsWith("#")) {
      continue
    }

    if ($trimmed.StartsWith("export ")) {
      $trimmed = $trimmed.Substring(7).Trim()
    }

    $parts = $trimmed -split "=", 2
    if ($parts.Count -ne 2) {
      continue
    }

    $key = $parts[0].Trim()
    $value = $parts[1].Trim().Trim("'`"")
    if (-not [string]::IsNullOrWhiteSpace($key)) {
      $result[$key] = $value
    }
  }

  return $result
}

function Escape-SingleQuotes([string]$Value) {
  return $Value.Replace("'", "''")
}

function Get-PortOwner([int]$Port) {
  $connection = Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue | Select-Object -First 1
  if ($null -eq $connection) {
    return $null
  }

  $processName = ""
  $processPath = ""
  try {
    $process = Get-Process -Id $connection.OwningProcess -ErrorAction Stop
    $processName = $process.ProcessName
    $processPath = $process.Path
  } catch {
    $processName = "PID $($connection.OwningProcess)"
  }

  return @{
    Port = $Port
    Process = $processName
    Pid = $connection.OwningProcess
    Path = $processPath
  }
}

function Test-IsProjectProcess([hashtable]$Owner, [string]$ProjectRoot, [string[]]$ServiceNames) {
  if ($null -eq $Owner) {
    return $false
  }

  $normalizedNames = $ServiceNames | ForEach-Object { $_.ToLowerInvariant() }
  $processName = [string]$Owner.Process
  if (-not [string]::IsNullOrWhiteSpace($processName) -and $normalizedNames -contains $processName.ToLowerInvariant()) {
    return $true
  }

  $processPath = [string]$Owner.Path
  if ([string]::IsNullOrWhiteSpace($processPath)) {
    return $false
  }

  try {
    $resolvedProjectRoot = [System.IO.Path]::GetFullPath($ProjectRoot).TrimEnd('\') + '\'
    $resolvedProcessPath = [System.IO.Path]::GetFullPath($processPath)
    return $resolvedProcessPath.StartsWith($resolvedProjectRoot, [System.StringComparison]::OrdinalIgnoreCase)
  } catch {
    return $false
  }
}

function Get-FreePort([int]$PreferredPort, [int[]]$ReservedPorts) {
  $candidate = $PreferredPort
  while ($candidate -lt 65535) {
    if ($ReservedPorts -contains $candidate) {
      $candidate++
      continue
    }

    $owner = Get-PortOwner $candidate
    if ($null -eq $owner) {
      return $candidate
    }

    $candidate++
  }

  throw "Failed to find a free TCP port starting from $PreferredPort"
}

function Get-ServicePort([array]$Services, [string]$Name) {
  $match = $Services | Where-Object { $_.Name -eq $Name } | Select-Object -First 1
  if ($null -eq $match -or $null -eq $match.Port) {
    throw "Failed to resolve port for service '$Name'"
  }

  return [int]$match.Port
}

$envMap = Read-DotEnv $resolvedEnvFile

if (-not [string]::IsNullOrWhiteSpace($PgDsn)) {
  $envMap["PG_DSN"] = $PgDsn
}
if (-not [string]::IsNullOrWhiteSpace($PgReadOnlyDsn)) {
  $envMap["PG_READONLY_DSN"] = $PgReadOnlyDsn
}
if (-not [string]::IsNullOrWhiteSpace($LlmProvider)) {
  $envMap["LLM_PROVIDER"] = $LlmProvider
}

if (-not $envMap.ContainsKey("PG_DSN")) {
  $envMap["PG_DSN"] = "postgres://postgres:postgres@localhost:5432/drivee_analytics?sslmode=disable"
}
if (-not $envMap.ContainsKey("PG_READONLY_DSN")) {
  $envMap["PG_READONLY_DSN"] = "postgres://analytics_readonly:analytics_demo@localhost:5432/drivee_analytics?sslmode=disable"
}
if (-not $envMap.ContainsKey("LLM_PROVIDER")) {
  $envMap["LLM_PROVIDER"] = "qwen"
}
if (-not $envMap.ContainsKey("LLM_SETTINGS_FILE")) {
  $envMap["LLM_SETTINGS_FILE"] = "config/qwen_sql_settings.md"
}
if (-not $envMap.ContainsKey("CEREBRAS_MODEL")) {
  $envMap["CEREBRAS_MODEL"] = "qwen-3-235b-a22b-instruct-2507"
}
if (-not $envMap.ContainsKey("CEREBRAS_CHAT_URL")) {
  $envMap["CEREBRAS_CHAT_URL"] = "https://api.cerebras.ai/v1/chat/completions"
}

$services = @(
  @{ Name = "auth";    DefaultPort = 8085; Package = "./cmd/auth" },
  @{ Name = "chat";    DefaultPort = 8086; Package = "./cmd/chat" },
  @{ Name = "meta";    DefaultPort = 8084; Package = "./cmd/meta" },
  @{ Name = "llm";     DefaultPort = 8082; Package = "./cmd/llm" },
  @{ Name = "query";   DefaultPort = 8081; Package = "./cmd/query" },
  @{ Name = "reports"; DefaultPort = 8083; Package = "./cmd/reports" },
  @{ Name = "gateway"; DefaultPort = 8080; Package = "./cmd/gateway" }
)

if ($StopExisting) {
  $servicePorts = $services | ForEach-Object { [int]$_.DefaultPort }
  $serviceNames = $services | ForEach-Object { [string]$_.Name }
  $connections = foreach ($port in $servicePorts) {
    Get-PortOwner $port
  }

  $projectOwners = $connections |
    Where-Object { $_ -ne $null -and (Test-IsProjectProcess -Owner $_ -ProjectRoot $root -ServiceNames $serviceNames) }

  $externalOwners = $connections |
    Where-Object { $_ -ne $null -and -not (Test-IsProjectProcess -Owner $_ -ProjectRoot $root -ServiceNames $serviceNames) }

  $pidsToStop = $projectOwners |
    ForEach-Object { $_["Pid"] } |
    Where-Object { $_ -ne $null } |
    Select-Object -Unique

  foreach ($owner in $externalOwners) {
    Write-Warning ("Port {0} is occupied by external process {1} (PID {2}); it will not be stopped automatically." -f $owner.Port, $owner.Process, $owner.Pid)
  }

  foreach ($processId in $pidsToStop) {
    try {
      $proc = Get-Process -Id $processId -ErrorAction Stop
      Write-Host "Stopping $($proc.ProcessName) (PID $processId)..."
      Stop-Process -Id $processId -Force -ErrorAction Stop
    } catch {
      Write-Warning "Failed to stop PID ${processId}: $($_.Exception.Message)"
    }
  }

  Start-Sleep -Milliseconds 500
}

$busyPorts = @()
foreach ($service in $services) {
  $preferredPort = [int]$service.DefaultPort
  $owner = Get-PortOwner $preferredPort
  if ($null -eq $owner) {
    $service["Port"] = $preferredPort
    continue
  }

  $busyPorts += @{
    Name = $service.Name
    PreferredPort = $preferredPort
    Process = $owner.Process
    Pid = $owner.Pid
    Path = $owner.Path
  }
}

if ($busyPorts.Count -gt 0 -and -not $AllowPortFallback -and -not $StopExisting) {
  Write-Error "Required service ports are busy. Refusing to start with alternate ports because that can make you open an old frontend from another process."
  foreach ($entry in $busyPorts) {
    Write-Error ("  {0}: port {1} is busy by {2} (PID {3})." -f $entry.Name, $entry.PreferredPort, $entry.Process, $entry.Pid)
  }
  Write-Host ""
  Write-Host "Resolve it in one of these ways:"
  Write-Host "  1. Re-run with -StopExisting if those ports are occupied by a previous local project run."
  Write-Host "  2. Stop the conflicting process manually and re-run."
  Write-Host "  3. Re-run with -AllowPortFallback and open the exact gateway URL printed by this script."
  exit 1
}

$reservedPorts = @()
foreach ($service in $services) {
  $preferredPort = [int]$service.DefaultPort
  $busyEntry = $busyPorts | Where-Object { $_.Name -eq $service.Name } | Select-Object -First 1
  if ($null -eq $busyEntry) {
    $service["Port"] = $preferredPort
    $reservedPorts += $preferredPort
    continue
  }

  $resolvedPort = Get-FreePort -PreferredPort ($preferredPort + 1) -ReservedPorts $reservedPorts
  $service["Port"] = $resolvedPort
  $reservedPorts += $resolvedPort
  $busyEntry["ChosenPort"] = $resolvedPort
}

if ($busyPorts.Count -gt 0) {
  if ($StopExisting -and -not $AllowPortFallback) {
    Write-Warning "Some default service ports are still busy after -StopExisting. Alternate free ports will be used for this run."
  } else {
    Write-Warning "Some default service ports are busy. Alternate free ports will be used for this run:"
  }
  foreach ($entry in $busyPorts) {
    Write-Warning ("  {0}: {1} is busy by {2} (PID {3}); using {4} instead." -f $entry.Name, $entry.PreferredPort, $entry.Process, $entry.Pid, $entry.ChosenPort)
  }
}

New-Item -ItemType Directory -Force -Path $binDir | Out-Null

foreach ($service in $services) {
  $outputPath = Join-Path $binDir ($service.Name + ".exe")
  Write-Host "Building $($service.Name)..."
  $buildEnv = @{
    GOOS   = "windows"
    GOARCH = $hostArch
    CGO_ENABLED = "0"
  }
  $previousValues = @{}
  foreach ($entry in $buildEnv.GetEnumerator()) {
    $previousValues[$entry.Key] = [Environment]::GetEnvironmentVariable($entry.Key, "Process")
    [Environment]::SetEnvironmentVariable($entry.Key, $entry.Value, "Process")
  }

  try {
    & go build -o $outputPath $service.Package
  } finally {
    foreach ($entry in $buildEnv.GetEnumerator()) {
      [Environment]::SetEnvironmentVariable($entry.Key, $previousValues[$entry.Key], "Process")
    }
  }

  if ($LASTEXITCODE -ne 0) {
    throw "Failed to build $($service.Name)"
  }
  $service["Executable"] = $outputPath
}

$serviceUrls = @{
  query = 'http://localhost:{0}' -f (Get-ServicePort -Services $services -Name "query")
  llm = 'http://localhost:{0}' -f (Get-ServicePort -Services $services -Name "llm")
  reports = 'http://localhost:{0}' -f (Get-ServicePort -Services $services -Name "reports")
  meta = 'http://localhost:{0}' -f (Get-ServicePort -Services $services -Name "meta")
  auth = 'http://localhost:{0}' -f (Get-ServicePort -Services $services -Name "auth")
  chat = 'http://localhost:{0}' -f (Get-ServicePort -Services $services -Name "chat")
}

foreach ($service in $services) {
  $envAssignments = foreach ($entry in $envMap.GetEnumerator()) {
    '$env:{0} = ''{1}''' -f $entry.Key, (Escape-SingleQuotes $entry.Value)
  }

  $scriptLines = @(
    "Set-Location '$root'"
    $envAssignments
    '$env:QUERY_SERVICE_URL = ''{0}''' -f $serviceUrls.query
    '$env:LLM_SERVICE_URL = ''{0}''' -f $serviceUrls.llm
    '$env:REPORTS_SERVICE_URL = ''{0}''' -f $serviceUrls.reports
    '$env:META_SERVICE_URL = ''{0}''' -f $serviceUrls.meta
    '$env:AUTH_SERVICE_URL = ''{0}''' -f $serviceUrls.auth
    '$env:CHAT_SERVICE_URL = ''{0}''' -f $serviceUrls.chat
    '$env:PORT = ''{0}''' -f $service.Port
    '& ''{0}''' -f (Escape-SingleQuotes $service.Executable)
  )

  $script = [string]::Join([Environment]::NewLine, $scriptLines)
  $encodedScript = [Convert]::ToBase64String([Text.Encoding]::Unicode.GetBytes($script))
  Start-Process powershell -ArgumentList "-NoExit", "-EncodedCommand", $encodedScript | Out-Null
}

$gatewayPort = Get-ServicePort -Services $services -Name "gateway"

Write-Host "Drivee Analytics services are starting in separate PowerShell windows."
Write-Host "Environment file: $resolvedEnvFile"
Write-Host "Gateway URL: http://localhost:$gatewayPort"
Write-Host "Open the gateway URL above after the services finish booting."
