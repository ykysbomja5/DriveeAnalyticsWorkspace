param(
    [Parameter(Mandatory = $true)]
    [string]$Database,

    [Parameter(Mandatory = $true)]
    [string]$IncityCsv,

    [Parameter(Mandatory = $true)]
    [string]$DriverDetailCsv,

    [Parameter(Mandatory = $true)]
    [string]$PassDetailCsv,

    [string]$User = "postgres",
    [string]$HostName = "localhost",
    [int]$Port = 5432
)

$ErrorActionPreference = "Stop"

function Convert-ToPsqlPath {
    param([string]$PathValue)
    return (Resolve-Path -LiteralPath $PathValue).Path.Replace("\", "/").Replace("'", "''")
}

$scriptPath = Join-Path $PSScriptRoot "import-new-datasets.sql"
$incityPath = Convert-ToPsqlPath $IncityCsv
$driverPath = Convert-ToPsqlPath $DriverDetailCsv
$passPath = Convert-ToPsqlPath $PassDetailCsv
$tempScript = Join-Path ([System.IO.Path]::GetTempPath()) ("drivee-import-new-datasets-{0}.sql" -f ([System.Guid]::NewGuid().ToString("N")))

try {
    $sql = Get-Content -LiteralPath $scriptPath -Raw
    $sql = $sql.Replace("D:/Download/incity.csv", $incityPath)
    $sql = $sql.Replace("D:/Download/driver_detail.csv", $driverPath)
    $sql = $sql.Replace("D:/Download/pass_detail.csv", $passPath)
    Set-Content -LiteralPath $tempScript -Value $sql -Encoding UTF8

    psql `
        --host $HostName `
        --port $Port `
        --username $User `
        --dbname $Database `
        --file $tempScript
}
finally {
    if (Test-Path -LiteralPath $tempScript) {
        Remove-Item -LiteralPath $tempScript -Force
    }
}
