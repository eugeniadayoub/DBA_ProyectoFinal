param()

# PowerShell script para descargar el ZIP MGN desde OneDrive
# Uso: Set-Item -Path env:ONEDRIVE_URL -Value "<link>"; .\download_mgn.ps1

$out = "C:\app\MGN2024_00_COLOMBIA.zip"

if (Test-Path $out) {
    Write-Host "Download skipped: $out ya existe."
    exit 0
}

$url = $env:ONEDRIVE_URL
if (-not $url) {
    $txt = "C:\app\onedrive_url.txt"
    if (Test-Path $txt) {
        $url = Get-Content $txt -Raw
    }
}

if (-not $url) {
    Write-Error "ERROR: no se encontró URL de descarga. Define la variable de entorno ONEDRIVE_URL o crea C:\app\onedrive_url.txt con la URL compartida de OneDrive."
    exit 2
}

Write-Host "Descargando MGN ZIP desde: $url"
Write-Host "-> destino: $out"

try {
    Invoke-WebRequest -Uri $url -OutFile $out -UseBasicParsing -ErrorAction Stop
    $fi = Get-Item $out
    Write-Host "Descarga finalizada: $($fi.Length) bytes descargados."
} catch {
    Write-Error "ERROR: la descarga falló: $_"
    exit 3
}

exit 0
