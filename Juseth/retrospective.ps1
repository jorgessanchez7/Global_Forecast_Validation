# retrospective.ps1
$ErrorActionPreference = "Stop"

# --- RUTAS ---
$PYTHON  = "C:\Users\jsanchez\AppData\Local\anaconda3\envs\glofas\python.exe"
$SCRIPT  = "C:\Users\jsanchez\Documents\github\Global_Forecast_Validation\Juseth\extract_row_values_v2_Alvaro.py"
$WORKDIR = Split-Path $SCRIPT
$LOG_DIR = "C:\Users\jsanchez\Downloads\profesor_Alvaro\Simulated_Data\GEOGLOWS_v2\Logs"

# --- COMPROBACIONES ---
if (-not (Test-Path $PYTHON)) { throw "No se encontró PYTHON en: $PYTHON" }
if (-not (Test-Path $SCRIPT)) { throw "No se encontró SCRIPT en: $SCRIPT" }
New-Item -ItemType Directory -Path $LOG_DIR -Force | Out-Null

# --- RANGOS [start, finish) ---
$ranges = @(
    @(0, 200),
    @(200, 400)
)

$procs = @()

foreach ($r in $ranges) {
    $start  = $r[0]
    $finish = $r[1]

    $logOut = Join-Path $LOG_DIR ("retrospective_{0}_{1}.out.log" -f $start, $finish)
    $logErr = Join-Path $LOG_DIR ("retrospective_{0}_{1}.err.log" -f $start, $finish)

    # -u = unbuffered (stdout/stderr inmediatos en los logs)
    $args = "-u `"$SCRIPT`" --start $start --finish $finish"

    Write-Host ("Launching: {0} {1}" -f $PYTHON, $args)
    Write-Host ("  stdout -> {0}" -f $logOut)
    Write-Host ("  stderr -> {0}" -f $logErr)

    $p = Start-Process -FilePath $PYTHON `
        -ArgumentList $args `
        -WorkingDirectory $WORKDIR `
        -RedirectStandardOutput $logOut `
        -RedirectStandardError  $logErr `
        -NoNewWindow `
        -PassThru

    $procs += $p
}

Write-Host "All jobs launched. Waiting..."
$procs | ForEach-Object { $_.WaitForExit() }
Write-Host "All jobs finished."
