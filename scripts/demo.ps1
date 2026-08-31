$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
$work = Join-Path $env:TEMP "orbit-demo-$PID"
New-Item -ItemType Directory -Force -Path $work | Out-Null

Push-Location $root
$previousCache = $env:GOCACHE
try {
    $env:GOCACHE = Join-Path $env:TEMP 'orbit-demo-go-cache'
    go build -o (Join-Path $work 'controller.exe') ./cmd/controller
    go build -o (Join-Path $work 'worker.exe') ./cmd/worker
    go build -o (Join-Path $work 'orbit.exe') ./cmd/orbit

    $controller = Start-Process -WindowStyle Hidden -FilePath (Join-Path $work 'controller.exe') -ArgumentList '-addr','127.0.0.1:19000','-worker-timeout','2s' -PassThru
    $workerB = Start-Process -WindowStyle Hidden -FilePath (Join-Path $work 'worker.exe') -ArgumentList '-controller','127.0.0.1:19000','-id','worker-b','-duration','10s' -PassThru
    $workerA = Start-Process -WindowStyle Hidden -FilePath (Join-Path $work 'worker.exe') -ArgumentList '-controller','127.0.0.1:19000','-id','worker-a','-duration','10s' -PassThru
    Start-Sleep -Seconds 2

    & (Join-Path $work 'orbit.exe') submit -controller 127.0.0.1:19000 -id demo-1 -cpu 1 -memory-mb 512
    Stop-Process -Id $workerA.Id
    Start-Sleep -Seconds 1
    & (Join-Path $work 'orbit.exe') status -controller 127.0.0.1:19000 -id demo-1
    Start-Sleep -Seconds 11
    & (Join-Path $work 'orbit.exe') status -controller 127.0.0.1:19000 -id demo-1
}
finally {
    foreach ($process in @($workerA, $workerB, $controller)) {
        if ($null -ne $process -and -not $process.HasExited) { Stop-Process -Id $process.Id -Force }
    }
    $env:GOCACHE = $previousCache
    Pop-Location
    Remove-Item -LiteralPath $work -Recurse -Force
}
