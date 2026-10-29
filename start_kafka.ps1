<#
.SYNOPSIS
    Starts Apache Kafka 3.7.0 in KRaft mode (no ZooKeeper).
    Run this script in a dedicated PowerShell window BEFORE starting the producer/consumer.

.NOTES
    - First run: formats the KRaft storage (creates a cluster UUID).
    - Subsequent runs: detects that storage is already formatted and skips formatting.
    - Logs are written to C:\tmp\kraft-combined-logs by default.
#>

$KAFKA_HOME = "C:\Kafka\kafka_2.13-3.7.0"
$KAFKA_BIN  = "$KAFKA_HOME\bin\windows"
$KRAFT_CFG  = "$KAFKA_HOME\config\kraft\server.properties"
$LOG_DIR    = "C:\tmp\kraft-combined-logs"

# Ensure valid JAVA_HOME
if (-not (Test-Path "$env:JAVA_HOME\bin\java.exe")) {
    if (Test-Path "C:\Program Files\Java\jdk-24\bin\java.exe") {
        $env:JAVA_HOME = "C:\Program Files\Java\jdk-24"
    }
}

# Set Kafka heap options to avoid deprecated wmic call on modern Windows
if (-not $env:KAFKA_HEAP_OPTS) {
    $env:KAFKA_HEAP_OPTS = "-Xmx1G -Xms1G"
}

Write-Host "=== Kafka 3.7.0 KRaft Startup ===" -ForegroundColor Cyan

# -- Check if storage needs formatting -----------------------------------------
$metaFile = "$LOG_DIR\meta.properties"
if (-not (Test-Path $metaFile)) {
    Write-Host "[1/2] Formatting KRaft storage ..." -ForegroundColor Yellow
    $uuid = & "$KAFKA_BIN\kafka-storage.bat" random-uuid
    Write-Host "      Cluster UUID: $uuid" -ForegroundColor Gray
    & "$KAFKA_BIN\kafka-storage.bat" format -t $uuid -c $KRAFT_CFG
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Storage format failed. Check the config at $KRAFT_CFG"
        exit 1
    }
} else {
    Write-Host "[1/2] KRaft storage already formatted - skipping format step." -ForegroundColor Green
}

# -- Start the broker -----------------------------------------------------------
Write-Host "[2/2] Starting Kafka broker ... (Ctrl+C to stop)" -ForegroundColor Yellow
& "$KAFKA_BIN\kafka-server-start.bat" $KRAFT_CFG
