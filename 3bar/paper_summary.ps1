$ErrorActionPreference = "Stop"

$tradesPath = Join-Path $PSScriptRoot "reports\live\paper_trades.jsonl"
$statePath = Join-Path $PSScriptRoot "state\live_state.json"

$activeTrade = $null
if (Test-Path $statePath) {
    try {
        $state = Get-Content $statePath -Raw | ConvertFrom-Json
        $activeTrade = $state.active_trade
    }
    catch {
        Write-Output "Warning: failed to read state file: $statePath"
    }
}

$rows = @()
if (Test-Path $tradesPath) {
    $rows = Get-Content $tradesPath |
        Where-Object { $_.Trim() -ne "" } |
        ForEach-Object { $_ | ConvertFrom-Json }
}

Write-Output "3bar paper summary"
Write-Output "trades_file: $tradesPath"
Write-Output "state_file: $statePath"
Write-Output ""

if ($activeTrade) {
    Write-Output "active paper trade:"
    $activeTrade |
        Select-Object symbol,direction,signal_time,entry_time,entry_price,stop_price,quantity,armed_trail,last_bar_time |
        Format-Table -AutoSize
    Write-Output ""
}
else {
    Write-Output "active paper trade: none"
    Write-Output ""
}

if (-not $rows -or $rows.Count -eq 0) {
    Write-Output "closed paper trades: 0"
    Write-Output "net_pnl: 0.00"
    exit 0
}

$totalNet = ($rows | Measure-Object -Property net_pnl -Sum).Sum
$totalGross = ($rows | Measure-Object -Property gross_pnl -Sum).Sum
$wins = ($rows | Where-Object { $_.net_pnl -gt 0 }).Count
$losses = ($rows | Where-Object { $_.net_pnl -lt 0 }).Count
$flat = ($rows | Where-Object { $_.net_pnl -eq 0 }).Count
$count = $rows.Count
$winRate = if ($count -gt 0) { [math]::Round(($wins / $count) * 100, 2) } else { 0 }
$avgNet = if ($count -gt 0) { [math]::Round($totalNet / $count, 4) } else { 0 }

Write-Output "closed paper trades: $count"
Write-Output "wins: $wins"
Write-Output "losses: $losses"
Write-Output "flat: $flat"
Write-Output "win_rate_percent: $winRate"
Write-Output ("gross_pnl: {0:N4}" -f $totalGross)
Write-Output ("net_pnl: {0:N4}" -f $totalNet)
Write-Output ("avg_net_per_trade: {0:N4}" -f $avgNet)
Write-Output ""
Write-Output "last closed paper trades:"

$rows |
    Select-Object time,symbol,direction,entry_time,exit_time,entry_price,exit_price,exit_reason,quantity,net_pnl |
    Select-Object -Last 10 |
    Format-Table -AutoSize
