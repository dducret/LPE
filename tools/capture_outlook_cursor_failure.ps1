param(
    [int]$ProcessId = 0,
    [int]$WaitForOutlookSeconds = 300,
    [string]$CdbPath = 'C:\Program Files\WindowsApps\Microsoft.WinDbg_1.2606.22001.0_x64__8wekyb3d8bbwe\amd64\cdb.exe'
)

$ErrorActionPreference = 'Stop'

if (-not (Test-Path -LiteralPath $CdbPath)) {
    throw "cdb.exe was not found at $CdbPath"
}

if ($ProcessId -eq 0) {
    $deadline = (Get-Date).AddSeconds($WaitForOutlookSeconds)
    do {
        $outlook = Get-Process -Name OUTLOOK -ErrorAction SilentlyContinue |
            Sort-Object StartTime -Descending |
            Select-Object -First 1
        if ($outlook) {
            $ProcessId = $outlook.Id
            break
        }
        Start-Sleep -Milliseconds 500
    } while ((Get-Date) -lt $deadline)
}

if ($ProcessId -eq 0) {
    throw "Outlook did not start within $WaitForOutlookSeconds seconds."
}

$stamp = Get-Date -Format 'yyyyMMdd-HHmmss'
$outputDirectory = Join-Path $PSScriptRoot '..\target'
$outputDirectory = [System.IO.Path]::GetFullPath($outputDirectory)
$dumpPath = Join-Path $outputDirectory "outlook-cursor-failure-$stamp.dmp"
$logPath = Join-Path $outputDirectory "outlook-cursor-failure-$stamp.log"
$commandPath = Join-Path $outputDirectory "outlook-cursor-failure-$stamp.cdb"
$dumpPathForCdb = $dumpPath.Replace('\', '/')

$nonFinderCallBreakpoint = @"
.printf \"CURSOR_PROVIDER_CALL path=non_finder explorer=%p provider=%p method=%y flags=0x%08x\\n\", @rsi, @rcx, @rax, @ebx; r @`$t0 = @rax; r @`$t1 = @rcx; r @`$t2 = @rsi; r @`$t3 = @ebx; gc
"@.Trim()

$nonFinderReturnBreakpoint = @"
.printf \"CURSOR_PROVIDER_RETURN path=non_finder hresult=0x%08x explorer=%p provider=%p method=%y flags=0x%08x\\n\", @eax, @`$t2, @`$t1, @`$t0, @`$t3; .if ((@eax & 0x80000000) != 0) { .printf \"CAPTURED_OUTLOOK_CURSOR_FAILURE path=non_finder hresult=0x%08x\\n\", @eax; .dump /ma $dumpPathForCdb; .detach; q } .else { gc }
"@.Trim()

$finderCallBreakpoint = @"
.printf \"CURSOR_PROVIDER_CALL path=finder explorer=%p provider=%p method=%y flags=0x%08x\\n\", @rsi, @rcx, @rax, @ebx; r @`$t0 = @rax; r @`$t1 = @rcx; r @`$t2 = @rsi; r @`$t3 = @ebx; gc
"@.Trim()

$finderReturnBreakpoint = @"
.printf \"CURSOR_PROVIDER_RETURN path=finder hresult=0x%08x explorer=%p provider=%p method=%y flags=0x%08x\\n\", @eax, @`$t2, @`$t1, @`$t0, @`$t3; .if ((@eax & 0x80000000) != 0) { .printf \"CAPTURED_OUTLOOK_CURSOR_FAILURE path=finder hresult=0x%08x\\n\", @eax; .dump /ma $dumpPathForCdb; .detach; q } .else { gc }
"@.Trim()

$cursorReturnBreakpoint = @"
.printf \"OUTLOOK_HRGETCURSOR_RETURN hresult=0x%08x explorer=%p flags=0x%08x\\n\", @eax, @r14, @ebx; .if ((@eax & 0x80000000) != 0) { .printf \"CAPTURED_OUTLOOK_CURSOR_FAILURE path=hr_get_cursor hresult=0x%08x\\n\", @eax; .dump /ma $dumpPathForCdb; .detach; q } .else { gc }
"@.Trim()

$viewDefinitionFailureBreakpoint = @"
.printf \"CAPTURED_OUTLOOK_VIEW_DEFINITION_FAILURE hresult=0x%08x tag=0x%08x caller=%y\\n\", @ecx, @edx, poi(@rsp); kv; .dump /ma $dumpPathForCdb; .detach; q
"@.Trim()

$switchViewFailureBreakpoint = @"
.printf \"CAPTURED_OUTLOOK_SWITCH_VIEW_FAILURE hresult=0x%08x tag=0x%08x caller=%y\\n\", @ecx, @edx, poi(@rsp); kv; .dump /ma $dumpPathForCdb; .detach; q
"@.Trim()

$commands = @(
    '.symfix'
    '.reload /f outlook.exe'
    'sxd av'
    "bp OUTLOOK!ExplorerObject::HrGetCursor+0x82 `"$nonFinderCallBreakpoint`""
    "bp OUTLOOK!ExplorerObject::HrGetCursor+0x88 `"$nonFinderReturnBreakpoint`""
    "bp OUTLOOK!ExplorerObject::HrGetCursor+0x187 `"$finderCallBreakpoint`""
    "bp OUTLOOK!ExplorerObject::HrGetCursor+0x18d `"$finderReturnBreakpoint`""
    "bp OUTLOOK!ExplorerObject::HrReallySwitchView+0x1187 `"$cursorReturnBreakpoint`""
    "bp OUTLOOK!ExplorerViewCollection::HrDoGetCurrentView+0x5ca `"$viewDefinitionFailureBreakpoint`""
    "bp OUTLOOK!ExplorerObject::HrReallySwitchView+0x1815 `"$switchViewFailureBreakpoint`""
    '.printf "OUTLOOK_VIEW_FAILURE_CAPTURE_ARMED\\n"'
    'g'
)
[System.IO.File]::WriteAllLines($commandPath, $commands, [System.Text.Encoding]::ASCII)

Write-Host "Attaching to Outlook PID $ProcessId. Outlook can pause briefly while symbols load."
Write-Host "The script exits at the first terminal view-definition or cursor failure and writes:"
Write-Host "  $dumpPath"
Write-Host "  $logPath"

& $CdbPath -p $ProcessId -logo $logPath -cf $commandPath
if ($LASTEXITCODE -ne 0) {
    throw "cdb.exe exited with code $LASTEXITCODE. See $logPath"
}

if (-not (Test-Path -LiteralPath $dumpPath)) {
    throw "Outlook exited or the debugger detached before a view failure was captured. See $logPath"
}

Write-Host "Captured Outlook view failure: $dumpPath"
