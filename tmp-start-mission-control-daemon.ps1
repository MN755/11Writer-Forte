$ErrorActionPreference = "Stop"

$env:MISSION_CONTROL_RUNTIME_ROOT = "C:\tmp\codex-mission-control-runtime"
$env:MISSION_CONTROL_CODEX_PROFILE_ROOT = "C:\tmp\codex-mission-control-runtime\codex-profile"

& "C:\Users\mike\OneDrive\Desktop\Codex Mission Control\scripts\start-mission-control-daemon.ps1"
