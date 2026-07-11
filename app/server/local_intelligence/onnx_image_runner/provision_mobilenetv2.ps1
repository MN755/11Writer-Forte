[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$DataDir,
    [Parameter(Mandatory = $true)]
    [string]$StagedModelPath,
    [Parameter(Mandatory = $true)]
    [string]$RunnerPython,
    [Parameter(Mandatory = $true)]
    [string]$BenchmarkReportPath,
    [string]$RunnerScriptPath = '',
    [string]$ApprovedBy = 'operator'
)

$ErrorActionPreference = 'Stop'
$ExpectedModelSha256 = 'c1c513582d56afceff8516c73804e484c81c6a830712ab6d682253f4a3cd042f'
$SourceUrl = 'https://huggingface.co/onnxmodelzoo/mobilenetv2-7/resolve/main/mobilenetv2-7.onnx'

if (-not $RunnerScriptPath) { $RunnerScriptPath = Join-Path $PSScriptRoot 'onnx_image_runner.py' }

function Get-Sha256([string]$Path) {
    return (Get-FileHash -LiteralPath $Path -Algorithm SHA256).Hash.ToLowerInvariant()
}

$dataRoot = [IO.Path]::GetFullPath($DataDir)
$staged = [IO.Path]::GetFullPath($StagedModelPath)
$python = [IO.Path]::GetFullPath($RunnerPython)
$runner = [IO.Path]::GetFullPath($RunnerScriptPath)
$benchmark = [IO.Path]::GetFullPath($BenchmarkReportPath)
foreach ($path in @($staged, $python, $runner, $benchmark)) {
    if (-not (Test-Path -LiteralPath $path -PathType Leaf)) { throw "Required file is missing: $path" }
}
if ((Get-Sha256 $staged) -ne $ExpectedModelSha256) {
    throw "Staged model checksum differs from the approved MobileNetV2 source; refusing provision."
}

$modelDir = Join-Path $dataRoot 'models\onnxmodelzoo'
$runnerDir = Join-Path $dataRoot 'local_intelligence\onnx_runner'
New-Item -ItemType Directory -Force -Path $modelDir, $runnerDir | Out-Null
$modelDestination = Join-Path $modelDir 'mobilenetv2-7.onnx'
$runnerDestination = Join-Path $runnerDir 'onnx_image_runner.py'
Copy-Item -LiteralPath $staged -Destination $modelDestination -Force
Copy-Item -LiteralPath $runner -Destination $runnerDestination -Force
if ((Get-Sha256 $modelDestination) -ne $ExpectedModelSha256) { throw 'Copy checksum verification failed.' }

# Verify only locally provisioned dependencies. This script never invokes pip or any network client.
& $python -c 'import numpy, onnxruntime, PIL; print(onnxruntime.__version__)'
if ($LASTEXITCODE -ne 0) { throw 'Runner Python lacks an operator-provisioned numpy/onnxruntime/Pillow stack.' }

$sbomPath = Join-Path $runnerDir 'mobilenetv2-7.sbom.json'
$packageLockPath = Join-Path $runnerDir 'onnxruntime-package-lock.json'
$sbom = @{
    component = 'onnxmodelzoo/mobilenetv2-7'; model_sha256 = $ExpectedModelSha256
    source_url = $SourceUrl; runner_asset_sha256 = (Get-Sha256 $runnerDestination)
    runner_python_sha256 = (Get-Sha256 $python); created_at = (Get-Date).ToUniversalTime().ToString('o')
} | ConvertTo-Json -Depth 5
$sbom | Set-Content -LiteralPath $sbomPath -Encoding UTF8
@{ python = $python; onnxruntime = (& $python -c 'import onnxruntime; print(onnxruntime.__version__)') } |
    ConvertTo-Json | Set-Content -LiteralPath $packageLockPath -Encoding UTF8

$manifest = @{
    model_id = 'onnxmodelzoo-mobilenetv2'; model_version = '7'; task = 'image_embedding'
    runtime = 'approved_command_v1'; model_path = $modelDestination; model_sha256 = $ExpectedModelSha256
    runner_executable_path = $python; runner_executable_sha256 = (Get-Sha256 $python)
    runner_arguments = @($runnerDestination); runner_asset_path = $runnerDestination; runner_asset_sha256 = (Get-Sha256 $runnerDestination)
    license_id = 'Apache-2.0'; upstream_origin = $SourceUrl; sbom_sha256 = (Get-Sha256 $sbomPath)
    package_lock_sha256 = (Get-Sha256 $packageLockPath); cve_review_ref = 'OPERATOR-MUST-REPLACE-AFTER-CVE-REVIEW'
    network_disabled = $true; gpu_vram_mb = 4096; cpu_fallback = $true; timeout_seconds = 30; queue_depth_limit = 1
    approved_by = $ApprovedBy; benchmark = (Get-Content -Raw -LiteralPath $benchmark | ConvertFrom-Json)
}
$manifestPath = Join-Path $runnerDir 'mobilenetv2-7.approval.json'
$manifest | ConvertTo-Json -Depth 10 | Set-Content -LiteralPath $manifestPath -Encoding UTF8
Write-Output "Provisioned local model and generated unapproved manifest: $manifestPath"
Write-Output 'Review license, CVEs, benchmark, runner asset hash, and replace cve_review_ref before POST /api/local-intelligence/models/approve.'
