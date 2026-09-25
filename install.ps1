# Install the peql command on Windows from a GitHub release:
#
#   powershell -ExecutionPolicy ByPass -c "irm https://github.com/griot-cloud/peql/releases/latest/download/install.ps1 | iex"
#
# $env:PEQL_VERSION picks a release (default: the latest), and $env:PEQL_INSTALL_DIR picks
# where peql.exe goes (default: %LOCALAPPDATA%\Programs\peql\bin, added to the user PATH).
# The archive's sha256 is checked before anything is installed. The x64 build also runs on
# Windows on Arm.
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'
# Windows PowerShell 5.1 may default to TLS 1.0, which GitHub refuses.
[Net.ServicePointManager]::SecurityProtocol = [Net.ServicePointManager]::SecurityProtocol -bor [Net.SecurityProtocolType]::Tls12

$Repo = 'griot-cloud/peql'
$Bin = 'peql'
$Target = 'x86_64-pc-windows-msvc'
$Version = if ($env:PEQL_VERSION) { $env:PEQL_VERSION } else { 'latest' }
$Dir = if ($env:PEQL_INSTALL_DIR) { $env:PEQL_INSTALL_DIR } else { Join-Path $env:LOCALAPPDATA "Programs\$Bin\bin" }

$Archive = "$Bin-$Target.zip"
$Base = if ($Version -eq 'latest') {
    "https://github.com/$Repo/releases/latest/download"
} else {
    "https://github.com/$Repo/releases/download/v$($Version.TrimStart('v'))"
}

$Tmp = Join-Path ([IO.Path]::GetTempPath()) ([Guid]::NewGuid())
New-Item -ItemType Directory -Path $Tmp | Out-Null
try {
    Write-Host "downloading $Archive ($Version)"
    Invoke-WebRequest "$Base/$Archive" -OutFile "$Tmp\$Archive" -UseBasicParsing
    Invoke-WebRequest "$Base/$Archive.sha256" -OutFile "$Tmp\$Archive.sha256" -UseBasicParsing
    $Want = ((Get-Content "$Tmp\$Archive.sha256" -Raw).Trim() -split '\s+')[0]
    $Got = (Get-FileHash "$Tmp\$Archive" -Algorithm SHA256).Hash
    if ($Got -ne $Want.ToUpper()) { throw "checksum mismatch for $Archive" }

    Expand-Archive "$Tmp\$Archive" -DestinationPath $Tmp -Force
    New-Item -ItemType Directory -Path $Dir -Force | Out-Null
    Copy-Item "$Tmp\$Bin-$Target\$Bin.exe" (Join-Path $Dir "$Bin.exe") -Force
    Write-Host "installed $(& (Join-Path $Dir "$Bin.exe") --version) to $Dir"

    $UserPath = [Environment]::GetEnvironmentVariable('Path', 'User')
    if (-not (($UserPath -split ';') -contains $Dir)) {
        $NewPath = if ($UserPath) { "$UserPath;$Dir" } else { $Dir }
        [Environment]::SetEnvironmentVariable('Path', $NewPath, 'User')
        Write-Host "added $Dir to your user PATH; open a new terminal to use $Bin"
    }
} finally {
    Remove-Item $Tmp -Recurse -Force -ErrorAction SilentlyContinue
}
