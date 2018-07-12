$ErrorActionPreference="Stop"
$BaseDir=$env:TEAMCITY_CHECKOUT_DIR
if (-Not $BaseDir) {
    $BaseDir=$PSScriptRoot
}
$Password="password"
$Port=7699
$Python="python.exe"
$TestArgs=$args

$ErrorBoltKitNotAvailable=11
$ErrorServerCleanUpFailed=18
$ErrorServerInstallFailed=13
$ErrorServerConfigFailed=14
$ErrorServerStartFailed=15
$ErrorServerStopFailed=16
$ErrorTestsFailed=199

trap {
    Exit 1
}

Function CheckBoltKit()
{
    Write-Host "Checking boltkit..."
    & $Python -c "import boltkit" *> $null
    If ( $LASTEXITCODE -ne 0 )
    {
        throw @{ 
            Code = $ErrorBoltKitNotAvailable
            Message = "FATAL: The boltkit library is not available. Use 'pip install boltkit' to install." 
        }
    }
}

Function Cleanup($Target)
{
    Write-Host $Target
    try
    {
        Get-ChildItem -Path $Target -Recurse -ErrorAction Ignore | Remove-Item -Force -Recurse -ErrorAction Stop
    }
    catch
    {
        throw @{ 
            Code = $ErrorServerCleanUpFailed
            Message = "FATAL: Server directory cleanup failed." 
        }
    }
}

Function InstallServer($Target, $Version)
{
    Write-Host "-- Installing server"
    $Server = & cmd.exe /C "neoctrl-install ${Version} ${Target}"
    if ( $LASTEXITCODE -ne 0 )
    {
        throw @{ 
            Code = $ErrorServerInstallFailed
            Message = "FATAL: Server installation failed." 
        }
    }
    Write-Host "-- Server installed at $Server"

    Write-Host "-- Configuring server to listen on port $Port"
    & neoctrl-configure "$Server" dbms.connector.bolt.listen_address=:$Port
    if ( $LASTEXITCODE -ne 0 )
    {
        throw @{ 
            Code = $ErrorServerConfigFailed
            Message = "FATAL: Unable to configure server port." 
        }
    }

    Write-Host "-- Configuring server to accept IPv6 connections"
    & neoctrl-configure "$Server" dbms.connectors.default_listen_address=::
    if ( $LASTEXITCODE -ne 0 )
    {
        throw @{ 
            Code = $ErrorServerConfigFailed
            Message = "FATAL: Unable to configure server for IPv6." 
        }
    }

    Write-Host "-- Setting initial password"
    & neoctrl-set-initial-password "$Password" "$Server"
    if ( $LASTEXITCODE -ne 0 )
    {
        throw @{ 
            Code = $ErrorServerConfigFailed
            Message = "FATAL: Unable to set initial password." 
        }
    }

    Return $Server
}


Function StartServer($Server)
{
    Write-Host "-- Starting server"
    $BoltUri = Invoke-Expression "neoctrl-start $Server" | Select-String "^bolt:"
    if ( $LASTEXITCODE -ne 0 )
    {
        throw @{ 
            Code = $ErrorServerStartFailed
            Message = "FATAL: Failed to start server." 
        }
    }
    Write-Host "-- Server is listening at $BoltUri"

    Return $BoltUri
}

Function StopServer($Server)
{
    Write-Host "-- Stopping server"
    & neoctrl-stop $Server
    if ( $LASTEXITCODE -ne 0 )
    {
        throw @{ 
            Code = $ErrorServerStopFailed
            Message = "FATAL: Failed to stop server."
        }
    }
}

Function RunTests($Version)
{
    Write-Host $Version

    $ServerBase = "$BaseDir\build\server"
    Cleanup $ServerBase

    Write-Host "Testing against Neo4j $Version"
    $Server = InstallServer $ServerBase $Version

    $BoltUri = StartServer $Server
    try
    {
        Write-Host "-- Running tests"
        $env:BOLT_PORT=$Port
        $env:BOLT_PASSWORD=$Password
        & go test $TestArgs
        if ( $LASTEXITCODE -ne 0 )
        {
            throw @{ 
                Code = $ErrorTestsFailed
                Message = "FATAL: Test execution failed."
            }
        }
    }
    finally
    {
        StopServer $Server
    }
}


try
{
    CheckBoltKit

    $Version=$env:NEO4J_VERSION
    if (-Not $Version) {
        $Version="-e 3.4"
    }

    RunTests $Version
}
catch
{
    $ErrorCode = 1
    $ErrorMessage = $_.Exception.Message

    If ( $_.TargetObject.Code -and $_.TargetObject.Message )
    {
        $ErrorCode = $_.TargetObject.Code
        $ErrorMessage = $_.TargetObject.Message
    }

    If ( $env:TEAMCITY_PROJECT_NAME )
    {
        $CleanedErrorMessage = $ErrorMessage -replace "[^a-zA-Z0-9., ]"

        Write-Host "##teamcity[buildProblem description='$($CleanedErrorMessage)' identity='$($ErrorCode)']"
        Write-Host "##teamcity[buildStatus status='FAILURE' text='$($CleanedErrorMessage)']"
    }
    Else
    {
        Write-Host "$($ErrorMessage) [$($ErrorCode)]"
    }

    Exit $ErrorCode
}
