#file to run the system 

#go to the root
$ScriptFolder = Split-Path -Parent $MyInvocation.MyCommand.Path
$RootDir = [System.IO.Path]::GetFullPath("$ScriptFolder\..\..")
Set-Location $RootDir

#close old containers
docker-compose -f "src/docker/docker-compose.yaml" down

docker-compose -f "src/docker/docker-compose.yaml" build --no-cache

#run the docker
docker-compose -f "src/docker/docker-compose.yaml" up -d

$LogCmd = "Set-Location '$RootDir'; docker-compose -f src/docker/docker-compose.yaml logs -f"
Start-Process powershell -ArgumentList "-NoExit", "-Command", "$LogCmd"

#messege and wait
Write-Host "Starting system, waiting" -ForegroundColor Cyan
Start-Sleep -Seconds 15

#open the front
$FrontendDir = "$RootDir\src\etc\frontend"
Set-Location $FrontendDir

if (Test-Path "package.json") {
    Write-Host "Found package.json, installing dependencies if needed" -ForegroundColor Green
    npm install

    Write-Host "Starting frontend with npm start" -ForegroundColor Green
    Start-Process powershell -ArgumentList "-NoExit", "-Command", "npm start"

    do {
        Start-Sleep -Seconds 1
        $tcpConnection = Test-NetConnection -ComputerName "localhost" -Port 4200
    } until ($tcpConnection.TcpTestSucceeded)

    Write-Host "Opening browser" -ForegroundColor Cyan

    Start-Process "http://localhost:4200"
  
} 
