# Create necessary directories
if (!(Test-Path "etc")) { New-Item -ItemType Directory -Path "etc" }
if (!(Test-Path "etc\src")) { New-Item -ItemType Directory -Path "etc\src" }
if (!(Test-Path "archive")) { New-Item -ItemType Directory -Path "archive" }

# Move entire app folder into etc/src
if (Test-Path "src\app") {
    Move-Item "src\app" "etc\src\" -Force
}

# Move frontend folder into etc/src
if (Test-Path "src\frontend") {
    Move-Item "src\frontend" "etc\src\" -Force
}

# Archive everything else that is not:
$keep = @("src","docker","server","etc","README.md","report.pdf","students.json","proto","paxos")
Get-ChildItem -Path "." | Where-Object { $keep -notcontains $_.Name } | ForEach-Object {
    Move-Item $_.FullName "archive\" -Force
}

Write-Host "Project reorganized successfully. 'app' and frontend are now in etc/src/"
