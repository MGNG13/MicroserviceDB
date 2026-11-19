$ErrorActionPreference = "Stop"
# docker compose busca docker-compose.yml en el directorio actual.
Set-Location -Path $PSScriptRoot
# Secretos del despliegue: se generan la primera vez y se guardan en .env (junto a este script).
# docker compose lee .env automáticamente. No compartas ni subas ese archivo al repositorio.
$envFile = Join-Path $PSScriptRoot ".env"
function New-Secret([int]$Bytes) {
    $buf = New-Object byte[] $Bytes
    [System.Security.Cryptography.RandomNumberGenerator]::Create().GetBytes($buf)
    return (($buf | ForEach-Object { $_.ToString("x2") }) -join "")
}
function Get-EnvValue([string]$Name) {
    if (-not (Test-Path $envFile)) { return $null }
    $line = Get-Content $envFile | Where-Object { $_ -match "^$Name=" } | Select-Object -First 1
    if ($line) { return $line.Substring($Name.Length + 1) }
    return $null
}
function Ensure-Secret([string]$Name, [string]$Value) {
    $current = Get-EnvValue $Name
    if ($current) { return $current }
    Add-Content -Path $envFile -Value "$Name=$Value"
    Write-Host "$Name generated in $envFile" -ForegroundColor Yellow
    return $Value
}
$null = Ensure-Secret "MICROSERVICEDB_ADMIN_API_KEY" ("mdb_" + (New-Secret 24))
$MONGO_ADMIN_PASS = Ensure-Secret "MONGO_ADMIN_PASSWORD" (New-Secret 24)
$null = Ensure-Secret "MONGO_KEYFILE_CONTENT" (New-Secret 64)
$DRAGONFLY_PASS = Ensure-Secret "DRAGONFLY_PASSWORD" (New-Secret 24)
$MONGO_ADMIN_USER = "admin"

# Ejecuta mongosh; con keyFile el cluster exige auth una vez creado el usuario admin
# (antes se usa la excepción de localhost). Si la llamada sin credenciales es rechazada, reintenta con ellas.
function Invoke-Mongosh([string]$Service, [int]$Port, [string]$Js) {
    $out = $Js | docker compose exec -T $Service mongosh --port $Port --quiet 2>&1
    $code = $LASTEXITCODE
    if (($out -join "`n") -match "requires authentication|Unauthorized|not authorized|command .* requires") {
        $out = $Js | docker compose exec -T $Service mongosh --port $Port --quiet `
            -u $MONGO_ADMIN_USER -p $MONGO_ADMIN_PASS --authenticationDatabase admin 2>&1
        $code = $LASTEXITCODE
    }
    $global:LASTEXITCODE = $code
    return $out
}
Write-Host "========================================" -ForegroundColor Cyan
Write-Host " MongoDB + Dragonfly Optimized Cluster" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Starting Docker Compose..." -ForegroundColor Yellow
docker compose up -d
if ($LASTEXITCODE -ne 0) {
    throw "docker compose up failed"
}
function Wait-Mongo {
    param (
        [string]$Service,
        [int]$Port
    )
    Write-Host "Waiting for $Service..." -ForegroundColor Yellow
    for ($i = 1; $i -le 60; $i++) {
        $output = docker compose exec -T $Service mongosh `
            --port $Port `
            --quiet `
            --eval 'db.adminCommand({ping:1}).ok' 2>$null
        if ($LASTEXITCODE -eq 0 -and $output -match "1") {
            Write-Host "$Service is ready." -ForegroundColor Green
            return
        }
        Start-Sleep -Seconds 2
    }
    throw "$Service did not become ready."
}
function Init-ReplicaSet {
    param (
        [string]$Service,
        [int]$Port,
        [string]$Name,
        [string]$Config
    )
    Write-Host "Checking $Name..." -ForegroundColor Cyan
    $js = @"
try {
  const s = rs.status();
  if (s.ok === 1) {
    print("$Name already initialized");
  }
} catch (e) {
  if (e.codeName === "Unauthorized" || /requires authentication/.test(String(e))) throw e;
  rs.initiate($Config);
  print("$Name initialized");
}
"@
    Invoke-Mongosh $Service $Port $js | Write-Host
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to initialize/check $Name"
    }
}
function Wait-Primary {
    param (
        [string]$Service,
        [int]$Port,
        [string]$Name
    )
    Write-Host "Waiting for PRIMARY in $Name..." -ForegroundColor Yellow
    for ($i = 1; $i -le 60; $i++) {
        $evalScript = "print(rs.status().members.some(x => x.stateStr === 'PRIMARY'))"
        $result = Invoke-Mongosh $Service $Port $evalScript
        if ($LASTEXITCODE -eq 0 -and $result -match "true") {
            Write-Host "$Name PRIMARY elected." -ForegroundColor Green
            return
        }
        Start-Sleep -Seconds 2
    }
    throw "$Name did not elect a PRIMARY."
}
Wait-Mongo "config1" 27019
Wait-Mongo "config2" 27019
Wait-Mongo "config3" 27019
Wait-Mongo "shard1a" 27018
Wait-Mongo "shard1b" 27018
Wait-Mongo "shard1c" 27018
Wait-Mongo "shard2a" 27018
Wait-Mongo "shard2b" 27018
Wait-Mongo "shard2c" 27018
Wait-Mongo "shard3a" 27018
Wait-Mongo "shard3b" 27018
Wait-Mongo "shard3c" 27018
Write-Host ""
Write-Host "Initializing replica sets..." -ForegroundColor Cyan
Init-ReplicaSet "config1" 27019 "configRS" @'
{
  _id: "configRS",
  configsvr: true,
  members: [
    { _id: 0, host: "config1:27019" },
    { _id: 1, host: "config2:27019" },
    { _id: 2, host: "config3:27019" }
  ]
}
'@
Init-ReplicaSet "shard1a" 27018 "shard1RS" @'
{
  _id: "shard1RS",
  members: [
    { _id: 0, host: "shard1a:27018" },
    { _id: 1, host: "shard1b:27018" },
    { _id: 2, host: "shard1c:27018" }
  ]
}
'@
Init-ReplicaSet "shard2a" 27018 "shard2RS" @'
{
  _id: "shard2RS",
  members: [
    { _id: 0, host: "shard2a:27018" },
    { _id: 1, host: "shard2b:27018" },
    { _id: 2, host: "shard2c:27018" }
  ]
}
'@
Init-ReplicaSet "shard3a" 27018 "shard3RS" @'
{
  _id: "shard3RS",
  members: [
    { _id: 0, host: "shard3a:27018" },
    { _id: 1, host: "shard3b:27018" },
    { _id: 2, host: "shard3c:27018" }
  ]
}
'@
Wait-Primary "config1" 27019 "configRS"
Wait-Primary "shard1a" 27018 "shard1RS"
Wait-Primary "shard2a" 27018 "shard2RS"
Wait-Primary "shard3a" 27018 "shard3RS"
Write-Host ""
Write-Host "Restarting mongos routers..." -ForegroundColor Cyan
docker compose restart mongos1 mongos2
if ($LASTEXITCODE -ne 0) {
    throw "Failed to restart mongos routers"
}
Start-Sleep -Seconds 10
function Add-Shard {
    param (
        [string]$Shard
    )
    $js = @"
const shards = db.adminCommand({ listShards: 1 }).shards || [];
if (!shards.some(s => s._id === "$($Shard.Split('/')[0])")) {
  print(db.adminCommand({ addShard: "$Shard" }));
} else {
  print("$($Shard.Split('/')[0]) already configured");
}
"@
    Invoke-Mongosh "mongos1" 27017 $js | Write-Host
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to register $Shard"
    }
}
# Con keyFile, la excepción de localhost de mongos solo permite crear el primer usuario:
# el admin se crea antes de registrar los shards.
Write-Host ""
Write-Host "Creating admin user..." -ForegroundColor Cyan
$adminJs = @"
const adminUser = '$MONGO_ADMIN_USER';
const adminPass = '$MONGO_ADMIN_PASS';
db = db.getSiblingDB('admin');
// createUser directo: con la excepción de localhost no se puede consultar getUser.
try {
  db.createUser({ user: adminUser, pwd: adminPass, roles: [{ role: 'root', db: 'admin' }] });
  print('Admin user created successfully');
} catch (e) {
  if (/already exists/.test(e.message)) {
    print('Admin user already exists, skipping creation');
  } else {
    print('Error managing admin user: ' + e.message);
    throw e;
  }
}
"@
Invoke-Mongosh "mongos1" 27017 $adminJs | Write-Host
if ($LASTEXITCODE -ne 0) {
    throw "Failed to create admin user"
}
Write-Host ""
Write-Host "Registering shards in mongos..." -ForegroundColor Cyan
Add-Shard "shard1RS/shard1a:27018,shard1b:27018,shard1c:27018"
Add-Shard "shard2RS/shard2a:27018,shard2b:27018,shard2c:27018"
Add-Shard "shard3RS/shard3a:27018,shard3b:27018,shard3c:27018"
Write-Host ""
Write-Host "========================================" -ForegroundColor Green
Write-Host " Cluster Ready" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green
Write-Host ""
Write-Host "MongoDB routers:" -ForegroundColor Cyan
Write-Host "mongodb://localhost:27017"
Write-Host "mongodb://localhost:27020"
Write-Host ""
Write-Host "Recommended MongoDB URI (con auth):" -ForegroundColor Cyan
Write-Host "mongodb://$MONGO_ADMIN_USER`:<MONGO_ADMIN_PASSWORD>@localhost:27017,localhost:27020/?authSource=admin&serverSelectionTimeoutMS=5000"
Write-Host ""
Write-Host "Credentials:" -ForegroundColor Cyan
Write-Host "  Username: $MONGO_ADMIN_USER"
Write-Host "  Password: MONGO_ADMIN_PASSWORD in $envFile"
Write-Host "  Auth DB:  admin"
Write-Host ""
Write-Host "Dragonfly cache:" -ForegroundColor Cyan
Write-Host "redis://:<DRAGONFLY_PASSWORD>@localhost:6379 (password in $envFile)"
Write-Host ""
Invoke-Mongosh "mongos1" 27017 'db.adminCommand({ listShards: 1 })' | Write-Host
Write-Host ""
Write-Host "Building and starting MicroserviceDB..." -ForegroundColor Cyan
docker compose --profile app up -d --build microservicedb
if ($LASTEXITCODE -ne 0) {
    throw "Failed to start MicroserviceDB"
}
Write-Host "MicroserviceDB:" -ForegroundColor Cyan
Write-Host "  HTTP/UI:   http://localhost:3329/"
Write-Host "  WebSocket: ws://localhost:3329/ws"
Write-Host "  gRPC:      localhost:3330"
Write-Host "  API key:   MICROSERVICEDB_ADMIN_API_KEY in $envFile"
Write-Host ""
Write-Host "Setup complete!" -ForegroundColor Green
