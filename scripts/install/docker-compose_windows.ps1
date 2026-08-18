$ErrorActionPreference = "Stop"
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
  rs.initiate($Config);
  print("$Name initialized");
}
"@
    $js | docker compose exec -T $Service mongosh --port $Port --quiet
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
        $evalScript = "rs.status().members.some(x => x.stateStr === 'PRIMARY')"
        $result = docker compose exec -T $Service mongosh `
            --port $Port `
            --quiet `
            --eval $evalScript 2>$null
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
    $js | docker compose exec -T mongos1 mongosh --quiet
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to register $Shard"
    }
}
Write-Host ""
Write-Host "Registering shards in mongos..." -ForegroundColor Cyan
Add-Shard "shard1RS/shard1a:27018,shard1b:27018,shard1c:27018"
Add-Shard "shard2RS/shard2a:27018,shard2b:27018,shard2c:27018"
Add-Shard "shard3RS/shard3a:27018,shard3b:27018,shard3c:27018"
$MONGO_ADMIN_USER = "admin"
$MONGO_ADMIN_PASS = "MicroserviceDB2026_MGNG13"
Write-Host ""
Write-Host "Creating admin user..." -ForegroundColor Cyan
$adminJs = @"
const adminUser = '$MONGO_ADMIN_USER';
const adminPass = '$MONGO_ADMIN_PASS';
db = db.getSiblingDB('admin');
try {
  const existing = db.getUser(adminUser);
  if (existing) {
    print('Admin user already exists, skipping creation');
  } else {
    db.createUser({
      user: adminUser,
      pwd: adminPass,
      roles: [{ role: 'root', db: 'admin' }]
    });
    print('Admin user created successfully');
  }
} catch (e) {
  print('Error managing admin user: ' + e.message);
  throw e;
}
"@
$adminJs | docker compose exec -T mongos1 mongosh --quiet
if ($LASTEXITCODE -ne 0) {
    throw "Failed to create admin user"
}
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
Write-Host "mongodb://$MONGO_ADMIN_USER`:$MONGO_ADMIN_PASS@localhost:27017,localhost:27020/?authSource=admin&serverSelectionTimeoutMS=5000"
Write-Host ""
Write-Host "Credentials:" -ForegroundColor Cyan
Write-Host "  Username: $MONGO_ADMIN_USER"
Write-Host "  Password: $MONGO_ADMIN_PASS"
Write-Host "  Auth DB:  admin"
Write-Host ""
Write-Host "Dragonfly cache:" -ForegroundColor Cyan
Write-Host "redis://localhost:6379"
Write-Host ""
'db.adminCommand({ listShards: 1 })' | docker compose exec -T mongos1 mongosh --quiet
Write-Host ""
Write-Host "Setup complete!" -ForegroundColor Green
