$ErrorActionPreference = "Stop"
Write-Host "========================================" -ForegroundColor Cyan
Write-Host " MongoDB + Dragonfly Cluster Setup" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
# ============================================================
# START
# ============================================================
Write-Host ""
Write-Host "Starting Docker Compose..." -ForegroundColor Yellow
docker compose up -d
if ($LASTEXITCODE -ne 0) {
    throw "docker compose up failed"
}
# ============================================================
# WAIT FOR MONGODB
# ============================================================
Start-Sleep -Seconds 10
function Wait-Mongo {
    param (
        [string]$Service,
        [int]$Port
    )
    Write-Host "Waiting for $Service..." -ForegroundColor Yellow
    for ($i = 1; $i -le 30; $i++) {
        $output = docker compose exec -T $Service mongosh `
            --port $Port `
            --quiet `
            --eval 'db.adminCommand({ping:1})' 2>&1
        if ($LASTEXITCODE -eq 0) {
            Write-Host "$Service is ready." -ForegroundColor Green
            return
        }
        Start-Sleep -Seconds 2
    }
    throw "$Service did not become ready."
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
# ============================================================
# CONFIG SERVER
# ============================================================
Write-Host ""
Write-Host "Initializing configRS..." -ForegroundColor Cyan
@'
rs.initiate({
  _id: "configRS",
  configsvr: true,
  members: [
    { _id: 0, host: "config1:27019" },
    { _id: 1, host: "config2:27019" },
    { _id: 2, host: "config3:27019" }
  ]
})
'@ | docker compose exec -T config1 mongosh --port 27019
if ($LASTEXITCODE -ne 0) {
    throw "Failed to initialize configRS"
}
# ============================================================
# SHARD 1
# ============================================================
Write-Host ""
Write-Host "Initializing shard1RS..." -ForegroundColor Cyan
@'
rs.initiate({
  _id: "shard1RS",
  members: [
    { _id: 0, host: "shard1a:27018" },
    { _id: 1, host: "shard1b:27018" },
    { _id: 2, host: "shard1c:27018" }
  ]
})
'@ | docker compose exec -T shard1a mongosh --port 27018
if ($LASTEXITCODE -ne 0) {
    throw "Failed to initialize shard1RS"
}
# ============================================================
# SHARD 2
# ============================================================
Write-Host ""
Write-Host "Initializing shard2RS..." -ForegroundColor Cyan
@'
rs.initiate({
  _id: "shard2RS",
  members: [
    { _id: 0, host: "shard2a:27018" },
    { _id: 1, host: "shard2b:27018" },
    { _id: 2, host: "shard2c:27018" }
  ]
})
'@ | docker compose exec -T shard2a mongosh --port 27018
if ($LASTEXITCODE -ne 0) {
    throw "Failed to initialize shard2RS"
}
# ============================================================
# SHARD 3
# ============================================================
Write-Host ""
Write-Host "Initializing shard3RS..." -ForegroundColor Cyan
@'
rs.initiate({
  _id: "shard3RS",
  members: [
    { _id: 0, host: "shard3a:27018" },
    { _id: 1, host: "shard3b:27018" },
    { _id: 2, host: "shard3c:27018" }
  ]
})
'@ | docker compose exec -T shard3a mongosh --port 27018
if ($LASTEXITCODE -ne 0) {
    throw "Failed to initialize shard3RS"
}
# ============================================================
# WAIT FOR PRIMARY ELECTIONS
# ============================================================
Write-Host ""
Write-Host "Waiting for replica set elections..." -ForegroundColor Yellow
Start-Sleep -Seconds 15
# ============================================================
# RESTART MONGOS
# ============================================================
Write-Host ""
Write-Host "Restarting mongos..." -ForegroundColor Cyan
docker compose restart mongos
if ($LASTEXITCODE -ne 0) {
    throw "Failed to restart mongos"
}
Start-Sleep -Seconds 10
# ============================================================
# ADD SHARDS
# ============================================================
Write-Host ""
Write-Host "Adding shards to mongos..." -ForegroundColor Cyan
@'
sh.addShard("shard1RS/shard1a:27018,shard1b:27018,shard1c:27018")
sh.addShard("shard2RS/shard2a:27018,shard2b:27018,shard2c:27018")
sh.addShard("shard3RS/shard3a:27018,shard3b:27018,shard3c:27018")
'@ | docker compose exec -T mongos mongosh
if ($LASTEXITCODE -ne 0) {
    throw "Failed to add shards"
}
# ============================================================
# VERIFY
# ============================================================
Write-Host ""
Write-Host "========================================" -ForegroundColor Green
Write-Host " MongoDB Cluster Initialized!" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green
Write-Host ""
Write-Host "Shards:" -ForegroundColor Cyan
@'
db.adminCommand({ listShards: 1 })
'@ | docker compose exec -T mongos mongosh
Write-Host ""
Write-Host "Config Replica Set:" -ForegroundColor Cyan
@'
rs.status().members.map(x => ({
  name: x.name,
  state: x.stateStr
}))
'@ | docker compose exec -T config1 mongosh --port 27019
Write-Host ""
Write-Host "Shard 1 Replica Set:" -ForegroundColor Cyan
@'
rs.status().members.map(x => ({
  name: x.name,
  state: x.stateStr
}))
'@ | docker compose exec -T shard1a mongosh --port 27018
Write-Host ""
Write-Host "Shard 2 Replica Set:" -ForegroundColor Cyan
@'
rs.status().members.map(x => ({
  name: x.name,
  state: x.stateStr
}))
'@ | docker compose exec -T shard2a mongosh --port 27018
Write-Host ""
Write-Host "Shard 3 Replica Set:" -ForegroundColor Cyan
@'
rs.status().members.map(x => ({
  name: x.name,
  state: x.stateStr
}))
'@ | docker compose exec -T shard3a mongosh --port 27018
Write-Host ""
Write-Host "========================================" -ForegroundColor Green
Write-Host " CONNECTIONS" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green
Write-Host ""
Write-Host "MongoDB :" -ForegroundColor Cyan
Write-Host "mongodb://localhost:27017"
Write-Host ""
Write-Host "Dragonfly :" -ForegroundColor Cyan
Write-Host "redis://localhost:6379"
Write-Host ""
Write-Host "Setup complete!" -ForegroundColor Green