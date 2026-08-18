#!/bin/bash
set -euo pipefail
CYAN='\033[0;36m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'
echo -e "${CYAN}========================================${NC}"
echo -e "${CYAN} MongoDB + Dragonfly Optimized Cluster${NC}"
echo -e "${CYAN}========================================${NC}"
echo ""
echo -e "${YELLOW}Starting Docker Compose...${NC}"
docker compose up -d
wait_mongo() {
    local service="$1"
    local port="$2"
    echo -e "${YELLOW}Waiting for $service...${NC}"
    for i in $(seq 1 60); do
        if docker compose exec -T "$service" mongosh \
            --port "$port" \
            --quiet \
            --eval 'db.adminCommand({ping:1}).ok' 2>/dev/null | grep -q 1; then
            echo -e "${GREEN}$service is ready.${NC}"
            return 0
        fi
        sleep 2
    done
    echo -e "${RED}$service did not become ready.${NC}" >&2
    exit 1
}
init_rs() {
    local service="$1"
    local port="$2"
    local rs_name="$3"
    local config="$4"
    echo -e "${CYAN}Checking $rs_name...${NC}"
    docker compose exec -T "$service" mongosh \
        --port "$port" \
        --quiet \
        --eval "
try {
  const s = rs.status();
  if (s.ok === 1) {
    print('$rs_name already initialized');
  }
} catch (e) {
  rs.initiate($config);
  print('$rs_name initialized');
}
"
}
wait_primary() {
    local service="$1"
    local port="$2"
    local rs_name="$3"
    echo -e "${YELLOW}Waiting for PRIMARY in $rs_name...${NC}"
    for i in $(seq 1 60); do
        if docker compose exec -T "$service" mongosh \
            --port "$port" \
            --quiet \
            --eval 'rs.status().members.some(x => x.stateStr === "PRIMARY")' 2>/dev/null | grep -q true; then
            echo -e "${GREEN}$rs_name PRIMARY elected.${NC}"
            return 0
        fi
        sleep 2
    done
    echo -e "${RED}$rs_name did not elect a PRIMARY.${NC}" >&2
    exit 1
}
wait_mongo "config1" 27019
wait_mongo "config2" 27019
wait_mongo "config3" 27019
wait_mongo "shard1a" 27018
wait_mongo "shard1b" 27018
wait_mongo "shard1c" 27018
wait_mongo "shard2a" 27018
wait_mongo "shard2b" 27018
wait_mongo "shard2c" 27018
wait_mongo "shard3a" 27018
wait_mongo "shard3b" 27018
wait_mongo "shard3c" 27018
echo ""
echo -e "${CYAN}Initializing replica sets...${NC}"
init_rs "config1" 27019 "configRS" '{
  _id: "configRS",
  configsvr: true,
  members: [
    { _id: 0, host: "config1:27019" },
    { _id: 1, host: "config2:27019" },
    { _id: 2, host: "config3:27019" }
  ]
}'
init_rs "shard1a" 27018 "shard1RS" '{
  _id: "shard1RS",
  members: [
    { _id: 0, host: "shard1a:27018" },
    { _id: 1, host: "shard1b:27018" },
    { _id: 2, host: "shard1c:27018" }
  ]
}'
init_rs "shard2a" 27018 "shard2RS" '{
  _id: "shard2RS",
  members: [
    { _id: 0, host: "shard2a:27018" },
    { _id: 1, host: "shard2b:27018" },
    { _id: 2, host: "shard2c:27018" }
  ]
}'
init_rs "shard3a" 27018 "shard3RS" '{
  _id: "shard3RS",
  members: [
    { _id: 0, host: "shard3a:27018" },
    { _id: 1, host: "shard3b:27018" },
    { _id: 2, host: "shard3c:27018" }
  ]
}'
wait_primary "config1" 27019 "configRS"
wait_primary "shard1a" 27018 "shard1RS"
wait_primary "shard2a" 27018 "shard2RS"
wait_primary "shard3a" 27018 "shard3RS"
echo ""
echo -e "${CYAN}Restarting mongos routers...${NC}"
docker compose restart mongos1 mongos2
sleep 10
add_shard() {
    local shard="$1"
    docker compose exec -T mongos1 mongosh \
        --quiet \
        --eval "
const shards = db.adminCommand({ listShards: 1 }).shards || [];
if (!shards.some(s => s._id === '$shard')) {
  print(db.adminCommand({ addShard: '$shard' }));
} else {
  print('$shard already configured');
}
"
}
echo ""
echo -e "${CYAN}Registering shards in mongos...${NC}"
add_shard "shard1RS/shard1a:27018,shard1b:27018,shard1c:27018"
add_shard "shard2RS/shard2a:27018,shard2b:27018,shard2c:27018"
add_shard "shard3RS/shard3a:27018,shard3b:27018,shard3c:27018"
MONGO_ADMIN_USER="admin"
MONGO_ADMIN_PASS="MicroserviceDB2026_MGNG13"
echo ""
echo -e "${CYAN}Creating admin user...${NC}"
docker compose exec -T mongos1 mongosh \
    --quiet \
    --eval "
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
"
echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN} Cluster Ready${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo -e "${CYAN}MongoDB routers:${NC}"
echo "mongodb://localhost:27017"
echo "mongodb://localhost:27020"
echo ""
echo -e "${CYAN}Recommended MongoDB URI (con auth):${NC}"
echo "mongodb://${MONGO_ADMIN_USER}:${MONGO_ADMIN_PASS}@localhost:27017,localhost:27020/?authSource=admin&serverSelectionTimeoutMS=5000"
echo ""
echo -e "${CYAN}Credentials:${NC}"
echo "  Username: ${MONGO_ADMIN_USER}"
echo "  Password: ${MONGO_ADMIN_PASS}"
echo "  Auth DB:  admin"
echo ""
echo -e "${CYAN}Dragonfly cache:${NC}"
echo "redis://localhost:6379"
echo ""
docker compose exec -T mongos1 mongosh --quiet --eval 'db.adminCommand({ listShards: 1 })'
echo ""
echo -e "${GREEN}Setup complete!${NC}"
