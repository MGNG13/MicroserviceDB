#!/bin/bash
set -e
CYAN='\033[0;36m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'
echo -e "${CYAN}========================================${NC}"
echo -e "${CYAN} MongoDB + Dragonfly Cluster Setup${NC}"
echo -e "${CYAN}========================================${NC}"
echo ""
echo -e "${YELLOW}Starting Docker Compose...${NC}"
docker compose up -d
sleep 10
wait_mongo() {
    local service="$1"
    local port="$2"
    echo -e "${YELLOW}Waiting for $service...${NC}"
    for i in $(seq 1 30); do
        if docker compose exec -T "$service" mongosh \
            --port "$port" \
            --quiet \
            --eval 'db.adminCommand({ping:1})' >/dev/null 2>&1; then
            echo -e "${GREEN}$service is ready.${NC}"
            return 0
        fi
        sleep 2
    done
    echo -e "${RED}$service did not become ready.${NC}" >&2
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
echo -e "${CYAN}Initializing configRS...${NC}"
docker compose exec -T config1 mongosh --port 27019 <<'EOF'
rs.initiate({
  _id: "configRS",
  configsvr: true,
  members: [
    { _id: 0, host: "config1:27019" },
    { _id: 1, host: "config2:27019" },
    { _id: 2, host: "config3:27019" }
  ]
})
EOF
echo ""
echo -e "${CYAN}Initializing shard1RS...${NC}"
docker compose exec -T shard1a mongosh --port 27018 <<'EOF'
rs.initiate({
  _id: "shard1RS",
  members: [
    { _id: 0, host: "shard1a:27018" },
    { _id: 1, host: "shard1b:27018" },
    { _id: 2, host: "shard1c:27018" }
  ]
})
EOF
echo ""
echo -e "${CYAN}Initializing shard2RS...${NC}"
docker compose exec -T shard2a mongosh --port 27018 <<'EOF'
rs.initiate({
  _id: "shard2RS",
  members: [
    { _id: 0, host: "shard2a:27018" },
    { _id: 1, host: "shard2b:27018" },
    { _id: 2, host: "shard2c:27018" }
  ]
})
EOF
echo ""
echo -e "${CYAN}Initializing shard3RS...${NC}"
docker compose exec -T shard3a mongosh --port 27018 <<'EOF'
rs.initiate({
  _id: "shard3RS",
  members: [
    { _id: 0, host: "shard3a:27018" },
    { _id: 1, host: "shard3b:27018" },
    { _id: 2, host: "shard3c:27018" }
  ]
})
EOF
echo ""
echo -e "${YELLOW}Waiting for replica set elections...${NC}"
sleep 15
echo ""
echo -e "${CYAN}Restarting mongos...${NC}"
docker compose restart mongos
sleep 10
echo ""
echo -e "${CYAN}Adding shards to mongos...${NC}"
docker compose exec -T mongos mongosh <<'EOF'
sh.addShard("shard1RS/shard1a:27018,shard1b:27018,shard1c:27018")
sh.addShard("shard2RS/shard2a:27018,shard2b:27018,shard2c:27018")
sh.addShard("shard3RS/shard3a:27018,shard3b:27018,shard3c:27018")
EOF
echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN} MongoDB Cluster Initialized!${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo -e "${CYAN}Shards:${NC}"
docker compose exec -T mongos mongosh <<'EOF'
db.adminCommand({ listShards: 1 })
EOF
echo ""
echo -e "${CYAN}Config Replica Set:${NC}"
docker compose exec -T config1 mongosh --port 27019 <<'EOF'
rs.status().members.map(x => ({
  name: x.name,
  state: x.stateStr
}))
EOF
echo ""
echo -e "${CYAN}Shard 1 Replica Set:${NC}"
docker compose exec -T shard1a mongosh --port 27018 <<'EOF'
rs.status().members.map(x => ({
  name: x.name,
  state: x.stateStr
}))
EOF
echo ""
echo -e "${CYAN}Shard 2 Replica Set:${NC}"
docker compose exec -T shard2a mongosh --port 27018 <<'EOF'
rs.status().members.map(x => ({
  name: x.name,
  state: x.stateStr
}))
EOF
echo ""
echo -e "${CYAN}Shard 3 Replica Set:${NC}"
docker compose exec -T shard3a mongosh --port 27018 <<'EOF'
rs.status().members.map(x => ({
  name: x.name,
  state: x.stateStr
}))
EOF
echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN} CONNECTIONS${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo -e "${CYAN}MongoDB :${NC}"
echo "mongodb://localhost:27017"
echo ""
echo -e "${CYAN}Dragonfly :${NC}"
echo "redis://localhost:6379"
echo ""
echo -e "${GREEN}Setup complete!${NC}"
