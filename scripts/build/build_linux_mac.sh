rustup default stable
rm -rf Build/ target/ Cargo.lock ./json-db-server ./MicroserviceDB
mkdir -p Build/
cargo build --release
if [ -f ./target/release/json-db-server.exe ]; then
  cp ./target/release/json-db-server.exe ./Build/MicroserviceDB.exe
elif [ -f ./target/release/json-db-server ]; then
  cp ./target/release/json-db-server ./Build/MicroserviceDB
fi
(cd Library && tsc --project tsconfig.json)
rm -rf target/ Cargo.lock ./json-db-server ./MicroserviceDB