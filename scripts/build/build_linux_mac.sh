clear
rustup default stable
rm -rf build/ target/ Cargo.lock ./microservice-db ./MicroserviceDB
cargo build --release
if [ -f ./target/release/microservice-db.exe ]; then
  cp ./target/release/microservice-db.exe ./MicroserviceDB.exe
elif [ -f ./target/release/microservice-db ]; then
  cp ./target/release/microservice-db ./MicroserviceDB
fi
# (cd lib/typescript && tsc --project tsconfig.json)