cls
rustup default stable
$itemsToRemove = @("build", "target", "Cargo.lock", "microservice-db", "MicroserviceDB")
foreach ($item in $itemsToRemove) {
    if (Test-Path $item) {
        Remove-Item -Recurse -Force $item -ErrorAction SilentlyContinue
    }
}
cargo build --release
if (Test-Path "./target/release/microservice-db.exe") {
    Copy-Item "./target/release/microservice-db.exe" "./MicroserviceDB.exe"
}
elseif (Test-Path "./target/release/microservice-db") {
    Copy-Item "./target/release/microservice-db" "./MicroserviceDB"
}
# Push-Location Library
# try {
#     tsc --project tsconfig.json
# }
# finally {
#     Pop-Location
# }