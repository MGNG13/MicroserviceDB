rustup default stable
$itemsToRemove = @("Build", "target", "Cargo.lock", "json-db-server", "MicroserviceDB")
foreach ($item in $itemsToRemove) {
    if (Test-Path $item) {
        Remove-Item -Recurse -Force $item -ErrorAction SilentlyContinue
    }
}
New-Item -ItemType Directory -Force -Path Build | Out-Null
cargo build --release
if (Test-Path "./target/release/json-db-server.exe") {
    Copy-Item "./target/release/json-db-server.exe" "./Build/MicroserviceDB.exe"
}
elseif (Test-Path "./target/release/json-db-server") {
    Copy-Item "./target/release/json-db-server" "./Build/MicroserviceDB"
}
Push-Location Library
try {
    tsc --project tsconfig.json
}
finally {
    Pop-Location
}
foreach ($item in $itemsToRemove) {
    if (Test-Path $item) {
        Remove-Item -Recurse -Force $item -ErrorAction SilentlyContinue
    }
}