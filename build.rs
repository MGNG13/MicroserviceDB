// Genera el servidor gRPC a partir de proto/microservicedb.proto.
// Usa el `protoc` indicado en PROTOC, o `.tools/protoc` si existe, o el binario
// incluido en el crate `protoc-bin-vendored` (no hace falta instalar protoc).
fn main() -> Result<(), Box<dyn std::error::Error>> {
    if std::env::var_os("PROTOC").is_none() {
        let local = std::path::Path::new(".tools").join("protoc");
        let protoc = if local.exists() {
            local
        } else {
            protoc_bin_vendored::protoc_bin_path()?
        };
        std::env::set_var("PROTOC", protoc);
    }
    tonic_build::configure()
        .build_server(true)
        .build_client(false)
        .compile_protos(&["proto/microservicedb.proto"], &["proto/"])?;
    println!("cargo:rerun-if-changed=proto/microservicedb.proto");
    Ok(())
}
