use std::os::unix::ffi::OsStrExt;

fn main() {
    for entry in std::fs::read_dir("/proc/self/fd").expect("read process descriptors") {
        let entry = entry.expect("read process descriptor entry");
        let target = match std::fs::read_link(entry.path()) {
            Ok(target) => target,
            Err(_) => continue,
        };
        if target
            .as_os_str()
            .as_bytes()
            .windows(b"iq-rift-executable-test".len())
            .any(|window| window == b"iq-rift-executable-test")
        {
            eprintln!("Rift executable descriptor leaked into the Rift process");
            std::process::exit(97);
        }
    }

    let status = std::process::Command::new(env!("REAL_RIFT_EXECUTABLE"))
        .args(std::env::args_os().skip(1))
        .status()
        .expect("run real Rift executable");
    std::process::exit(status.code().unwrap_or(98));
}
