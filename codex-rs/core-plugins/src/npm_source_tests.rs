use super::*;
use flate2::Compression;
use flate2::write::GzEncoder;
use pretty_assertions::assert_eq;
use std::io::Cursor;
use std::io::Write;

#[cfg(unix)]
#[test]
fn materialize_npm_plugin_source_uses_packed_package_root() {
    let codex_home = tempfile::tempdir().expect("create codex home");
    let fake_npm_dir = tempfile::tempdir().expect("create fake npm directory");
    let archive_bytes =
        npm_package_archive_bytes("@acme/plugin", "1.2.0").expect("build fixture archive");
    let archive_path = fake_npm_dir.path().join("fixture.tgz");
    fs::write(&archive_path, &archive_bytes).expect("write fixture archive");
    let fake_npm = fake_npm_dir.path().join("npm");
    codex_utils_cargo_bin::write_executable(
        &fake_npm,
        &format!(
            r#"#!/bin/sh
destination=""
previous=""
for argument in "$@"; do
  if [ "$previous" = "--pack-destination" ]; then
    destination="$argument"
  fi
  previous="$argument"
done
cp "{}" "$destination/acme-plugin-1.2.0.tgz"
printf '%s\n' "$@" > "$destination/args.txt"
pwd > "$destination/pwd.txt"
"#,
            archive_path.display()
        ),
    )
    .expect("write fake npm");

    let (plugin_root, tempdir) = materialize_npm_plugin_source_with_command(
        codex_home.path(),
        "@acme/plugin",
        Some("^1.2.0"),
        Some("https://npm.example.com"),
        fake_npm.as_os_str(),
    )
    .expect("materialize npm source");

    assert_eq!(
        plugin_root.as_path(),
        tempdir.path().join("extracted/package")
    );
    assert!(
        plugin_root
            .as_path()
            .join(".codex-plugin/plugin.json")
            .is_file()
    );
    let args = fs::read_to_string(tempdir.path().join("args.txt")).expect("read npm arguments");
    assert!(args.contains("pack"));
    assert!(args.contains("--ignore-scripts"));
    assert!(args.contains("--registry"));
    assert!(args.contains("https://npm.example.com"));
    assert!(args.contains("@acme/plugin@^1.2.0"));
    assert!(!args.contains("install"));
    let npm_working_directory = fs::canonicalize(
        fs::read_to_string(tempdir.path().join("pwd.txt"))
            .expect("read npm working directory")
            .trim(),
    )
    .expect("canonicalize npm working directory");
    assert_eq!(
        npm_working_directory,
        fs::canonicalize(tempdir.path()).expect("canonicalize tempdir")
    );
}

fn npm_package_archive_bytes(package: &str, version: &str) -> std::io::Result<Vec<u8>> {
    let encoder = GzEncoder::new(Vec::new(), Compression::default());
    let mut archive = tar::Builder::new(encoder);
    append_archive_file(
        &mut archive,
        "package/package.json",
        format!(r#"{{"name":"{package}","version":"{version}"}}"#).as_bytes(),
    )?;
    append_archive_file(
        &mut archive,
        "package/.codex-plugin/plugin.json",
        br#"{"name":"plugin"}"#,
    )?;
    let encoder = archive.into_inner()?;
    encoder.finish()
}

fn append_archive_file<W: Write>(
    archive: &mut tar::Builder<W>,
    path: &str,
    contents: &[u8],
) -> std::io::Result<()> {
    let mut header = tar::Header::new_gnu();
    header.set_size(contents.len() as u64);
    header.set_mode(0o644);
    header.set_cksum();
    archive.append_data(&mut header, path, Cursor::new(contents))
}

#[tokio::test]
async fn managed_npm_pack_preserves_arguments_and_reaps_on_late_deadline() {
    let home = tempfile::tempdir().unwrap();
    let ready = home.path().join("ready");
    let args_path = home.path().join("args");
    let executable = home.path().join("npm-probe");
    codex_utils_cargo_bin::write_executable(
        &executable,
        &format!(
            "#!/bin/sh\nprintf '%s\\n' \"$@\" > '{}'\nprintf '%s' $$ > '{}'\nexec /bin/sleep 5\n",
            args_path.display(),
            ready.display()
        ),
    )
    .unwrap();
    let registry = crate::PluginTaskRegistry::default();
    let destination = home.path().to_owned();
    let expected_destination = destination.to_string_lossy().into_owned();
    let (done, result) = tokio::sync::oneshot::channel();
    let receipt = registry
        .spawn_thread("npm-command-stop-probe", move || {
            let _ = done.send(pack_npm_package(
                &destination,
                "@acme/plugin",
                Some("1.2.3"),
                Some("https://registry.example.invalid"),
                executable.as_os_str(),
            ));
        })
        .unwrap();
    let pid: i32 = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            if let Ok(raw) = fs::read_to_string(&ready) {
                if let Ok(pid) = raw.parse() {
                    break pid;
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let arguments = fs::read_to_string(args_path).unwrap();
    assert_eq!(
        arguments.lines().collect::<Vec<_>>(),
        vec![
            "pack",
            "--ignore-scripts",
            "--pack-destination",
            &expected_destination,
            "--registry",
            "https://registry.example.invalid",
            "--",
            "@acme/plugin@1.2.3"
        ]
    );
    let _ = registry.shutdown_until(tokio::time::Instant::now()).await;
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(2), result)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err()
            .contains("shutdown deadline")
    );
    assert_eq!(receipt.await, crate::retirement::JoinOutcome::Joined);
    assert!(
        registry
            .shutdown_until(tokio::time::Instant::now())
            .await
            .is_clean()
    );
    assert_eq!(unsafe { libc::kill(pid, 0) }, -1);
    assert_eq!(
        std::io::Error::last_os_error().raw_os_error(),
        Some(libc::ESRCH)
    );
}

#[tokio::test]
async fn expired_npm_materialization_creates_no_staging_directory() {
    let home = tempfile::tempdir().unwrap();
    let registry = crate::PluginTaskRegistry::default();
    registry.shutdown_until(tokio::time::Instant::now()).await;
    let _scope = crate::retirement::WorkerStopScope::enter(registry.worker_stop());
    let error = materialize_npm_plugin_source_with_command(
        home.path(),
        "@acme/plugin",
        None,
        None,
        OsStr::new("/not/a/real/npm"),
    )
    .unwrap_err();
    assert!(error.contains("shutdown deadline"));
    assert!(!home.path().join(NPM_PLUGIN_SOURCE_STAGING_DIR).exists());
}
