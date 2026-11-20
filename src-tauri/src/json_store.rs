use serde::{Serialize, de::DeserializeOwned};
use std::{
    fs,
    io::{Read, Write},
    path::{Path, PathBuf},
};

pub(crate) const MAX_JSON_BYTES: u64 = 32 * 1024 * 1024;

pub(crate) struct JsonStore;

impl JsonStore {
    pub(crate) fn ensure_object(path: &Path) -> anyhow::Result<()> {
        if !path.exists() {
            Self::write_atomic(path, &serde_json::json!({}))?;
        }
        Ok(())
    }

    pub(crate) fn read_or_default<T>(path: &Path, default: impl FnOnce() -> T) -> anyhow::Result<T>
    where
        T: DeserializeOwned,
    {
        let file = match fs::File::open(path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(default()),
            Err(error) => return Err(error.into()),
        };
        Self::check_size(path, file.metadata()?.len(), MAX_JSON_BYTES)?;
        let mut bytes = Vec::new();
        file.take(MAX_JSON_BYTES + 1).read_to_end(&mut bytes)?;
        Self::check_size(path, bytes.len() as u64, MAX_JSON_BYTES)?;
        serde_json::from_slice(&bytes)
            .map_err(|error| anyhow::anyhow!("JSON 数据文件损坏（{}）：{error}", path.display()))
    }

    pub(crate) fn write_atomic<T: Serialize>(path: &Path, value: &T) -> anyhow::Result<()> {
        Self::write_atomic_bytes(path, &serde_json::to_vec_pretty(value)?)
    }

    pub(crate) fn encode<T: Serialize>(path: &Path, value: &T) -> anyhow::Result<Vec<u8>> {
        let bytes = serde_json::to_vec_pretty(value)?;
        Self::check_size(path, bytes.len() as u64, MAX_JSON_BYTES)?;
        Ok(bytes)
    }

    pub(crate) fn write_atomic_bytes(path: &Path, bytes: &[u8]) -> anyhow::Result<()> {
        Self::write_bytes_with_limit(path, bytes, MAX_JSON_BYTES)
    }

    fn check_size(path: &Path, size: u64, limit: u64) -> anyhow::Result<()> {
        if size > limit {
            anyhow::bail!(
                "JSON 数据文件超过 {} MB：{}",
                limit / (1024 * 1024),
                path.display()
            );
        }
        Ok(())
    }

    pub(crate) fn write_bytes_with_limit(
        path: &Path,
        bytes: &[u8],
        limit: u64,
    ) -> anyhow::Result<()> {
        Self::check_size(path, bytes.len() as u64, limit)?;
        let same_length =
            fs::metadata(path).is_ok_and(|metadata| metadata.len() == bytes.len() as u64);
        if same_length && fs::read(path).is_ok_and(|current| current == bytes) {
            secure_file(path)?;
            sync_parent_directory(path)?;
            return Ok(());
        }
        let temporary = temporary_path(path);
        {
            let mut file = create_private_file(&temporary)?;
            file.write_all(bytes)?;
            file.sync_all()?;
        }
        if let Err(error) = replace_file(&temporary, path) {
            let _ = fs::remove_file(&temporary);
            return Err(error);
        }
        secure_file(path)?;
        sync_parent_directory(path)?;
        Ok(())
    }
}

#[cfg(unix)]
pub(crate) fn sync_parent_directory(path: &Path) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::File::open(parent)?.sync_all()?;
    }
    Ok(())
}

#[cfg(not(unix))]
pub(crate) fn sync_parent_directory(_path: &Path) -> std::io::Result<()> {
    // Windows replacements use MOVEFILE_WRITE_THROUGH below.
    Ok(())
}

pub(crate) fn temporary_path(path: &Path) -> PathBuf {
    let name = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("data.json");
    path.with_file_name(format!(".{name}.{}.tmp", uuid::Uuid::new_v4().simple()))
}

fn create_private_file(path: &Path) -> std::io::Result<fs::File> {
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options.open(path)
}

#[cfg(unix)]
fn secure_file(path: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))
}

#[cfg(not(unix))]
fn secure_file(_path: &Path) -> std::io::Result<()> {
    Ok(())
}

#[cfg(windows)]
fn replace_file(source: &Path, target: &Path) -> anyhow::Result<()> {
    use std::os::windows::ffi::OsStrExt;
    use windows::{
        Win32::Storage::FileSystem::{
            MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH, MoveFileExW,
        },
        core::PCWSTR,
    };
    let source = source
        .as_os_str()
        .encode_wide()
        .chain([0])
        .collect::<Vec<_>>();
    let target = target
        .as_os_str()
        .encode_wide()
        .chain([0])
        .collect::<Vec<_>>();
    unsafe {
        MoveFileExW(
            PCWSTR(source.as_ptr()),
            PCWSTR(target.as_ptr()),
            MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
        )?;
    }
    Ok(())
}

#[cfg(not(windows))]
fn replace_file(source: &Path, target: &Path) -> anyhow::Result<()> {
    fs::rename(source, target)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_and_write_accept_the_same_exact_boundary() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("data.json");
        let text = "a".repeat(MAX_JSON_BYTES as usize - 2);
        JsonStore::write_atomic(&path, &text).unwrap();
        assert_eq!(fs::metadata(&path).unwrap().len(), MAX_JSON_BYTES);
        assert!(JsonStore::write_atomic(&path, &(text.clone() + "b")).is_err());
        assert_eq!(
            JsonStore::read_or_default(&path, String::new).unwrap(),
            text
        );
    }

    #[cfg(unix)]
    #[test]
    fn identical_write_keeps_existing_file() {
        use std::os::unix::fs::MetadataExt;

        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("data.json");
        JsonStore::write_atomic(&path, &serde_json::json!({ "value": 1 })).unwrap();
        let inode = fs::metadata(&path).unwrap().ino();

        JsonStore::write_atomic(&path, &serde_json::json!({ "value": 1 })).unwrap();

        assert_eq!(fs::metadata(path).unwrap().ino(), inode);
    }
}
