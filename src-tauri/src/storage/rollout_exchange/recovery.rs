use super::{
    create_private_sibling, file_identity, is_same_file, move_into_private_sibling,
    rename_flag_unsupported,
};
use crate::storage::{
    codex_rollout_walks, create_private_file, sync_parent_directory, temporary_path,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeSet,
    fs,
    io::{self, Read, Write},
    path::{Component, Path, PathBuf},
};

const RECEIPT: &str = "pending.json";
const RETIRED: &str = "finished.json";
const CONTENT: &str = "prepared";
const OWNER: &str = "writer.json";
const ORIGINAL: &str = "original";
const LOCK: &str = "lock";

#[derive(Clone, Copy, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
enum Operation {
    Copy,
    RestoreOriginal,
}

#[derive(Deserialize, Serialize)]
struct Receipt {
    operation: Operation,
    filename: String,
    prepared_bytes: Option<u64>,
}

pub(super) struct PendingWrite {
    directory: PathBuf,
    target: PathBuf,
    operation: Operation,
    prepared_bytes: Option<u64>,
    lock: Option<fs::File>,
}

impl PendingWrite {
    pub(super) fn copy(source: &Path, target: &Path) -> anyhow::Result<Self> {
        let mut pending = Self::new(target, Operation::Copy)?;
        let prepared = pending.prepared();
        let mut output = create_private_file(&prepared)?;
        pending.prepared_bytes = Some(io::copy(&mut fs::File::open(source)?, &mut output)?);
        output.sync_all()?;
        drop(output);
        pending.record()?;
        Ok(pending)
    }

    pub(super) fn detach(target: &Path) -> anyhow::Result<Self> {
        let mut pending = Self::new(target, Operation::RestoreOriginal)?;
        pending.record()?;
        if let Err(error) = move_into_private_sibling(target, &pending.original()) {
            pending.finish();
            return Err(error.into());
        }
        Ok(pending)
    }

    fn new(target: &Path, operation: Operation) -> anyhow::Result<Self> {
        target_filename(target)?;
        let directory = create_private_sibling(target)?;
        let lock = lock_record(&directory.join(LOCK), true)?;
        Ok(Self {
            directory,
            target: target.to_owned(),
            operation,
            prepared_bytes: None,
            lock: Some(lock),
        })
    }

    fn record(&self) -> anyhow::Result<()> {
        self.write_receipt(RECEIPT)?;
        sync_name(&self.target)?;
        Ok(())
    }

    fn write_receipt(&self, name: &str) -> anyhow::Result<()> {
        write_record(
            &self.directory.join(name),
            &Receipt {
                operation: self.operation,
                filename: target_filename(&self.target)?.to_owned(),
                prepared_bytes: self.prepared_bytes,
            },
        )
    }

    pub(super) fn prepared(&self) -> PathBuf {
        self.directory.join(CONTENT)
    }

    pub(super) fn original(&self) -> PathBuf {
        self.directory.join(ORIGINAL)
    }

    pub(super) fn claim(&self, output: &fs::File) -> anyhow::Result<()> {
        // Record the identity of the descriptor opened with create_new before
        // the first byte lands at the public path.
        write_record(&self.directory.join(OWNER), &file_identity(output)?)
    }

    pub(super) fn finish(&mut self) {
        // Record that replay is no longer needed even if the old receipt is
        // locked or removing it fails. Leave that confirmation until cleanup
        // of the active record is durable.
        if let Err(error) = self.write_receipt(RETIRED) {
            tracing::warn!(%error, path = %self.directory.display(), "无法确认文件恢复记录已经结束");
        }
        self.cleanup_retired();
    }

    fn cleanup_retired(&mut self) {
        let active = self.directory.join(RECEIPT);
        match fs::remove_file(&active) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => {
                tracing::warn!(%error, path = %self.directory.display(), "文件恢复记录尚未清理");
                return;
            }
        }
        if let Err(error) = sync_name(&active) {
            tracing::warn!(%error, path = %self.directory.display(), "清理恢复记录后无法同步目录");
            return;
        }
        if self.operation == Operation::Copy {
            let _ = fs::remove_file(self.prepared());
            let _ = fs::remove_file(self.directory.join(OWNER));
        }
        let _ = fs::remove_file(self.directory.join(RETIRED));
        drop(self.lock.take());
        let _ = fs::remove_file(self.directory.join(LOCK));
        let _ = fs::remove_dir(&self.directory);
    }

    fn reopen(directory: PathBuf) -> anyhow::Result<Option<Self>> {
        let marker = directory.join(RECEIPT);
        let retired = directory.join(RETIRED);
        if !retired.try_exists()? && !marker.try_exists()? {
            return Ok(None);
        }
        let lock = match lock_record(&directory.join(LOCK), false) {
            Ok(lock) => lock,
            Err(error)
                if error.kind() == io::ErrorKind::NotFound
                    && !retired.try_exists()?
                    && !marker.try_exists()? =>
            {
                return Ok(None);
            }
            Err(error) => return Err(error.into()),
        };
        let completed = read_record::<Receipt>(&retired)?;
        let is_completed = completed.is_some();
        let receipt = match completed {
            Some(receipt) => Some(receipt),
            None => read_record(&marker)?,
        };
        let Some(receipt) = receipt else {
            return Ok(None);
        };
        let filename = Path::new(&receipt.filename);
        let mut components = filename.components();
        anyhow::ensure!(
            matches!(components.next(), Some(Component::Normal(_)))
                && components.next().is_none()
                && filename.file_name() == Some(filename.as_os_str()),
            "文件恢复记录的目标名称无效：{}",
            directory.display()
        );
        let prefix = format!(".{}.", receipt.filename);
        anyhow::ensure!(
            directory
                .file_name()
                .and_then(|name| name.to_str())
                .and_then(|name| name.strip_prefix(&prefix))
                .and_then(|name| name.strip_suffix(".codextools-original"))
                .is_some_and(|id| id.len() == 32 && id.bytes().all(|byte| byte.is_ascii_hexdigit())),
            "文件恢复记录与目录不匹配：{}",
            directory.display()
        );
        let target = directory
            .parent()
            .ok_or_else(|| anyhow::anyhow!("文件恢复目录缺少上级目录"))?
            .join(filename);
        let mut pending = Self {
            directory,
            target,
            operation: receipt.operation,
            prepared_bytes: receipt.prepared_bytes,
            lock: Some(lock),
        };
        if is_completed {
            pending.cleanup_retired();
            return Ok(None);
        }
        Ok(Some(pending))
    }

    fn resume_copy(
        &mut self,
        may_write: &mut impl FnMut() -> anyhow::Result<bool>,
    ) -> anyhow::Result<()> {
        match fs::symlink_metadata(&self.target) {
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                // A copy can resume only the inode it already owns. A missing
                // destination may be unpublished or have been rolled back.
                self.finish();
                return Ok(());
            }
            Err(error) => return Err(error.into()),
            Ok(metadata) => anyhow::ensure!(
                metadata.is_file(),
                "目标路径无法用于文件恢复：{}",
                self.target.display()
            ),
        }
        let owner = read_record::<super::FileIdentity>(&self.directory.join(OWNER))?;
        let mut output = fs::OpenOptions::new()
            .read(true)
            .append(true)
            .open(&self.target)?;
        let identity = file_identity(&output)?;
        anyhow::ensure!(
            owner.as_ref() == Some(&identity) && is_same_file(&output, &self.target)?,
            "无法确认目标属于未完成的写入，已保留当前文件：{}",
            self.target.display()
        );
        let mut prepared = open_regular(&self.prepared())?;
        let length = prepared.metadata()?.len();
        anyhow::ensure!(
            self.prepared_bytes == Some(length),
            "恢复副本的长度不完整：{}",
            self.prepared().display()
        );
        if self.target.extension().is_some_and(|ext| ext == "sqlite") {
            for suffix in ["-wal", "-shm", "-journal"] {
                let mut sidecar = self.target.as_os_str().to_os_string();
                sidecar.push(suffix);
                anyhow::ensure!(
                    !Path::new(&sidecar).try_exists()?,
                    "SQLite 历史库正在使用，已保留恢复文件：{}",
                    self.target.display()
                );
            }
        }
        let written = output.metadata()?.len();
        anyhow::ensure!(
            written <= length,
            "目标文件包含外部追加，已保留当前文件：{}",
            self.target.display()
        );
        compare_prefix(&mut prepared, &mut output, written)?;
        anyhow::ensure!(
            may_write()? && is_same_file(&output, &self.target)?,
            "恢复目标已变化，已保留待恢复文件"
        );
        io::copy(&mut prepared, &mut output)?;
        output.sync_all()?;
        anyhow::ensure!(
            is_same_file(&output, &self.target)? && output.metadata()?.len() == length,
            "恢复期间目标文件发生变化：{}",
            self.target.display()
        );
        sync_name(&self.target)?;
        drop(output);
        drop(prepared);
        self.finish();
        Ok(())
    }
}

fn lock_record(path: &Path, create: bool) -> io::Result<fs::File> {
    let mut options = fs::OpenOptions::new();
    options.read(true).write(true).create_new(create);
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        options.share_mode(0);
    }
    let file = options.open(path)?;
    #[cfg(unix)]
    {
        use std::os::fd::AsRawFd;
        unsafe extern "C" {
            fn flock(fd: i32, operation: i32) -> i32;
        }
        const LOCK_EXCLUSIVE: i32 = 2;
        const LOCK_NONBLOCKING: i32 = 4;
        // Dropping the descriptor also releases the lock, including on exit.
        if unsafe { flock(file.as_raw_fd(), LOCK_EXCLUSIVE | LOCK_NONBLOCKING) } == -1 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(file)
}

fn target_filename(target: &Path) -> anyhow::Result<&str> {
    target
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| anyhow::anyhow!("无法记录待恢复文件名：{}", target.display()))
}

fn open_regular(path: &Path) -> io::Result<fs::File> {
    if !fs::symlink_metadata(path)?.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "recovery file is not a regular file",
        ));
    }
    fs::File::open(path)
}

fn write_record(path: &Path, value: &impl Serialize) -> anyhow::Result<()> {
    let bytes = serde_json::to_vec(value)?;
    let temporary = temporary_path(path);
    let mut file = create_private_file(&temporary)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    drop(file);
    crate::storage::replace_file(&temporary, path)?;
    sync_name(path)?;
    Ok(())
}

fn read_record<T: serde::de::DeserializeOwned>(path: &Path) -> anyhow::Result<Option<T>> {
    let file = match open_regular(path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let mut bytes = Vec::new();
    file.take(4097).read_to_end(&mut bytes)?;
    anyhow::ensure!(
        bytes.len() <= 4096,
        "文件恢复记录超过长度限制：{}",
        path.display()
    );
    Ok(Some(serde_json::from_slice(&bytes)?))
}

pub(super) fn sync_name(path: &Path) -> io::Result<()> {
    // Some removable volumes cannot fsync directories. Synced regular files
    // and name changes still support recovery after a process interruption.
    match sync_parent_directory(path) {
        Err(error) if rename_flag_unsupported(&error) => Ok(()),
        result => result,
    }
}

fn compare_prefix(
    prepared: &mut fs::File,
    output: &mut fs::File,
    mut remaining: u64,
) -> anyhow::Result<()> {
    let mut expected = [0_u8; 64 * 1024];
    let mut actual = [0_u8; 64 * 1024];
    while remaining > 0 {
        let count = remaining.min(expected.len() as u64) as usize;
        prepared.read_exact(&mut expected[..count])?;
        output.read_exact(&mut actual[..count])?;
        anyhow::ensure!(
            expected[..count] == actual[..count],
            "目标文件包含外部修改，已保留当前内容"
        );
        remaining -= count as u64;
    }
    Ok(())
}

pub(super) fn recover(
    home: &Path,
    mut may_write: impl FnMut() -> anyhow::Result<bool>,
    publish: impl Fn(&Path, &Path) -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    let is_recovery_dir = |path: &Path| {
        path.extension()
            .is_some_and(|extension| extension == "codextools-original")
    };
    let mut directories = BTreeSet::new();
    if !home.try_exists()? {
        return Ok(());
    }
    for entry in fs::read_dir(home)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() && is_recovery_dir(&entry.path()) {
            directories.insert(entry.path().canonicalize()?);
        }
    }
    for walk in codex_rollout_walks(home) {
        let mut entries = walk.into_iter();
        while let Some(entry) = entries.next() {
            let Ok(entry) = entry else { continue };
            if entry.file_type().is_dir() && is_recovery_dir(entry.path()) {
                entries.skip_current_dir();
                directories.insert(entry.path().canonicalize()?);
            }
        }
    }
    let mut pending = Vec::new();
    for directory in directories {
        pending.extend(PendingWrite::reopen(directory)?);
    }
    // A partially copied replacement must finish before handling its displaced original.
    pending.sort_by_key(|pending| pending.operation);
    for mut entry in pending {
        anyhow::ensure!(may_write()?, "请退出 Codex 后恢复未完成的文件写入");
        let result = match entry.operation {
            Operation::Copy => entry.resume_copy(&mut may_write),
            Operation::RestoreOriginal => (|| -> anyhow::Result<()> {
                let original = entry.original();
                if !entry.target.try_exists()? && original.try_exists()? {
                    publish(&original, &entry.target)?;
                    sync_name(&entry.target)?;
                }
                entry.finish();
                Ok(())
            })(),
        };
        result.map_err(|error| {
            anyhow::anyhow!(
                "无法恢复 {}（{error}）。恢复文件保留在 {}。",
                entry.target.display(),
                entry.directory.display()
            )
        })?;
    }
    Ok(())
}
