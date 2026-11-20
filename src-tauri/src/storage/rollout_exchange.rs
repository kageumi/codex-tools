use super::{
    ConditionalWrite, FilePublication, UnfinishedFilePublication, create_private_file,
    sync_parent_directory, temporary_path,
};
use std::{
    fs, io,
    path::{Path, PathBuf},
};

mod recovery;

#[cfg(target_os = "macos")]
pub(super) fn recover_pending(
    home: &Path,
    may_write: impl FnMut() -> anyhow::Result<bool>,
) -> anyhow::Result<()> {
    recovery::recover(home, may_write, publish_if_absent)
}

// A rename alone strands writes made through an earlier file descriptor. Swap
// names first so both inodes survive, then examine the displaced inode after
// Darwin has established that nobody has it open.
#[cfg(target_os = "macos")]
pub(super) fn replace(
    path: &Path,
    staged: &Path,
    expected: &[u8],
    replacement: &[u8],
) -> anyhow::Result<ConditionalWrite> {
    checked_exchange(
        path,
        staged,
        expected,
        replacement,
        swap,
        publish_if_absent,
        |path, bytes| discard_closed_file_if(path, |file| Ok(fs::read(file)? == bytes)),
        sync_parent_directory,
    )
}

#[allow(clippy::too_many_arguments)] // Inject each filesystem step independently in fault tests.
fn checked_exchange(
    path: &Path,
    staged: &Path,
    expected: &[u8],
    replacement: &[u8],
    mut swap: impl FnMut(&Path, &Path) -> io::Result<()>,
    publish: impl FnMut(&Path, &Path) -> anyhow::Result<()>,
    discard: impl Fn(&Path, &[u8]) -> anyhow::Result<DisplacedFile>,
    sync_directory: impl FnOnce(&Path) -> io::Result<()>,
) -> anyhow::Result<ConditionalWrite> {
    if let Err(error) = swap(path, staged) {
        if rename_flag_unsupported(&error) {
            return replace_using_moves(path, staged, expected, publish, sync_directory);
        }
        let _ = fs::remove_file(staged); // Still our unpublished candidate.
        return Err(error.into());
    }
    let checked = discard(staged, expected);
    match checked {
        Ok(DisplacedFile::Removed) => {
            return Ok(ConditionalWrite::Written {
                retained: None,
                publication: FilePublication::from_directory_sync(sync_directory(path)),
            });
        }
        Ok(DisplacedFile::Retained) => {
            return Ok(retained_replacement(path, staged, sync_directory));
        }
        _ => {}
    }
    // Put the actual original (including any appends) back. If anyone also
    // wrote to the briefly published candidate, retain that inode as well.
    swap(path, staged).map_err(|e| {
        anyhow::anyhow!("无法恢复会话原路径，原文件保留在 {}：{e}", staged.display())
    })?;
    if !matches!(discard(staged, replacement), Ok(DisplacedFile::Removed)) {
        anyhow::bail!(
            "会话已恢复；另一份可能仍在写入的文件保留在 {}",
            staged.display()
        );
    }
    sync_directory(path)?;
    checked.map(|_| ConditionalWrite::Changed)
}

fn replace_using_moves(
    path: &Path,
    staged: &Path,
    expected: &[u8],
    mut publish: impl FnMut(&Path, &Path) -> anyhow::Result<()>,
    sync_directory: impl FnOnce(&Path) -> io::Result<()>,
) -> anyhow::Result<ConditionalWrite> {
    let mut pending = match recovery::PendingWrite::detach(path) {
        Ok(pending) => pending,
        Err(error) => {
            let _ = fs::remove_file(staged);
            return Err(error);
        }
    };
    let displaced = pending.original();
    let published = (|| -> anyhow::Result<bool> {
        if !fs::symlink_metadata(&displaced)?.is_file() || fs::read(&displaced)? != expected {
            return Ok(false);
        }
        publish(staged, path)?;
        Ok(true)
    })();
    let _ = fs::remove_file(staged);
    if matches!(published, Ok(true)) {
        pending.finish();
        return Ok(retained_replacement(path, &displaced, sync_directory));
    }

    publish(&displaced, path).map_err(|error| {
        anyhow::anyhow!(
            "无法恢复原路径，原文件保留在 {}：{error}",
            displaced.display()
        )
    })?;
    recovery::sync_name(path)?;
    pending.finish();
    published.map(|_| ConditionalWrite::Changed)
}

fn create_private_sibling(path: &Path) -> io::Result<PathBuf> {
    let directory = temporary_path(path).with_extension("codextools-original");
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        fs::DirBuilder::new().mode(0o700).create(&directory)?;
    }
    #[cfg(not(unix))]
    fs::create_dir(&directory)?;
    Ok(directory)
}

pub(super) fn save_in_private_sibling(path: &Path) -> io::Result<PathBuf> {
    // Rename into a directory we exclusively created on this same volume.
    // This keeps the real inode without requiring hard links or RENAME_EXCL.
    let directory = create_private_sibling(path)?;
    let retained = directory.join("original");
    if let Err(error) = move_into_private_sibling(path, &retained) {
        let _ = fs::remove_dir(directory);
        return Err(error);
    }
    Ok(retained)
}

fn move_into_private_sibling(path: &Path, retained: &Path) -> io::Result<()> {
    fs::rename(path, retained)?;
    if let Err(error) = recovery::sync_name(retained) {
        tracing::warn!(%error, path = %retained.display(), "原文件已移入恢复目录，但目录同步失败");
    }
    Ok(())
}

#[cfg(target_os = "macos")]
pub(super) fn finish_private_sibling(path: &Path, retained: &Path) -> io::Result<()> {
    match fs::remove_dir(retained.parent().expect("private sibling has a parent")) {
        Ok(()) => sync_parent_directory(path),
        Err(error) if error.kind() == io::ErrorKind::DirectoryNotEmpty => {
            // The original was copied back on a volume without exclusive rename.
            // Keep its inode, since an already open writer may still be using it.
            report_retained_file(path, retained);
            Ok(())
        }
        Err(error) => Err(error),
    }
}

fn rename_flag_unsupported(error: &io::Error) -> bool {
    error.kind() == io::ErrorKind::InvalidInput || operation_unsupported(error)
}

fn operation_unsupported(error: &io::Error) -> bool {
    #[cfg(target_os = "macos")]
    {
        // Darwin uses distinct errnos. Rust 1.85 classifies them as Uncategorized.
        const ENOTSUP: i32 = 45;
        const EOPNOTSUPP: i32 = 102;
        if matches!(error.raw_os_error(), Some(ENOTSUP | EOPNOTSUPP)) {
            return true;
        }
    }
    error.kind() == io::ErrorKind::Unsupported
}

#[cfg(target_os = "macos")]
pub(super) fn publish_if_absent(source: &Path, target: &Path) -> anyhow::Result<()> {
    publish_if_absent_with(source, target, rename_if_absent, |input, output| {
        io::copy(input, output).map(|_| ())
    })
}

fn publish_if_absent_with(
    source: &Path,
    target: &Path,
    rename: impl FnOnce(&Path, &Path) -> io::Result<()>,
    copy: impl FnOnce(&mut fs::File, &mut fs::File) -> io::Result<()>,
) -> anyhow::Result<()> {
    match rename(source, target) {
        Ok(()) => return Ok(()),
        Err(error) if rename_flag_unsupported(&error) => {}
        Err(error) => return Err(error.into()),
    }
    // Some removable volumes support neither exclusive rename nor hard links.
    // Stage a recoverable copy before reserving the public name.
    let mut pending = recovery::PendingWrite::copy(source, target)?;
    let mut input = fs::File::open(pending.prepared())?;
    let mut output = match create_private_file(target) {
        Ok(output) => output,
        Err(error) => {
            pending.finish();
            return Err(error.into());
        }
    };
    let copied = pending
        .claim(&output)
        .map_err(io::Error::other)
        .and_then(|()| copy(&mut input, &mut output))
        .and_then(|()| output.sync_all());
    match is_same_file(&output, target) {
        Ok(true) => {}
        Ok(false) => {
            drop(input);
            pending.finish();
            anyhow::bail!("目标文件已被外部替换：{}", target.display());
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            drop(input);
            pending.finish();
            anyhow::bail!("目标文件已被外部移走：{}", target.display());
        }
        Err(quarantine_error) => {
            return Err(UnfinishedFilePublication {
                path: target.to_owned(),
                error: copied
                    .err()
                    .unwrap_or_else(|| io::Error::other("无法核对目标文件")),
                quarantine_error,
            }
            .into());
        }
    }
    if let Err(error) = copied {
        let retained = match save_in_private_sibling(target) {
            Ok(retained) => retained,
            Err(quarantine_error) => {
                return Err(UnfinishedFilePublication {
                    path: target.to_owned(),
                    error,
                    quarantine_error,
                }
                .into());
            }
        };
        drop(input);
        pending.finish();
        report_retained_file(target, &retained);
        anyhow::bail!(
            "写入未完成（{error}），换出的文件保留在 {}。",
            retained.display()
        );
    }
    drop(input);
    if let Err(error) = recovery::sync_name(target) {
        tracing::warn!(%error, path = %target.display(), "文件已写入，目录同步失败");
    }
    pending.finish();
    Ok(())
}

#[derive(serde::Deserialize, serde::Serialize, PartialEq, Eq)]
struct FileIdentity {
    device: u64,
    number: u64,
    created: Option<std::time::SystemTime>,
}

fn is_same_file(handle: &fs::File, path: &Path) -> io::Result<bool> {
    Ok(fs::symlink_metadata(path)?.is_file()
        && file_identity(handle)? == file_identity(&fs::File::open(path)?)?)
}

#[cfg(unix)]
fn file_identity(handle: &fs::File) -> io::Result<FileIdentity> {
    use std::os::unix::fs::MetadataExt;

    let file = handle.metadata()?;
    Ok(FileIdentity {
        device: file.dev(),
        number: file.ino(),
        created: file.created().ok(),
    })
}

#[cfg(all(windows, test))]
fn file_identity(handle: &fs::File) -> io::Result<FileIdentity> {
    use std::os::windows::io::AsRawHandle;
    use windows::Win32::{
        Foundation::HANDLE,
        Storage::FileSystem::{BY_HANDLE_FILE_INFORMATION, GetFileInformationByHandle},
    };

    let mut info = BY_HANDLE_FILE_INFORMATION::default();
    unsafe { GetFileInformationByHandle(HANDLE(handle.as_raw_handle()), &mut info) }
        .map_err(io::Error::other)?;
    Ok(FileIdentity {
        device: u64::from(info.dwVolumeSerialNumber),
        number: (u64::from(info.nFileIndexHigh) << 32) | u64::from(info.nFileIndexLow),
        created: handle.metadata()?.created().ok(),
    })
}

pub(super) fn report_retained_file(path: &Path, retained: &Path) {
    warn_retained_file(path, retained);
    FilePublication::from_directory_sync(sync_parent_directory(path)).warn_if_unsynced(path);
}

fn warn_retained_file(path: &Path, retained: &Path) {
    tracing::warn!(
        path = %path.display(),
        original = %retained.display(),
        "文件系统无法核实原文件已关闭，已保留原文件"
    );
}

fn retained_replacement(
    path: &Path,
    displaced: &Path,
    sync_directory: impl FnOnce(&Path) -> io::Result<()>,
) -> ConditionalWrite {
    warn_retained_file(path, displaced);
    ConditionalWrite::Written {
        retained: Some(displaced.to_owned()),
        publication: FilePublication::from_directory_sync(sync_directory(path)),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum DisplacedFile {
    Removed,
    Retained,
    Changed,
}

fn discard_if_closed_and_unchanged(
    displaced: &Path,
    unchanged: impl FnOnce(&Path) -> anyhow::Result<bool>,
    link: impl FnOnce(&Path, &Path) -> io::Result<()>,
    unlink_if_closed: impl FnOnce(&Path) -> io::Result<()>,
) -> anyhow::Result<DisplacedFile> {
    // Use a second link for the kernel's busy check. The original is still
    // named afterwards so a write completed just before the check is visible.
    let probe = temporary_path(displaced);
    match link(displaced, &probe) {
        Ok(()) => {}
        Err(error) if operation_unsupported(&error) => {
            // Without a second name we cannot safely remove an inode that a
            // pre-existing handle may still write to. Keep the actual inode.
            return if fs::symlink_metadata(displaced)?.is_file() && unchanged(displaced)? {
                Ok(DisplacedFile::Retained)
            } else {
                Ok(DisplacedFile::Changed)
            };
        }
        Err(error) => return Err(error.into()),
    }
    if let Err(error) = unlink_if_closed(&probe) {
        fs::remove_file(&probe)?;
        return if error.kind() == io::ErrorKind::ResourceBusy {
            Ok(DisplacedFile::Changed)
        } else {
            Err(error.into())
        };
    }
    #[cfg(target_os = "macos")]
    {
        use std::os::unix::fs::MetadataExt;
        let metadata = fs::symlink_metadata(displaced)?;
        if !metadata.is_file() || metadata.nlink() != 1 {
            return Ok(DisplacedFile::Changed); // Other paths could still admit writers.
        }
    }
    if !unchanged(displaced)? {
        return Ok(DisplacedFile::Changed);
    }
    fs::remove_file(displaced)?;
    Ok(DisplacedFile::Removed)
}

#[cfg(target_os = "macos")]
pub(super) fn discard_closed_file_if(
    path: &Path,
    unchanged: impl FnOnce(&Path) -> anyhow::Result<bool>,
) -> anyhow::Result<DisplacedFile> {
    discard_if_closed_and_unchanged(
        path,
        unchanged,
        |a, b| fs::hard_link(a, b),
        unlink_if_closed,
    )
}

#[cfg(target_os = "macos")]
fn c_path(path: &Path) -> io::Result<std::ffi::CString> {
    use std::os::unix::ffi::OsStrExt;
    Ok(std::ffi::CString::new(path.as_os_str().as_bytes())?)
}

#[cfg(target_os = "macos")]
fn rename_if_absent(source: &Path, target: &Path) -> io::Result<()> {
    const RENAME_EXCL: u32 = 4;
    rename_with_flags(source, target, RENAME_EXCL)
}

#[cfg(target_os = "macos")]
fn swap(a: &Path, b: &Path) -> io::Result<()> {
    const RENAME_SWAP: u32 = 2;
    rename_with_flags(a, b, RENAME_SWAP)
}

#[cfg(target_os = "macos")]
fn rename_with_flags(a: &Path, b: &Path, flags: u32) -> io::Result<()> {
    unsafe extern "C" {
        fn renamex_np(a: *const std::ffi::c_char, b: *const std::ffi::c_char, flags: u32) -> i32;
    }
    let (a, b) = (c_path(a)?, c_path(b)?);
    if unsafe { renamex_np(a.as_ptr(), b.as_ptr(), flags) } == -1 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(target_os = "macos")]
fn unlink_if_closed(path: &Path) -> io::Result<()> {
    unsafe extern "C" {
        fn unlinkat(dirfd: i32, path: *const std::ffi::c_char, flags: i32) -> i32;
        // libSystem's Carbon-semantics delete uses VNODE_REMOVE_NODELETEBUSY
        // on macOS 11 and later, including kernels predating AT_NODELETEBUSY.
        fn __delete(path: *const std::ffi::c_char) -> i32;
    }
    const AT_FDCWD: i32 = -2;
    const AT_NODELETEBUSY: i32 = 0x4000;
    let path = c_path(path)?;
    if unsafe { unlinkat(AT_FDCWD, path.as_ptr(), AT_NODELETEBUSY) } == -1 {
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::InvalidInput {
            return Err(error);
        }
        if unsafe { __delete(path.as_ptr()) } == -1 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(not(target_os = "macos"))]
    use std::cell::Cell;
    use std::io::Write;

    #[cfg(not(target_os = "macos"))]
    fn swap(a: &Path, b: &Path) -> io::Result<()> {
        // Deterministic substitute for the single Darwin syscall. Writer
        // actions are injected only at its boundaries.
        let saved = temporary_path(a);
        fs::rename(a, &saved)?;
        fs::rename(b, a)?;
        fs::rename(saved, b)
    }

    fn publish_without_rename(source: &Path, target: &Path) -> anyhow::Result<()> {
        publish_if_absent_with(
            source,
            target,
            |_, _| Err(io::ErrorKind::Unsupported.into()),
            |input, output| io::copy(input, output).map(|_| ()),
        )
    }

    fn discard_for_test(
        path: &Path,
        expected: &[u8],
        unlink: impl FnOnce(&Path) -> io::Result<()>,
    ) -> anyhow::Result<DisplacedFile> {
        discard_if_closed_and_unchanged(
            path,
            |path| Ok(fs::read(path)? == expected),
            |a, b| fs::hard_link(a, b),
            unlink,
        )
    }

    #[test]
    fn exchange_reports_publication_even_if_directory_sync_fails() {
        for swap_supported in [true, false] {
            let temp = tempfile::tempdir().unwrap();
            let live = temp.path().join("config.toml");
            let staged = temp.path().join("replacement");
            fs::write(&live, b"original").unwrap();
            fs::write(&staged, b"replacement").unwrap();
            let result = checked_exchange(
                &live,
                &staged,
                b"original",
                b"replacement",
                |a, b| {
                    if swap_supported {
                        swap(a, b)
                    } else {
                        Err(io::ErrorKind::Unsupported.into())
                    }
                },
                publish_without_rename,
                |file, bytes| discard_for_test(file, bytes, |p| fs::remove_file(p)),
                |path| {
                    assert_eq!(fs::read(path)?, b"replacement");
                    Err(io::Error::other("injected directory sync failure"))
                },
            )
            .unwrap();
            let ConditionalWrite::Written {
                retained,
                publication: FilePublication::DirectorySyncFailed(_),
            } = result
            else {
                panic!("replacement is already published");
            };
            assert_eq!(retained.is_none(), swap_supported);
            assert_eq!(fs::read(&live).unwrap(), b"replacement");
        }
    }

    #[test]
    fn rollout_exchange_preserves_appends_completed_during_publication() {
        let temp = tempfile::tempdir().unwrap();
        let live = temp.path().join("rollout.jsonl");
        let staged = temp.path().join("replacement");
        for both_writers in [false, true] {
            fs::write(&live, b"original").unwrap();
            fs::write(&staged, b"replacement").unwrap();
            let mut original_writer =
                Some(fs::OpenOptions::new().append(true).open(&live).unwrap());
            let result = checked_exchange(
                &live,
                &staged,
                b"original",
                b"replacement",
                |a, b| {
                    swap(a, b)?;
                    if let Some(mut writer) = original_writer.take() {
                        writer.write_all(b"+chat")?;
                        if both_writers {
                            fs::OpenOptions::new()
                                .append(true)
                                .open(a)?
                                .write_all(b"+other")?;
                        }
                    }
                    Ok(())
                },
                publish_without_rename,
                |path, bytes| discard_for_test(path, bytes, |p| fs::remove_file(p)),
                sync_parent_directory,
            );
            assert_eq!(fs::read(&live).unwrap(), b"original+chat");
            if both_writers {
                assert!(
                    result
                        .unwrap_err()
                        .to_string()
                        .contains(&staged.display().to_string())
                );
                assert_eq!(fs::read(&staged).unwrap(), b"replacement+other");
            } else {
                assert!(!result.unwrap().written());
                assert!(!staged.exists());
            }
        }
    }

    #[test]
    fn rollout_exchange_puts_an_open_writer_back_at_the_live_path() {
        let temp = tempfile::tempdir().unwrap();
        let live = temp.path().join("rollout.jsonl");
        let staged = temp.path().join("replacement");
        fs::write(&live, b"original").unwrap();
        fs::write(&staged, b"replacement").unwrap();
        let mut writer = fs::OpenOptions::new().append(true).open(&live).unwrap();
        #[cfg(target_os = "macos")]
        let closed = unlink_if_closed;
        #[cfg(not(target_os = "macos"))]
        let closed = {
            let first = Cell::new(true);
            move |p: &Path| {
                if first.replace(false) {
                    Err(io::ErrorKind::ResourceBusy.into())
                } else {
                    fs::remove_file(p)
                }
            }
        };
        let result = checked_exchange(
            &live,
            &staged,
            b"original",
            b"replacement",
            swap,
            publish_without_rename,
            |path, expected| discard_for_test(path, expected, &closed),
            sync_parent_directory,
        )
        .unwrap();
        assert!(!result.written());
        writer.write_all(b"+later chat").unwrap();
        assert_eq!(fs::read(&live).unwrap(), b"original+later chat");
        assert!(!staged.exists());
    }

    #[test]
    fn volumes_without_hard_links_can_replace_a_rollout_and_keep_its_actual_original() {
        for swap_supported in [true, false] {
            let temp = tempfile::tempdir().unwrap();
            let live = temp.path().join("rollout.jsonl");
            let staged = temp.path().join("replacement");
            fs::write(&live, b"original").unwrap();
            fs::write(&staged, b"replacement").unwrap();
            let mut writer = fs::OpenOptions::new().append(true).open(&live).unwrap();
            let result = checked_exchange(
                &live,
                &staged,
                b"original",
                b"replacement",
                |a, b| {
                    if swap_supported {
                        swap(a, b)
                    } else {
                        Err(io::ErrorKind::Unsupported.into())
                    }
                },
                publish_without_rename,
                |file, bytes| {
                    discard_if_closed_and_unchanged(
                        file,
                        |path| Ok(fs::read(path)? == bytes),
                        |_, _| Err(io::ErrorKind::Unsupported.into()),
                        |_| panic!("a failed link must never lead to removing the original"),
                    )
                },
                sync_parent_directory,
            )
            .unwrap();
            let ConditionalWrite::Written {
                retained: Some(retained),
                ..
            } = result
            else {
                panic!("replacement must keep the original when hard links are unavailable");
            };
            writer.write_all(b"+late append").unwrap();
            drop(writer);
            assert_eq!(fs::read(&live).unwrap(), b"replacement");
            assert_eq!(fs::read(&retained).unwrap(), b"original+late append");
            assert!(retained.starts_with(temp.path()));
            assert_eq!(fs::read_dir(temp.path()).unwrap().count(), 2);
        }
    }

    #[test]
    fn unsupported_swap_never_overwrites_a_recreated_live_name() {
        let temp = tempfile::tempdir().unwrap();
        let live = temp.path().join("rollout.jsonl");
        let staged = temp.path().join("replacement");
        fs::write(&live, b"original").unwrap();
        fs::write(&staged, b"replacement").unwrap();
        let mut retained = None;
        let error = checked_exchange(
            &live,
            &staged,
            b"original",
            b"replacement",
            |_, _| Err(io::ErrorKind::Unsupported.into()),
            |source, target| {
                if source == staged {
                    fs::write(&live, b"independently recreated")?;
                } else {
                    retained = Some(source.to_owned());
                }
                publish_without_rename(source, target)
            },
            |_, _| panic!("the unsupported exchange must use the non-destructive fallback"),
            sync_parent_directory,
        )
        .unwrap_err();
        let retained = retained.unwrap();
        assert!(error.to_string().contains(&retained.display().to_string()));
        assert_eq!(fs::read(&live).unwrap(), b"independently recreated");
        assert_eq!(fs::read(&retained).unwrap(), b"original");
        assert!(!staged.exists());
    }

    #[test]
    fn exclusive_copy_publishes_new_history_without_overwriting_an_existing_database() {
        let temp = tempfile::tempdir().unwrap();
        let staged = temp.path().join("staged.sqlite");
        let target = temp.path().join("history.sqlite");
        let source = rusqlite::Connection::open(&staged).unwrap();
        source
            .execute_batch("CREATE TABLE messages (id INTEGER); INSERT INTO messages VALUES (42)")
            .unwrap();
        drop(source);
        publish_without_rename(&staged, &target).unwrap();
        let original = fs::read(&target).unwrap();
        fs::write(&staged, b"independent overwrite attempt").unwrap();
        assert!(publish_without_rename(&staged, &target).is_err());
        assert_eq!(fs::read(&target).unwrap(), original);
        assert_eq!(
            rusqlite::Connection::open(&target)
                .unwrap()
                .query_row("SELECT id FROM messages", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            42
        );
    }

    #[test]
    fn exclusive_copy_failure_removes_partial_history_from_the_live_name() {
        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("staged.sqlite");
        let target = temp.path().join("history.sqlite");
        fs::write(&source, b"complete history").unwrap();
        let error = publish_if_absent_with(
            &source,
            &target,
            |_, _| Err(io::ErrorKind::Unsupported.into()),
            |_, output| {
                output.write_all(b"partial")?;
                Err(io::ErrorKind::StorageFull.into())
            },
        )
        .unwrap_err();
        let retained = fs::read_dir(temp.path())
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .find(|path| path.is_dir())
            .unwrap()
            .join("original");
        assert!(error.to_string().contains(&retained.display().to_string()));
        assert!(!target.exists());
        assert_eq!(fs::read(retained).unwrap(), b"partial");
        assert_eq!(fs::read(source).unwrap(), b"complete history");
    }

    #[test]
    fn exclusive_copy_does_not_write_through_a_recreated_path() {
        let temp = tempfile::tempdir().unwrap();
        let staged = temp.path().join("replacement");
        let live = temp.path().join("history.sqlite");
        let moved = temp.path().join("moved");
        fs::write(&staged, b"fixture replacement").unwrap();
        let result = publish_if_absent_with(
            &staged,
            &live,
            |_, _| Err(io::ErrorKind::Unsupported.into()),
            |input, output| {
                fs::rename(&live, &moved)?;
                fs::write(&live, b"independent writer")?;
                io::copy(input, output)?;
                Ok(())
            },
        );
        assert!(result.unwrap_err().to_string().contains("已被外部替换"));
        assert_eq!(fs::read(live).unwrap(), b"independent writer");
        assert_eq!(fs::read(moved).unwrap(), b"fixture replacement");
    }

    const INTERRUPTED_FIXTURE: &str = "CODEX_TOOLS_INTERRUPTED_WRITE_TEST";

    fn child_fixture() -> Option<PathBuf> {
        std::env::var_os(INTERRUPTED_FIXTURE).map(|path| {
            let path = PathBuf::from(path).canonicalize().unwrap();
            assert_eq!(path.file_name().unwrap(), "interrupted-write-fixture");
            assert!(path.starts_with(std::env::temp_dir().canonicalize().unwrap()));
            path
        })
    }

    fn interrupt_child(fixture: &Path, test: &str) {
        let child = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", test])
            .env(INTERRUPTED_FIXTURE, fixture)
            .output()
            .unwrap();
        assert_eq!(
            child.status.code(),
            Some(73),
            "{}{}",
            String::from_utf8_lossy(&child.stdout),
            String::from_utf8_lossy(&child.stderr)
        );
    }

    #[test]
    fn interrupted_history_copy_is_restored_on_the_next_startup() {
        use std::io::Read;

        if let Some(home) = child_fixture() {
            publish_if_absent_with(
                &home.join("staged.sqlite"),
                &home.join("thread_history_1.sqlite"),
                |_, _| Err(io::ErrorKind::Unsupported.into()),
                |input, output| {
                    let mut prefix = [0_u8; 32];
                    input.read_exact(&mut prefix)?;
                    output.write_all(&prefix)?;
                    output.sync_all()?;
                    std::process::exit(73);
                },
            )
            .unwrap();
            panic!("the compatibility copy did not run");
        }

        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("interrupted-write-fixture");
        fs::create_dir(&home).unwrap();
        let staged = home.join("staged.sqlite");
        let target = home.join("thread_history_1.sqlite");
        let source = rusqlite::Connection::open(&staged).unwrap();
        source
            .execute_batch("CREATE TABLE messages (id INTEGER); INSERT INTO messages VALUES (42)")
            .unwrap();
        drop(source);

        interrupt_child(
            &home,
            "storage::rollout_exchange::tests::interrupted_history_copy_is_restored_on_the_next_startup",
        );
        assert_eq!(fs::metadata(&target).unwrap().len(), 32);
        recovery::recover(&home, || Ok(true), publish_without_rename).unwrap();
        recovery::recover(
            &home,
            || panic!("completed records must not replay"),
            publish_without_rename,
        )
        .unwrap();
        assert_eq!(fs::read(&target).unwrap(), fs::read(staged).unwrap());
        let database = rusqlite::Connection::open(target).unwrap();
        assert_eq!(
            database
                .query_row("PRAGMA integrity_check", [], |row| row.get::<_, String>(0))
                .unwrap(),
            "ok"
        );
        assert_eq!(
            database
                .query_row("SELECT id FROM messages", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            42
        );
    }

    #[test]
    fn interrupted_rollout_move_restores_the_original_name_on_startup() {
        let original = b"{\"type\":\"session_meta\",\"payload\":{\"id\":\"fixture\"}}\n";
        let rollout_name = Path::new("sessions/2026/11/rollout-fixture.jsonl");
        if let Some(home) = child_fixture() {
            let live = home.join(rollout_name);
            checked_exchange(
                &live,
                &home.join("staged"),
                original,
                b"replacement",
                |_, _| Err(io::ErrorKind::Unsupported.into()),
                |_, target| {
                    assert!(!target.exists());
                    std::process::exit(73);
                },
                |_, _| panic!("the native exchange is unavailable in this fixture"),
                sync_parent_directory,
            )
            .unwrap();
            panic!("the compatibility move did not run");
        }

        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("interrupted-write-fixture");
        let rollout = home.join(rollout_name);
        fs::create_dir_all(rollout.parent().unwrap()).unwrap();
        fs::write(&rollout, original).unwrap();
        fs::write(home.join("staged"), b"replacement").unwrap();
        interrupt_child(
            &home,
            "storage::rollout_exchange::tests::interrupted_rollout_move_restores_the_original_name_on_startup",
        );
        assert!(!rollout.exists());
        assert!(recovery::recover(&home, || Ok(false), publish_without_rename).is_err());
        assert!(!rollout.exists());
        recovery::recover(&home, || Ok(true), publish_without_rename).unwrap();
        assert_eq!(fs::read(&rollout).unwrap(), original);
        assert_eq!(crate::provider_sync::rollout_files(&home), vec![rollout]);
    }

    #[test]
    fn recovery_preserves_an_independently_recreated_file() {
        let temp = tempfile::tempdir().unwrap();
        let staged = temp.path().join("fixture-source");
        let target = temp.path().join("thread_history_1.sqlite");
        fs::write(&staged, b"complete history").unwrap();
        let pending = recovery::PendingWrite::copy(&staged, &target).unwrap();
        let mut output = create_private_file(&target).unwrap();
        pending.claim(&output).unwrap();
        output.write_all(b"complete").unwrap();
        drop(output);
        let moved = temp.path().join("external-rename");
        fs::rename(&target, &moved).unwrap();
        fs::write(&target, b"external bytes").unwrap();
        assert!(recovery::recover(temp.path(), || Ok(true), publish_without_rename).is_err());
        drop(pending);
        assert!(recovery::recover(temp.path(), || Ok(true), publish_without_rename).is_err());
        assert_eq!(fs::read(target).unwrap(), b"external bytes");
        assert_eq!(fs::read(moved).unwrap(), b"complete");
    }

    #[test]
    fn recovery_leaves_an_unowned_empty_target_untouched() {
        let temp = tempfile::tempdir().unwrap();
        let staged = temp.path().join("fixture-source");
        let target = temp.path().join("thread_history_1.sqlite");
        fs::write(&staged, b"complete history").unwrap();
        let pending = recovery::PendingWrite::copy(&staged, &target).unwrap();
        let prepared = pending.prepared();
        drop(pending);
        let external = create_private_file(&target).unwrap();
        let identity = file_identity(&external).unwrap();
        drop(external);

        assert!(recovery::recover(temp.path(), || Ok(true), publish_without_rename).is_err());
        assert_eq!(fs::metadata(&target).unwrap().len(), 0);
        assert!(file_identity(&fs::File::open(&target).unwrap()).unwrap() == identity);
        assert_eq!(fs::read(prepared).unwrap(), b"complete history");
    }

    #[test]
    fn legacy_copy_receipt_cannot_recreate_a_rolled_back_history_database() {
        let temp = tempfile::tempdir().unwrap();
        let staged = temp.path().join("fixture-source");
        let target = temp.path().join("thread_history_1.sqlite");
        let history = b"published fixture history";
        fs::write(&staged, history).unwrap();
        let pending = recovery::PendingWrite::copy(&staged, &target).unwrap();
        let mut output = create_private_file(&target).unwrap();
        pending.claim(&output).unwrap();
        output.write_all(history).unwrap();
        output.sync_all().unwrap();
        drop(output);
        // Model an old receipt left behind after copy completion.
        let prepared = pending.prepared();
        drop(pending);
        crate::storage::remove_created_file_if_unchanged(&target, |path| {
            Ok(fs::read(path)? == history)
        })
        .unwrap();

        recovery::recover(temp.path(), || Ok(true), publish_without_rename).unwrap();
        assert!(!target.exists());
        assert!(!prepared.exists());
    }

    #[cfg(windows)]
    #[test]
    fn copy_completion_survives_a_lock_on_receipt_cleanup() {
        use std::os::windows::fs::OpenOptionsExt;

        let temp = tempfile::tempdir().unwrap();
        let staged = temp.path().join("fixture-source");
        let target = temp.path().join("thread_history_1.sqlite");
        let history = b"published fixture history";
        fs::write(&staged, history).unwrap();
        let mut pending = recovery::PendingWrite::copy(&staged, &target).unwrap();
        let active = pending.prepared().with_file_name("pending.json");
        let mut output = create_private_file(&target).unwrap();
        pending.claim(&output).unwrap();
        output.write_all(history).unwrap();
        output.sync_all().unwrap();
        drop(output);
        let locked = fs::OpenOptions::new()
            .read(true)
            .share_mode(0)
            .open(&active)
            .unwrap();
        pending.finish();
        drop(pending);
        assert!(active.is_file());

        fs::write(&target, b"changed after a successful copy").unwrap();
        recovery::recover(
            temp.path(),
            || panic!("committed copy receipt must be ignored"),
            publish_without_rename,
        )
        .unwrap();
        assert_eq!(
            fs::read(&target).unwrap(),
            b"changed after a successful copy"
        );

        fs::remove_file(&target).unwrap();
        drop(locked);
        recovery::recover(
            temp.path(),
            || panic!("committed copy receipt must be ignored after rollback"),
            publish_without_rename,
        )
        .unwrap();
        assert!(!target.exists());
        assert!(!active.exists());
    }

    #[test]
    fn interrupted_rollouts_in_linked_session_roots_are_recovered_once() {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("codex-home");
        let moved = temp.path().join("moved-sessions");
        fs::create_dir(&home).unwrap();
        fs::create_dir(&moved).unwrap();
        for name in ["sessions", "archived_sessions"] {
            let link = home.join(name);
            #[cfg(unix)]
            std::os::unix::fs::symlink(&moved, link).unwrap();
            #[cfg(windows)]
            {
                let result = std::process::Command::new("powershell.exe")
                    .args(["-NoProfile", "-Command",
                        "$null = New-Item -ItemType Junction -Path $env:CODEX_TOOLS_TEST_LINK -Target $env:CODEX_TOOLS_TEST_LINK_TARGET -ErrorAction Stop"])
                    .env("CODEX_TOOLS_TEST_LINK", link)
                    .env("CODEX_TOOLS_TEST_LINK_TARGET", &moved)
                    .output()
                    .unwrap();
                assert!(
                    result.status.success(),
                    "{}",
                    String::from_utf8_lossy(&result.stderr)
                );
            }
        }
        let relative = "rollout-fixture.jsonl";
        let rollout = home.join("sessions").join(relative);
        let original = b"{\"type\":\"session_meta\",\"payload\":{\"id\":\"fixture\"}}\n";
        fs::write(&rollout, original).unwrap();
        assert_eq!(
            crate::provider_sync::rollout_files(&home),
            vec![rollout.clone()]
        );
        let detached = recovery::PendingWrite::detach(&rollout).unwrap();
        drop(detached);
        assert!(!moved.join(relative).exists());

        recovery::recover(&home, || Ok(true), publish_without_rename).unwrap();
        assert_eq!(fs::read(&rollout).unwrap(), original);
        assert_eq!(crate::provider_sync::rollout_files(&home), vec![rollout]);
        assert_eq!(
            fs::read(home.join("archived_sessions").join(relative)).unwrap(),
            original
        );
    }
}
