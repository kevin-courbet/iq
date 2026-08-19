use anyhow::{Context, Result};
use std::ffi::OsStr;
use std::fs::{self, File, OpenOptions};
use std::os::fd::AsRawFd;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

pub(crate) struct DirectoryHandle {
    path: PathBuf,
    directory: File,
    device: u64,
    inode: u64,
}

pub fn remove_directory_with_identity(
    path: &Path,
    expected_device: u64,
    expected_inode: u64,
    label: &str,
) -> Result<()> {
    let directory = DirectoryHandle::open(path, label)?;
    if (directory.device, directory.inode) != (expected_device, expected_inode) {
        anyhow::bail!("{label} identity changed before cleanup");
    }
    directory.remove(label)
}

impl DirectoryHandle {
    pub(crate) fn open(path: &Path, label: &str) -> Result<Self> {
        let metadata = fs::symlink_metadata(path)
            .with_context(|| format!("inspect {label} {}", path.display()))?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            anyhow::bail!("open {label}: not a real directory");
        }
        let path = path.canonicalize()?;
        let directory = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(&path)
            .with_context(|| format!("open {label} {}", path.display()))?;
        Ok(Self {
            path,
            directory,
            device: metadata.dev(),
            inode: metadata.ino(),
        })
    }

    pub(crate) fn open_child(parent: &File, name: &OsStr, label: &str) -> Result<Self> {
        if name.is_empty() || Path::new(name).components().count() != 1 {
            anyhow::bail!("{label} name is invalid");
        }
        let parent_path = fs::read_link(format!("/proc/self/fd/{}", parent.as_raw_fd()))?;
        Self::open(&parent_path.join(name), label)
    }

    pub(crate) fn directory(&self) -> &File {
        &self.directory
    }

    pub(crate) fn open_file(&self, name: &OsStr, label: &str) -> Result<File> {
        if name.is_empty() || Path::new(name).components().count() != 1 {
            anyhow::bail!("{label} name is invalid");
        }
        let path = self.path.join(name);
        let metadata = fs::symlink_metadata(&path)?;
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            anyhow::bail!("{label} is not a regular file");
        }
        OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(path)
            .with_context(|| format!("open {label}"))
    }

    pub(crate) fn remove(self, label: &str) -> Result<()> {
        let metadata = fs::symlink_metadata(&self.path)?;
        if metadata.file_type().is_symlink()
            || !metadata.is_dir()
            || (metadata.dev(), metadata.ino()) != (self.device, self.inode)
        {
            anyhow::bail!("{label} identity changed before cleanup");
        }
        fs::remove_dir_all(&self.path).with_context(|| format!("remove {label}"))
    }
}
