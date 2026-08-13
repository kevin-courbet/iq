use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashSet};
use std::ffi::{CString, OsStr};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::fd::{AsRawFd, FromRawFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Component, Path, PathBuf};
use uuid::Uuid;

use crate::control_domain::{
    require_exact_text, require_sha, EncodedPath, ExactEffortIdentity, GuidanceAlternatives,
    PROTOCOL_VERSION,
};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentInput {
    pub version: u32,
    pub identity: ExactEffortIdentity,
    pub repository: RepositoryIdentity,
    pub source: SourceVariant,
    pub landing: LandingVariant,
    pub base_sha: String,
    pub rift: RiftIdentity,
    pub conflicts: Vec<ConflictEntry>,
    pub prior_outcomes: Vec<PriorOutcome>,
    pub validation_evidence: Vec<BoundedEvidence>,
    pub instructions: Vec<InstructionIdentity>,
    pub limits: ProtocolLimits,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RepositoryIdentity {
    pub repo_key: String,
    pub target_branch: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SourceVariant {
    RemoteBranch { branch: String, sha: String },
    LocalSubmission { submission_id: String, sha: String },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum LandingVariant {
    Direct,
    Provider { url: String },
    Squash,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RiftIdentity {
    pub rift_id: String,
    pub source_rift_id: String,
    pub relative_path: EncodedPath,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConflictEntry {
    pub path: EncodedPath,
    pub base_blob: Option<String>,
    pub target_blob: Option<String>,
    pub source_blob: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PriorOutcome {
    pub cycle_id: String,
    pub kind: String,
    pub evidence: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BoundedEvidence {
    pub kind: String,
    pub text: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstructionIdentity {
    pub path: EncodedPath,
    pub sha256: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProtocolLimits {
    pub max_result_bytes: u64,
    pub max_text_bytes: u64,
    pub max_paths: u32,
    pub max_evidence_entries: u32,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PublicationManifest {
    version: u32,
    files: BTreeMap<String, PublishedFile>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PublishedFile {
    size: u64,
    sha256: String,
}

pub struct PreparedPublication {
    directory: PathBuf,
    name: String,
    state: crate::control_domain::AtomicResultState,
}

impl PreparedPublication {
    pub fn state(&self) -> &crate::control_domain::AtomicResultState {
        &self.state
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
pub enum AgentResult {
    Resolved(ResolvedResult),
    GuidanceRequired(GuidanceResult),
    MechanicalFailure(MechanicalFailureResult),
}

impl AgentResult {
    pub fn identity(&self) -> &ExactEffortIdentity {
        match self {
            Self::Resolved(result) => &result.identity,
            Self::GuidanceRequired(result) => &result.identity,
            Self::MechanicalFailure(result) => &result.identity,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolvedResult {
    pub version: u32,
    pub identity: ExactEffortIdentity,
    pub staged_tree_sha256: String,
    pub changed_paths: Vec<EncodedPath>,
    pub checks: Vec<CheckEvidence>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CheckEvidence {
    pub command: String,
    pub status: CheckStatus,
    pub summary: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckStatus {
    Passed,
    Failed,
    NotRun,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GuidanceResult {
    pub version: u32,
    pub identity: ExactEffortIdentity,
    pub question: String,
    pub affected_contracts: Vec<String>,
    pub affected_paths: Vec<EncodedPath>,
    pub alternatives: GuidanceAlternatives,
    pub evidence: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MechanicalFailureResult {
    pub version: u32,
    pub identity: ExactEffortIdentity,
    pub operation: String,
    pub reason: String,
    pub evidence: String,
    pub rift_inspectable: bool,
}

impl AgentInput {
    pub fn validate(&self) -> Result<()> {
        if self.version != PROTOCOL_VERSION {
            anyhow::bail!("unsupported integration-agent input version");
        }
        validate_identity(&self.identity)?;
        require_exact_text(&self.repository.repo_key, "repository identity")?;
        require_exact_text(&self.repository.target_branch, "target branch")?;
        require_sha(&self.base_sha, "base SHA")?;
        validate_paths(
            self.conflicts.iter().map(|entry| &entry.path),
            self.limits.max_paths,
        )?;
        validate_paths(
            self.instructions.iter().map(|entry| &entry.path),
            self.limits.max_paths,
        )?;
        if self.validation_evidence.len() > self.limits.max_evidence_entries as usize {
            anyhow::bail!("validation evidence exceeds protocol entry limit");
        }
        if self.prior_outcomes.len() > self.limits.max_evidence_entries as usize {
            anyhow::bail!("prior outcomes exceed protocol entry limit");
        }
        for outcome in &self.prior_outcomes {
            require_exact_text(&outcome.cycle_id, "prior outcome cycle ID")?;
            require_exact_text(&outcome.kind, "prior outcome kind")?;
            require_bounded(
                &outcome.evidence,
                self.limits.max_text_bytes,
                "prior outcome evidence",
            )?;
        }
        for entry in &self.validation_evidence {
            require_exact_text(&entry.kind, "validation evidence kind")?;
            require_bounded(
                &entry.text,
                self.limits.max_text_bytes,
                "validation evidence",
            )?;
        }
        for instruction in &self.instructions {
            require_digest(&instruction.sha256, "instruction digest")?;
        }
        Ok(())
    }
}

pub fn parse_result(bytes: &[u8], input: &AgentInput) -> Result<AgentResult> {
    if bytes.len() as u64 > input.limits.max_result_bytes {
        anyhow::bail!("integration-agent result exceeds configured limit");
    }
    let result: AgentResult =
        serde_json::from_slice(bytes).context("parse strict integration-agent result")?;
    if result.identity() != &input.identity {
        anyhow::bail!("integration-agent result identity does not match cycle input");
    }
    match &result {
        AgentResult::Resolved(value) => {
            require_version(value.version)?;
            require_digest(&value.staged_tree_sha256, "staged-tree digest")?;
            validate_paths(value.changed_paths.iter(), input.limits.max_paths)?;
            if value.checks.len() > input.limits.max_evidence_entries as usize {
                anyhow::bail!("check evidence exceeds protocol entry limit");
            }
            for check in &value.checks {
                require_bounded(&check.command, input.limits.max_text_bytes, "check command")?;
                require_bounded(&check.summary, input.limits.max_text_bytes, "check summary")?;
            }
        }
        AgentResult::GuidanceRequired(value) => {
            require_version(value.version)?;
            require_bounded(
                &value.question,
                input.limits.max_text_bytes,
                "guidance question",
            )?;
            if value.affected_contracts.is_empty() || value.affected_paths.is_empty() {
                anyhow::bail!("guidance requires affected contracts and paths");
            }
            validate_paths(value.affected_paths.iter(), input.limits.max_paths)?;
            match &value.alternatives {
                GuidanceAlternatives::Explicit { values } if values.len() >= 2 => {
                    let mut unique = HashSet::new();
                    for alternative in values {
                        require_bounded(
                            alternative,
                            input.limits.max_text_bytes,
                            "guidance alternative",
                        )?;
                        if !unique.insert(alternative) {
                            anyhow::bail!("guidance alternatives must be unique");
                        }
                    }
                }
                GuidanceAlternatives::FreeText => {}
                _ => anyhow::bail!("guidance requires two alternatives or explicit free text"),
            }
            require_bounded(
                &value.evidence,
                input.limits.max_text_bytes,
                "guidance evidence",
            )?;
        }
        AgentResult::MechanicalFailure(value) => {
            require_version(value.version)?;
            require_bounded(
                &value.operation,
                input.limits.max_text_bytes,
                "mechanical operation",
            )?;
            require_bounded(
                &value.reason,
                input.limits.max_text_bytes,
                "mechanical reason",
            )?;
            require_bounded(
                &value.evidence,
                input.limits.max_text_bytes,
                "mechanical evidence",
            )?;
        }
    }
    Ok(result)
}

pub fn atomic_write_json<T: Serialize>(directory: &Path, name: &str, value: &T) -> Result<PathBuf> {
    let bytes = serde_json::to_vec_pretty(value)?;
    atomic_write(directory, name, &bytes)
}

pub fn atomic_write(directory: &Path, name: &str, bytes: &[u8]) -> Result<PathBuf> {
    let directory_file = open_protocol_directory(directory)?;
    validate_leaf(name)?;
    let temporary = format!(".{name}.tmp-{}", Uuid::new_v4());
    let mut file = create_file_at(&directory_file, &temporary)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    rename_at(&directory_file, &temporary, name)?;
    directory_file.sync_all()?;
    Ok(directory.join(name))
}

pub fn read_complete_result(directory: &Path, name: &str, max_bytes: u64) -> Result<Vec<u8>> {
    let directory_file = open_protocol_directory(directory)?;
    validate_leaf(name)?;
    let mut file = open_file_at(&directory_file, name)?;
    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.nlink() != 1 || metadata.len() > max_bytes {
        anyhow::bail!("agent result must be a bounded single-link regular file");
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    file.read_to_end(&mut bytes)?;
    let after = file.metadata()?;
    if (
        metadata.dev(),
        metadata.ino(),
        metadata.len(),
        metadata.nlink(),
    ) != (after.dev(), after.ino(), after.len(), after.nlink())
    {
        anyhow::bail!("agent result changed while reading");
    }
    Ok(bytes)
}

pub fn sha256_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn validate_identity(identity: &ExactEffortIdentity) -> Result<()> {
    for (value, label) in [
        (&identity.effort_id, "effort ID"),
        (&identity.item_id, "item ID"),
        (&identity.attempt_id, "attempt ID"),
        (&identity.cycle_id, "cycle ID"),
    ] {
        require_exact_text(value, label)?;
    }
    require_sha(&identity.target_sha, "target SHA")?;
    require_sha(&identity.source_sha, "source SHA")?;
    if let Some(candidate) = &identity.candidate_sha {
        require_sha(candidate, "candidate SHA")?;
    }
    Ok(())
}

fn validate_paths<'a>(paths: impl Iterator<Item = &'a EncodedPath>, max: u32) -> Result<()> {
    let mut unique = HashSet::new();
    for (count, path) in paths.enumerate() {
        if count >= max as usize {
            anyhow::bail!("protocol path list exceeds configured limit");
        }
        path.to_bytes()?;
        if !unique.insert(path) {
            anyhow::bail!("protocol path list contains a duplicate");
        }
    }
    Ok(())
}

fn require_version(version: u32) -> Result<()> {
    if version != PROTOCOL_VERSION {
        anyhow::bail!("unsupported integration-agent result version");
    }
    Ok(())
}

fn require_digest(value: &str, label: &str) -> Result<()> {
    if value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        anyhow::bail!("{label} must be a SHA-256 digest");
    }
    Ok(())
}

fn require_bounded(value: &str, max: u64, label: &str) -> Result<()> {
    require_exact_text(value, label)?;
    if value.len() as u64 > max {
        anyhow::bail!("{label} exceeds configured text limit");
    }
    Ok(())
}

fn open_protocol_directory(path: &Path) -> Result<File> {
    let file = open_absolute_directory(path)?;
    verify_private_directory(&file, "protocol directory")?;
    Ok(file)
}

fn validate_leaf(name: &str) -> Result<()> {
    if name.is_empty() || name.as_bytes().contains(&b'/') || name == "." || name == ".." {
        anyhow::bail!("invalid protocol file name");
    }
    Ok(())
}

fn create_file_at(directory: &File, name: &str) -> Result<File> {
    let name = CString::new(name)?;
    let fd = unsafe {
        libc::openat(
            directory.as_raw_fd(),
            name.as_ptr(),
            libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            0o600,
        )
    };
    if fd < 0 {
        return Err(std::io::Error::last_os_error()).context("create protocol file");
    }
    Ok(unsafe { File::from_raw_fd(fd) })
}

fn open_file_at(directory: &File, name: &str) -> Result<File> {
    let name = CString::new(name)?;
    let fd = unsafe {
        libc::openat(
            directory.as_raw_fd(),
            name.as_ptr(),
            libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        )
    };
    if fd < 0 {
        return Err(std::io::Error::last_os_error()).context("open protocol file");
    }
    Ok(unsafe { File::from_raw_fd(fd) })
}

fn rename_at(directory: &File, from: &str, to: &str) -> Result<()> {
    let from = std::ffi::CString::new(from)?;
    let to = std::ffi::CString::new(to)?;
    if unsafe {
        libc::renameat(
            directory.as_raw_fd(),
            from.as_ptr(),
            directory.as_raw_fd(),
            to.as_ptr(),
        )
    } != 0
    {
        return Err(std::io::Error::last_os_error()).context("publish protocol file");
    }
    Ok(())
}

pub fn protocol_directory(workspace: &Path, cycle_id: &str) -> Result<PathBuf> {
    require_exact_text(cycle_id, "cycle ID")?;
    if cycle_id.as_bytes().contains(&b'/') {
        anyhow::bail!("cycle ID is not a valid path component");
    }
    let workspace_file = open_absolute_directory(workspace)?;
    let root = workspace.join(".iq-agent-protocol");
    let root_file = create_or_open_private_directory(&workspace_file, ".iq-agent-protocol")?;
    let directory = root.join(cycle_id);
    create_private_directory(&root_file, cycle_id)?;
    Ok(directory)
}

pub fn prepare_publication(protocol: &Path) -> Result<PreparedPublication> {
    let protocol_file =
        open_protocol_directory(protocol).context("open publication protocol directory")?;
    let name = format!(".publication-{}", Uuid::new_v4());
    let directory_file = create_private_directory(&protocol_file, &name)?;
    let metadata = directory_file.metadata()?;
    Ok(PreparedPublication {
        directory: protocol.join(&name),
        name,
        state: crate::control_domain::AtomicResultState::Writing {
            device: metadata.dev(),
            inode: metadata.ino(),
        },
    })
}

pub fn publish_result_bundle(
    protocol: &Path,
    prepared: PreparedPublication,
    export: &Path,
    max_result_bytes: u64,
    max_total_bytes: u64,
) -> Result<crate::control_domain::AtomicResultState> {
    let protocol_file = open_protocol_directory(protocol)?;
    let publication_file = open_protocol_directory(&prepared.directory)
        .context("open prepared publication directory")?;
    let metadata = publication_file.metadata()?;
    let crate::control_domain::AtomicResultState::Writing { device, inode } = prepared.state else {
        unreachable!("prepared publication always has writing identity")
    };
    if (metadata.dev(), metadata.ino()) != (device, inode) {
        anyhow::bail!("prepared publication directory identity changed");
    }
    let export_file = open_protocol_directory(export).context("open trusted sandbox export")?;
    let limits = [
        ("result.json", max_result_bytes),
        ("staged.patch", max_total_bytes),
        ("staged.paths", 1024 * 1024),
        ("unstaged.paths", 1024 * 1024),
        ("staged.tree", 129),
        ("head", 129),
        ("refs", 1024 * 1024),
        ("config", 1024 * 1024),
        ("remotes", 1024 * 1024),
    ];
    let mut total = 0_u64;
    let mut files = BTreeMap::new();
    for (name, maximum) in limits {
        let mut source = open_file_at(&export_file, name)
            .with_context(|| format!("open staged publication file {name}"))?;
        let source_metadata = source.metadata()?;
        if !source_metadata.is_file()
            || source_metadata.nlink() != 1
            || source_metadata.len() > maximum
        {
            anyhow::bail!("staged publication file {name} has invalid identity or size");
        }
        total = total
            .checked_add(source_metadata.len())
            .context("published result size overflow")?;
        if total > max_total_bytes {
            anyhow::bail!("published result bundle exceeds configured writable bound");
        }
        let mut destination = create_file_at(&publication_file, name)?;
        let mut digest = Sha256::new();
        let copied = std::io::copy(
            &mut std::io::Read::by_ref(&mut source).take(maximum + 1),
            &mut DigestWriter {
                file: &mut destination,
                digest: &mut digest,
            },
        )?;
        if copied != source_metadata.len() {
            anyhow::bail!("staged publication file {name} changed while copying");
        }
        destination.sync_all()?;
        files.insert(
            name.to_string(),
            PublishedFile {
                size: copied,
                sha256: format!("{:x}", digest.finalize()),
            },
        );
    }
    let manifest = PublicationManifest { version: 1, files };
    let manifest_bytes = serde_json::to_vec(&manifest)?;
    let mut manifest_file = create_file_at(&publication_file, "manifest.json")?;
    manifest_file.write_all(&manifest_bytes)?;
    manifest_file.sync_all()?;
    publication_file.sync_all()?;
    rename_at(&protocol_file, &prepared.name, "publication")?;
    protocol_file.sync_all()?;
    let published = open_directory_at(&protocol_file, "publication")?;
    let published_metadata = published.metadata()?;
    if (published_metadata.dev(), published_metadata.ino()) != (device, inode) {
        anyhow::bail!("published result directory identity changed");
    }
    Ok(crate::control_domain::AtomicResultState::Complete {
        device,
        inode,
        sha256: sha256_hex(&manifest_bytes),
    })
}

pub fn read_published_result(
    protocol: &Path,
    max_result_bytes: u64,
    max_total_bytes: u64,
) -> Result<Option<(Vec<u8>, PathBuf, crate::control_domain::AtomicResultState)>> {
    let protocol_file = open_protocol_directory(protocol)?;
    let publication_file = match open_directory_at(&protocol_file, "publication") {
        Ok(file) => file,
        Err(error)
            if error
                .downcast_ref::<std::io::Error>()
                .is_some_and(|error| error.kind() == std::io::ErrorKind::NotFound) =>
        {
            return Ok(None)
        }
        Err(error) => return Err(error).context("open completed result publication"),
    };
    verify_private_directory(&publication_file, "result publication")?;
    let manifest_bytes = read_bounded_file_at(&publication_file, "manifest.json", 64 * 1024)?;
    let manifest: PublicationManifest =
        crate::control_domain::parse_strict_json(&manifest_bytes, "result publication manifest")?;
    if manifest.version != 1 {
        anyhow::bail!("unsupported result publication manifest version");
    }
    let limits = BTreeMap::from([
        ("result.json", max_result_bytes),
        ("staged.patch", max_total_bytes),
        ("staged.paths", 1024 * 1024),
        ("unstaged.paths", 1024 * 1024),
        ("staged.tree", 129),
        ("head", 129),
        ("refs", 1024 * 1024),
        ("config", 1024 * 1024),
        ("remotes", 1024 * 1024),
    ]);
    if manifest.files.len() != limits.len()
        || manifest
            .files
            .keys()
            .any(|name| !limits.contains_key(name.as_str()))
    {
        anyhow::bail!("result publication manifest has unexpected files");
    }
    let mut total = 0_u64;
    let mut result = None;
    for (name, maximum) in limits {
        let bytes = read_bounded_file_at(&publication_file, name, maximum)?;
        let identity = manifest
            .files
            .get(name)
            .context("result publication manifest is incomplete")?;
        if identity.size != bytes.len() as u64 || identity.sha256 != sha256_hex(&bytes) {
            anyhow::bail!("published file {name} differs from its durable manifest");
        }
        total = total
            .checked_add(identity.size)
            .context("published result size overflow")?;
        if total > max_total_bytes {
            anyhow::bail!("published result bundle exceeds configured writable bound");
        }
        if name == "result.json" {
            result = Some(bytes);
        }
    }
    let metadata = publication_file.metadata()?;
    Ok(Some((
        result.context("published result is absent")?,
        protocol.join("publication"),
        crate::control_domain::AtomicResultState::Complete {
            device: metadata.dev(),
            inode: metadata.ino(),
            sha256: sha256_hex(&manifest_bytes),
        },
    )))
}

pub fn remove_protocol_cycle(workspace: &Path, cycle_id: &str) -> Result<()> {
    validate_leaf(cycle_id)?;
    let workspace_file = open_absolute_directory(workspace)?;
    let root_file = match open_directory_at(&workspace_file, ".iq-agent-protocol") {
        Ok(file) => file,
        Err(error) if is_not_found(&error) => return Ok(()),
        Err(error) => return Err(error).context("open protocol root for removal"),
    };
    verify_private_directory(&root_file, "protocol root")?;
    let cycle_file = match open_directory_at(&root_file, cycle_id) {
        Ok(file) => file,
        Err(error) if is_not_found(&error) => return Ok(()),
        Err(error) => return Err(error).context("open protocol cycle for removal"),
    };
    verify_private_directory(&cycle_file, "protocol cycle")?;
    let quarantine = format!(".remove-{cycle_id}-{}", Uuid::new_v4());
    rename_at(&root_file, cycle_id, &quarantine)?;
    root_file.sync_all()?;
    fs::remove_dir_all(workspace.join(".iq-agent-protocol").join(&quarantine))?;
    root_file.sync_all()?;
    Ok(())
}

fn is_not_found(error: &anyhow::Error) -> bool {
    error
        .downcast_ref::<std::io::Error>()
        .is_some_and(|error| error.kind() == std::io::ErrorKind::NotFound)
}

fn open_absolute_directory(path: &Path) -> Result<File> {
    if !path.is_absolute() {
        anyhow::bail!("directory path must be absolute");
    }
    let mut current = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open("/")?;
    for component in path.components() {
        match component {
            Component::RootDir => {}
            Component::Normal(name) => {
                let next = open_directory_at_os(&current, name)?;
                let metadata = next.metadata()?;
                if !metadata.is_dir() {
                    anyhow::bail!("path component is not a directory");
                }
                current = next;
            }
            _ => anyhow::bail!("directory path contains a non-normal component"),
        }
    }
    Ok(current)
}

fn create_or_open_private_directory(parent: &File, name: &str) -> Result<File> {
    match create_private_directory(parent, name) {
        Ok(file) => Ok(file),
        Err(error)
            if error
                .downcast_ref::<std::io::Error>()
                .and_then(std::io::Error::raw_os_error)
                == Some(libc::EEXIST) =>
        {
            let file = open_directory_at(parent, name)?;
            verify_private_directory(&file, "protocol root")?;
            Ok(file)
        }
        Err(error) => Err(error),
    }
}

fn create_private_directory(parent: &File, name: &str) -> Result<File> {
    validate_leaf(name)?;
    let name_c = CString::new(name)?;
    if unsafe { libc::mkdirat(parent.as_raw_fd(), name_c.as_ptr(), 0o700) } != 0 {
        return Err(std::io::Error::last_os_error()).context("create private protocol directory");
    }
    let file = open_directory_at(parent, name)?;
    verify_private_directory(&file, "created protocol directory")?;
    Ok(file)
}

fn open_directory_at(parent: &File, name: &str) -> Result<File> {
    validate_leaf(name)?;
    open_directory_at_os(parent, OsStr::new(name))
}

fn open_directory_at_os(parent: &File, name: &OsStr) -> Result<File> {
    let name = CString::new(name.as_bytes())?;
    let fd = unsafe {
        libc::openat(
            parent.as_raw_fd(),
            name.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        )
    };
    if fd < 0 {
        return Err(std::io::Error::last_os_error()).context("open directory component");
    }
    Ok(unsafe { File::from_raw_fd(fd) })
}

fn verify_private_directory(file: &File, label: &str) -> Result<()> {
    let metadata = file.metadata()?;
    if !metadata.is_dir()
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.permissions().mode() & 0o777 != 0o700
    {
        anyhow::bail!("{label} must be an owned mode-0700 real directory");
    }
    Ok(())
}

fn read_bounded_file_at(directory: &File, name: &str, max_bytes: u64) -> Result<Vec<u8>> {
    let mut file = open_file_at(directory, name)?;
    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.nlink() != 1 || metadata.len() > max_bytes {
        anyhow::bail!("published file {name} has invalid identity or size");
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    file.read_to_end(&mut bytes)?;
    let after = file.metadata()?;
    if (
        metadata.dev(),
        metadata.ino(),
        metadata.len(),
        metadata.nlink(),
    ) != (after.dev(), after.ino(), after.len(), after.nlink())
    {
        anyhow::bail!("published file {name} changed while reading");
    }
    Ok(bytes)
}

struct DigestWriter<'a> {
    file: &'a mut File,
    digest: &'a mut Sha256,
}

impl Write for DigestWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let written = self.file.write(bytes)?;
        self.digest.update(&bytes[..written]);
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush()
    }
}

pub fn is_protocol_path(path: &OsStr) -> bool {
    path.as_encoded_bytes().starts_with(b".iq-agent-protocol/")
        || path == OsStr::new(".iq-agent-protocol")
}
