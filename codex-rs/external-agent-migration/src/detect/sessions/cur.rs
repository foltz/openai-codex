use super::common::SessionFileCandidate;
use super::common::detect_recent_sessions;
use crate::model::ExternalAgentSessionImportLimits;
use crate::sessions::ExternalAgentSessionMigration;
use crate::sessions::SessionRecordFormat;
use std::collections::BTreeMap;
use std::collections::HashSet;
use std::fs;
use std::io;
use std::path::Path;
use std::path::PathBuf;

const MAX_CUR_PROJECT_PATH_PROBES: usize = 128;
const MAX_CUR_PROJECT_DIRECTORY_ENTRIES: usize = 4096;
const CUR_PROJECT_SEPARATORS: [&str; 11] =
    ["-", "_", ".", " ", "--", "..", "__", "  ", "+", "@", "&"];

pub fn detect_recent_cur_sessions(
    external_agent_home: &Path,
    codex_home: &Path,
) -> io::Result<Vec<ExternalAgentSessionMigration>> {
    detect_recent_cur_sessions_with_limits(
        external_agent_home,
        codex_home,
        ExternalAgentSessionImportLimits::default(),
    )
}

pub(crate) fn detect_recent_cur_sessions_with_limits(
    external_agent_home: &Path,
    codex_home: &Path,
    limits: ExternalAgentSessionImportLimits,
) -> io::Result<Vec<ExternalAgentSessionMigration>> {
    let projects_root = external_agent_home.join("projects");
    if !projects_root.is_dir() {
        return Ok(Vec::new());
    }

    let mut candidates = Vec::new();
    for project_entry in fs::read_dir(projects_root)? {
        let Ok(project_entry) = project_entry else {
            continue;
        };
        let project_storage = project_entry.path();
        if !project_storage.is_dir() {
            continue;
        }
        let fallback_cwd = cur_project_cwd(&project_storage, external_agent_home);
        for path in cur_transcript_files(&project_storage.join("agent-transcripts")) {
            candidates.push(SessionFileCandidate {
                path,
                fallback_cwd: fallback_cwd.clone(),
                record_format: SessionRecordFormat::Cur,
            });
        }
    }
    detect_recent_sessions(
        codex_home, candidates, /*require_existing_cwd*/ false, limits,
    )
}

fn cur_transcript_files(transcripts_root: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    let mut pending = vec![transcripts_root.to_path_buf()];
    while let Some(directory) = pending.pop() {
        let Ok(entries) = fs::read_dir(directory) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let Ok(file_type) = entry.file_type() else {
                continue;
            };
            if file_type.is_dir() {
                if entry.file_name() != "subagents" {
                    pending.push(path);
                }
            } else if file_type.is_file()
                && path.extension().and_then(|extension| extension.to_str()) == Some("jsonl")
            {
                files.push(path);
            }
        }
    }
    files.sort();
    files
}

fn cur_project_cwd(project_storage: &Path, external_agent_home: &Path) -> Option<PathBuf> {
    let encoded = project_storage.file_name()?.to_str()?;
    // Cursor stores projectless chats under this reserved project name.
    if encoded == "empty-window" {
        let external_agent_home = if external_agent_home.is_absolute() {
            external_agent_home.to_path_buf()
        } else {
            std::env::current_dir().ok()?.join(external_agent_home)
        };
        return external_agent_home.parent().map(Path::to_path_buf);
    }
    decode_cur_project_path(encoded)
}

fn decode_cur_project_path(encoded: &str) -> Option<PathBuf> {
    #[cfg(not(windows))]
    let path = PathBuf::from("/");

    #[cfg(windows)]
    let (encoded, path) = {
        let (drive, encoded) = decode_cur_windows_project_drive(encoded)?;
        (encoded, PathBuf::from(format!("{drive}:\\")))
    };

    let encoded = encoded.strip_prefix('-').unwrap_or(encoded);
    let components = encoded.split('-').collect::<Vec<_>>();
    if components.iter().any(|component| {
        component.is_empty()
            || matches!(*component, "." | "..")
            || component.contains(['/', '\\', ':'])
    }) {
        return None;
    }

    // Resolve each existing prefix before its descendants. Independent
    // punctuation in several ancestors must be combined, not guessed one
    // merge at a time. Exhausting either bound refuses the whole decode,
    // even when one candidate has already matched: uniqueness is unproven.
    let mut budget = CurProjectProbeBudget {
        paths: MAX_CUR_PROJECT_PATH_PROBES,
        entries: MAX_CUR_PROJECT_DIRECTORY_ENTRIES,
    };
    let mut pending = vec![(path, 0)];
    let mut visited = HashSet::new();
    let mut matched_path = None;
    while let Some((parent, offset)) = pending.pop() {
        if !visited.insert((parent.clone(), offset)) {
            continue;
        }
        for (candidate, next) in cur_project_children(&parent, &components, offset, &mut budget)? {
            if next == components.len() {
                if matched_path
                    .as_ref()
                    .is_some_and(|matched| matched != &candidate)
                {
                    return None;
                }
                matched_path = Some(candidate);
            } else {
                pending.push((candidate, next));
            }
        }
    }
    matched_path
}

struct CurProjectProbeBudget {
    paths: usize,
    entries: usize,
}

impl CurProjectProbeBudget {
    fn path(&mut self) -> Option<()> {
        self.paths = self.paths.checked_sub(1)?;
        Some(())
    }

    fn entry(&mut self) -> Option<()> {
        self.entries = self.entries.checked_sub(1)?;
        Some(())
    }
}

fn cur_project_children(
    parent: &Path,
    components: &[&str],
    offset: usize,
    budget: &mut CurProjectProbeBudget,
) -> Option<Vec<(PathBuf, usize)>> {
    let mut names = BTreeMap::<String, Vec<usize>>::new();
    for next in offset + 1..=components.len() {
        let suffix = &components[offset..next];
        if suffix.len() == 1 {
            names.entry(suffix[0].to_owned()).or_default().push(next);
        } else {
            for separator in CUR_PROJECT_SEPARATORS {
                let ends = names.entry(suffix.join(separator)).or_default();
                if !ends.contains(&next) {
                    ends.push(next);
                }
            }
        }
    }

    let mut children = Vec::new();
    // Small candidate sets are cheaper and independent of unrelated entries
    // in crowded temporary ancestors. Reserve half the remaining probes for
    // descendants; larger sets use bounded directory enumeration instead.
    if names.len() <= budget.paths / 2 {
        for (name, ends) in names {
            budget.path()?;
            let candidate = parent.join(name);
            if candidate.is_dir() {
                children.extend(ends.into_iter().map(|next| (candidate.clone(), next)));
            }
        }
        return Some(children);
    }
    budget.path()?;
    match fs::read_dir(parent) {
        Ok(entries) => {
            for entry in entries {
                budget.entry()?;
                let entry = entry.ok()?;
                let name = entry.file_name();
                let Some(name) = name.to_str() else { continue };
                let Some(ends) = names.get(name) else {
                    continue;
                };
                budget.path()?;
                let candidate = entry.path();
                if candidate.is_dir() {
                    children.extend(ends.iter().map(|next| (candidate.clone(), *next)));
                }
            }
        }
        Err(error) if error.kind() == io::ErrorKind::PermissionDenied => {
            // Search-only ancestors can be traversed but not enumerated.
            // Probe the bounded candidate names without requiring directory
            // read permission; ambiguity and budget exhaustion still refuse.
            for (name, ends) in names {
                budget.path()?;
                let candidate = parent.join(name);
                if candidate.is_dir() {
                    children.extend(ends.into_iter().map(|next| (candidate.clone(), next)));
                }
            }
        }
        Err(_) => return None,
    }
    Some(children)
}

#[cfg(any(windows, test))]
fn decode_cur_windows_project_drive(encoded: &str) -> Option<(char, &str)> {
    let drive = encoded.as_bytes().first().copied()?;
    if !drive.is_ascii_alphabetic() || encoded.as_bytes().get(1) != Some(&b'-') {
        return None;
    }

    Some((char::from(drive), encoded.get(2..)?))
}

#[cfg(test)]
#[path = "cur_tests.rs"]
mod tests;
