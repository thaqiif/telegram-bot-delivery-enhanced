use sha2::{Digest, Sha256};
use std::{
    fs::{self, File, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
};

#[derive(Debug)]
pub struct DurableFile {
    pub path: PathBuf,
    pub size: u64,
    pub sha256: [u8; 32],
}

fn sync_dir(path: &Path) -> io::Result<()> {
    File::open(path)?.sync_all()
}

pub fn temp_path(data_root: &Path, job_id: &str, file_id: &str) -> io::Result<PathBuf> {
    let directory = data_root.join("tmp").join(job_id);
    fs::create_dir_all(&directory)?;
    sync_dir(directory.parent().expect("temporary directory has parent"))?;
    Ok(directory.join(format!("{file_id}.part")))
}

pub fn finalize(
    data_root: &Path,
    job_id: &str,
    file_id: &str,
    part: &Path,
    size: u64,
    sha256: [u8; 32],
) -> io::Result<DurableFile> {
    OpenOptions::new().read(true).open(part)?.sync_data()?;
    sync_dir(part.parent().expect("part file has parent"))?;
    let files_root = data_root.join("files");
    let destination_directory = files_root.join(job_id);
    fs::create_dir_all(&destination_directory)?;
    sync_dir(&files_root)?;
    let final_path = destination_directory.join(file_id);
    fs::rename(part, &final_path)?;
    sync_dir(&destination_directory)?;
    Ok(DurableFile {
        path: final_path,
        size,
        sha256,
    })
}

pub fn persist(
    data_root: &Path,
    job_id: &str,
    file_id: &str,
    bytes: &[u8],
) -> io::Result<DurableFile> {
    let part = temp_path(data_root, job_id, file_id)?;
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&part)?;
    file.write_all(bytes)?;
    let digest: [u8; 32] = Sha256::digest(bytes).into();
    finalize(
        data_root,
        job_id,
        file_id,
        &part,
        bytes.len() as u64,
        digest,
    )
}
