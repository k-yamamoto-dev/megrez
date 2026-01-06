use std::fs::File;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::path::Path;

use anyhow::Result;

pub fn stream_file(path: &Path, writer: &mut impl Write) -> Result<()> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    io::copy(&mut reader, writer)?;
    Ok(())
}

pub fn is_stdin_path(path: &Path) -> bool {
    path.as_os_str() == "-"
}

pub fn read_prefix<R: Read>(reader: &mut R, limit: usize) -> Result<Vec<u8>> {
    let mut buf = vec![0u8; limit];
    let mut total = 0usize;
    while total < limit {
        let read = reader.read(&mut buf[total..])?;
        if read == 0 {
            break;
        }
        total += read;
    }
    buf.truncate(total);
    Ok(buf)
}

pub fn peek_first_non_ws<R: BufRead>(reader: &mut R) -> Result<Option<u8>> {
    loop {
        let mut found = None;
        {
            let buf = reader.fill_buf()?;
            if buf.is_empty() {
                return Ok(None);
            }
            for (idx, byte) in buf.iter().enumerate() {
                if !byte.is_ascii_whitespace() {
                    found = Some((idx, *byte));
                    break;
                }
            }
            if found.is_none() {
                let len = buf.len();
                reader.consume(len);
            }
        }
        if let Some((idx, byte)) = found {
            reader.consume(idx);
            return Ok(Some(byte));
        }
    }
}
