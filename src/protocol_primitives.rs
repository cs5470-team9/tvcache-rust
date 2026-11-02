use std::io;

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

pub async fn read_u8(stream: &mut TcpStream) -> Result<u8, io::Error> {
    let mut buffer = [0; 1];
    stream.read_exact(&mut buffer).await?;
    Ok(buffer[0])
}

pub async fn read_u16(stream: &mut TcpStream) -> Result<u16, io::Error> {
    let mut buffer = [0; 2];
    stream.read_exact(&mut buffer).await?;
    Ok(u16::from_be_bytes(buffer))
}

pub async fn read_u32(stream: &mut TcpStream) -> Result<u32, io::Error> {
    let mut buffer = [0; 4];
    stream.read_exact(&mut buffer).await?;
    Ok(u32::from_be_bytes(buffer))
}

pub async fn read_string_u16(stream: &mut TcpStream) -> Result<String, io::Error> {
    let len = read_u16(stream).await? as usize;
    read_string_bytes(stream, len).await
}

pub async fn read_string_u32(stream: &mut TcpStream) -> Result<String, io::Error> {
    let len = read_u32(stream).await? as usize;
    read_string_bytes(stream, len).await
}

async fn read_string_bytes(stream: &mut TcpStream, len: usize) -> Result<String, io::Error> {
    let mut buffer = vec![0; len];
    stream.read_exact(&mut buffer).await?;

    String::from_utf8(buffer).map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

pub async fn read_path(stream: &mut TcpStream) -> Result<Vec<String>, io::Error> {
    let path_count = read_u16(stream).await? as usize;
    let mut path = Vec::with_capacity(path_count);

    for _ in 0..path_count {
        path.push(read_string_u16(stream).await?);
    }

    Ok(path)
}

// Optional fields are encoded as a 0/1 flag byte followed by the value when 1.
async fn read_flag(stream: &mut TcpStream, field: &str) -> Result<bool, io::Error> {
    match read_u8(stream).await? {
        0 => Ok(false),
        1 => Ok(true),
        flag => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid {field} exists flag: {flag}"),
        )),
    }
}

pub async fn read_optional_u32(stream: &mut TcpStream, field: &str) -> Result<Option<u32>, io::Error> {
    if read_flag(stream, field).await? {
        return Ok(Some(read_u32(stream).await?));
    }

    Ok(None)
}

pub async fn read_optional_string_u16(
    stream: &mut TcpStream,
    field: &str,
) -> Result<Option<String>, io::Error> {
    if read_flag(stream, field).await? {
        return Ok(Some(read_string_u16(stream).await?));
    }

    Ok(None)
}

pub async fn read_optional_string_u32(
    stream: &mut TcpStream,
    field: &str,
) -> Result<Option<String>, io::Error> {
    if read_flag(stream, field).await? {
        return Ok(Some(read_string_u32(stream).await?));
    }

    Ok(None)
}

pub async fn write_u8(stream: &mut TcpStream, value: u8) -> Result<(), io::Error> {
    stream.write_all(&[value]).await
}

pub async fn write_bool(stream: &mut TcpStream, value: bool) -> Result<(), io::Error> {
    write_u8(stream, if value { 1 } else { 0 }).await
}

pub async fn write_u16(stream: &mut TcpStream, value: u16) -> Result<(), io::Error> {
    stream.write_all(&value.to_be_bytes()).await
}

pub async fn write_u32(stream: &mut TcpStream, value: u32) -> Result<(), io::Error> {
    stream.write_all(&value.to_be_bytes()).await
}

pub async fn write_u64(stream: &mut TcpStream, value: u64) -> Result<(), io::Error> {
    stream.write_all(&value.to_be_bytes()).await
}

pub async fn write_string_u16(stream: &mut TcpStream, value: &str) -> Result<(), io::Error> {
    let bytes = value.as_bytes();
    let len = u16::try_from(bytes.len()).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("string is too large for u16 length: {error}"),
        )
    })?;

    write_u16(stream, len).await?;
    stream.write_all(bytes).await
}

pub async fn write_string_u32(stream: &mut TcpStream, value: &str) -> Result<(), io::Error> {
    let bytes = value.as_bytes();
    let len = u32::try_from(bytes.len()).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("string is too large for u32 length: {error}"),
        )
    })?;

    write_u32(stream, len).await?;
    stream.write_all(bytes).await
}

pub async fn write_optional_string_u16(
    stream: &mut TcpStream,
    value: &Option<String>,
) -> Result<(), io::Error> {
    write_bool(stream, value.is_some()).await?;

    match value {
        Some(value) => write_string_u16(stream, value).await,
        None => Ok(()),
    }
}

pub async fn write_optional_string_u32(
    stream: &mut TcpStream,
    value: &Option<String>,
) -> Result<(), io::Error> {
    write_bool(stream, value.is_some()).await?;

    match value {
        Some(value) => write_string_u32(stream, value).await,
        None => Ok(()),
    }
}

pub async fn write_optional_path(
    stream: &mut TcpStream,
    path: &Option<Vec<String>>,
) -> Result<(), io::Error> {
    write_bool(stream, path.is_some()).await?;

    match path {
        Some(path) => write_path(stream, path).await,
        None => Ok(()),
    }
}

pub async fn write_path(stream: &mut TcpStream, path: &[String]) -> Result<(), io::Error> {
    let path_count = u16::try_from(path.len()).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("path has too many entries for u16 length: {error}"),
        )
    })?;

    write_u16(stream, path_count).await?;

    for entry in path {
        write_string_u16(stream, entry).await?;
    }

    Ok(())
}
