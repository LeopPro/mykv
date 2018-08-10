use std::fs::OpenOptions;
use std::fs::File;
use std::io::Write;
use byteorder::{LittleEndian, WriteBytesExt};
use std::io::Error;

struct BinLogger {
    log_file: File,
}

impl BinLogger {
    pub fn init(log_path: &str) -> Self {
        let log_file = OpenOptions::new().read(true).write(true)
            .create(true).append(true).open(log_path).unwrap();
        BinLogger { log_file }
    }

    pub fn log(&mut self, index: u64, data: &[u8]) -> Result<(), Error> {
        let mut log_bytes: Vec<u8> = vec![];
        log_bytes.write_u64::<LittleEndian>(index).unwrap();
        log_bytes.write_u64::<LittleEndian>(data.len() as u64)?;
        log_bytes.write_all(data);
        self.log_file.write_all(log_bytes.as_ref())?;
        self.log_file.flush()?;
        self.log_file.sync_all()?;
        Ok(())
    }

    pub fn close(&self) {
        match self.log_file.try_clone() {
            Ok(_) => println!("OK"),
            Err(error) => println!("{:?}", error)
        }
    }
}

#[cfg(test)]
mod tests {
    use storage::binlog::BinLogger;

    #[test]
    fn test() {
        let mut bin_logger = BinLogger::init("./test");
        bin_logger.log(123456, b"123321");
        bin_logger.close();
    }
}