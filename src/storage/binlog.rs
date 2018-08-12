use std::fs::OpenOptions;
use std::fs::File;
use std::io::Write;
use byteorder::{LittleEndian, WriteBytesExt};
use std::error::Error;
use common::Directive;
use bincode::{serialize, deserialize};
use std::io::Cursor;
use std::io::SeekFrom;
use std::io::Seek;
use std::io::Read;
use byteorder::ReadBytesExt;
use std::io::ErrorKind;

pub struct BinLogger {
    log_file: File,
}

impl BinLogger {
    pub fn init(log_path: &str) -> Self {
        let log_file = OpenOptions::new().read(true).write(true)
            .create(true).append(true).open(log_path).unwrap();
        BinLogger { log_file }
    }

    pub fn log(&mut self, directive: &Directive) -> Result<(), Box<Error>> {
        self.log_file.write_all(&directive.to_bytes()?)?;
        self.log_file.flush()?;
        self.log_file.sync_all()?;
        Ok(())
    }

    pub fn iter_after_index<F>(&mut self, index: u64, mut handler: F) where F: FnMut(Directive) {
        let mut directice_length_bytes = [0; 8];
        self.log_file.seek(SeekFrom::Start(0));
        while true {
            match self.log_file.read_exact(&mut directice_length_bytes) {
                Ok(_) => (),
                Err(error) => {
                    if error.kind() == ErrorKind::UnexpectedEof {
                        break;
                    } else {
                        panic!("Error")
                    }
                }
            }
            let directive_bytes_len = Cursor::new(directice_length_bytes).read_u64::<LittleEndian>().unwrap() as usize;
            let mut directive_bytes = Vec::with_capacity(directive_bytes_len);
            unsafe { directive_bytes.set_len(directive_bytes_len); }
            self.log_file.read_exact(&mut directive_bytes).unwrap();
            let directive = Directive::from_bytes_without_header(&directive_bytes).unwrap();
            if directive.index() > index {
                handler(directive);
            }
        }
        self.log_file.seek(SeekFrom::End(0));
    }

    pub fn switch(&mut self) {}
}

#[cfg(test)]
mod tests {
    use storage::binlog::BinLogger;
    use common::Directive;
    use common::DirectiveAction;

    #[test]
    fn test() {
        let mut v = vec![1 as u8];
        for i in 1..10000 {
            v.push((i % 256) as u8)
        }
        let directive = Directive::from(123, DirectiveAction::SET,
                                        vec![1 as u8, 2, 3], v);

        let mut bin_logger = BinLogger::init("./test");
        bin_logger.log(&directive);
    }

    #[test]
    fn test_read_log() {
        let mut bin_logger = BinLogger::init("./storage.binlog");
        bin_logger.iter_after_index(5, |directive| {
            println!("{:?}", directive)
        })
    }
}