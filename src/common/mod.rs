use bytes::Bytes;
use bincode::serialize;
use std::error::Error;
use byteorder::LittleEndian;
use byteorder::WriteBytesExt;
use byteorder::ReadBytesExt;
use std::io::Cursor;
use bincode::deserialize;
use std::io::Read;

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum DirectiveAction {
    SET,
    GET,
    REMOVE,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct Directive {
    index: u64,
    action: DirectiveAction,
    key: Vec<u8>,
    value: Vec<u8>,
}

impl Directive {
    pub fn from(index: u64, action: DirectiveAction, key: Vec<u8>, value: Vec<u8>) -> Self {
        Directive {
            index,
            action,
            key,
            value,
        }
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, Box<Error>> {
        let mut directive_bytes = serialize(self)?;
        println!("aa{}", directive_bytes.len());
        let mut log_bytes = Vec::with_capacity(directive_bytes.len() + 8);
        log_bytes.write_u64::<LittleEndian>(directive_bytes.len() as u64)?;
        log_bytes.append(&mut directive_bytes);
        Ok(log_bytes)
    }

    pub fn from_bytes(mut bytes: Vec<u8>) -> Result<Directive, Box<Error>> {
        let directive_bytes = bytes.split_off(8);
        let directive_bytes_len = Cursor::new(bytes).read_u64::<LittleEndian>()?;
        if directive_bytes.len() as u64 != directive_bytes_len {
            panic!("directive_bytes_len error")
        }
        Directive::from_bytes_without_header(&directive_bytes)
    }

    pub fn from_bytes_without_header(bytes: &[u8]) -> Result<Directive, Box<Error>> {
        Ok(deserialize(bytes)?)
    }

    pub fn is_action(&self, action: DirectiveAction) -> bool {
        self.action == action
    }

    pub fn action(&self) -> &DirectiveAction {
        &self.action
    }

    pub fn index(&self) -> u64 {
        self.index
    }

    pub fn key(&self) -> &Vec<u8> {
        &self.key
    }

    pub fn value(&self) -> &Vec<u8> {
        &self.value
    }
}

#[cfg(test)]
mod tests {
    use common::Directive;
    use common::DirectiveAction;
    use bytes::Bytes;

    #[test]
    fn directive_test() {
        let mut v = vec![1 as u8];
        for i in 1..10000 {
            v.push((i % 256) as u8)
        }
        let directive = Directive::from(123, DirectiveAction::SET,
                                        vec![1 as u8, 2, 3], v);
        let directive_from_bytes = Directive::from_bytes(directive.to_bytes().unwrap()).unwrap();
        println!("d1{:?}", directive);
        println!("d2{:?}", directive_from_bytes);

        assert_eq!(directive, directive_from_bytes)
    }
}

