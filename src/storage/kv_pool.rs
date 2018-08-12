use std::collections::HashMap;
use bytes::Bytes;
use bincode::serialize;
use std::fs::File;
use std::fs::OpenOptions;
use bincode::deserialize;
use std::path::Path;
use std::error::Error;
use std::io::Write;
use std::io::Read;

#[derive(Serialize, Deserialize, PartialEq, Clone, Debug)]
pub struct KvPool {
    last_log_index: u64,
    hashmap: HashMap<Vec<u8>, Vec<u8>>,
    dump_file_path: String,
}

impl KvPool {
    pub fn init(dump_file_path: &str) -> Result<Self, Box<Error>> {
        if Path::new(dump_file_path).exists() {
            KvPool::revert_from_file(dump_file_path)
        } else {
            Ok(KvPool {
                last_log_index: 0,
                hashmap: HashMap::new(),
                dump_file_path: String::from(dump_file_path),
            })
        }
    }

    pub fn set(&mut self, index: u64, key: Vec<u8>, value: Vec<u8>) {
        self.last_log_index = index;
        self.hashmap.insert(key, value);
    }

    pub fn get(&self, key: &Vec<u8>) -> Option<&Vec<u8>> {
        self.hashmap.get(key).clone()
    }

    pub fn remove(&mut self, index: u64, key: &Vec<u8>) {
        self.last_log_index = index;
        self.hashmap.remove(key);
    }

    pub fn dump_to_file(&self) -> Result<(), Box<Error>> {
        let mut dump_file = OpenOptions::new().read(false).write(true)
            .create(true).append(false).open(&self.dump_file_path)?;
        let mut dump_bytes = serialize(self)?;
        dump_file.write_all(&dump_bytes)?;
        dump_file.flush()?;
        dump_file.sync_all()?;
        Ok(())
    }

    pub fn revert_from_file(dump_file_path: &str) -> Result<KvPool, Box<Error>> {
        let mut dump_file = OpenOptions::new().read(true).write(false)
            .create(false).append(false).open(dump_file_path)?;
        let mut dump_bytes = Vec::with_capacity(dump_file.metadata()?.len() as usize);
        dump_file.read_to_end(&mut dump_bytes)?;
        let kv_pool: KvPool = deserialize(&dump_bytes)?;
        Ok(kv_pool)
    }

    pub fn last_log_index(&self) -> u64 {
        self.last_log_index
    }
}


#[cfg(test)]
mod tests {
    use storage::kv_pool::KvPool;
    use bytes::Bytes;

    #[test]
    fn test() {
        let mut v = vec![1 as u8];
        for i in 1..10000 {
            v.push((i % 256) as u8)
        }
        let mut kv_pool = KvPool::init("./db.dump").unwrap();
        for i in 1..100 {
            kv_pool.set(i, vec![1, 2, i as u8], v.clone());
        }
        kv_pool.dump_to_file().unwrap();

        let mut kv_pool2 = KvPool::init("./db.dump").unwrap();
        assert_eq!(kv_pool, kv_pool2);
    }
}