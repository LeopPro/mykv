use std::collections::HashMap;
use bytes::Bytes;

#[derive(Clone)]
struct KvPool {
    lastest_log_index: u64,
    hashmap: HashMap<Bytes, Bytes>,
}

impl KvPool {
    fn new() -> Self {
        KvPool {
            lastest_log_index: 0,
            hashmap: HashMap::new(),
        }
    }

    fn set(&mut self, index: u64, key: Bytes, value: Bytes) {
        self.lastest_log_index = index;
        self.hashmap.insert(key, value);
    }

    fn get(&self, key: &Bytes) -> Option<&Bytes> {
        self.hashmap.get(key).clone()
    }

    fn remove(&mut self, index: u64, key: &Bytes) {
        self.lastest_log_index = index;
        self.hashmap.remove(key);
    }
}


#[cfg(test)]
mod tests {
    use storage::kv_pool::KvPool;
    use bytes::Bytes;

    #[test]
    fn test() {
        let mut kv_pool = KvPool::new();
        kv_pool.set(1,Bytes::from(&b"Hello world"[..]),Bytes::from(&b"Hello world"[..]));
        println!("{:?}",kv_pool.get(&Bytes::from(&b"Hello world"[..])));
        kv_pool.remove(2,&Bytes::from(&b"Hello world"[..]));
        println!("{:?}",kv_pool.get(&Bytes::from(&b"Hello world"[..])));
    }
}