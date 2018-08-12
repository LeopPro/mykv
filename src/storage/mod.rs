mod binlog;
mod kv_pool;

use storage::binlog::BinLogger;
use storage::kv_pool::KvPool;
use common::Directive;
use common::DirectiveAction;
use std::thread;
use std::sync::mpsc;
use std::sync::mpsc::Sender;
use std::sync::mpsc::Receiver;
use std::time::SystemTime;
use std::error::Error;

pub struct Storage {
    kv_pool: KvPool,
    bin_logger: BinLogger,
    directive_sender: Sender<KvPool>,
    last_dump_time: SystemTime,
    variety_during_dump: u64,

}

const DUMP_VARIETY_PER_MINUTE_THRESHOLD: u64 = 16;
const ONCE_DUMP_VARIETY_THRESHOLD: u64 = 32;
const DUMP_FILE_PATH: &str = "./storage.dump";
const BINLOG_FILE_PATH: &str = "./storage.binlog";

impl Storage {
    pub fn init() -> Self {
        let (sender, receiver) = mpsc::channel();
        Storage::start_dump_thread(receiver);
        let mut bin_logger = BinLogger::init(BINLOG_FILE_PATH);
        let mut kv_pool = KvPool::init(DUMP_FILE_PATH).unwrap();
        bin_logger.iter_after_index(kv_pool.last_log_index(), |directive| {
            Storage::execute(&mut kv_pool, &directive)
        });
        Storage {
            kv_pool,
            bin_logger,
            directive_sender: sender,
            last_dump_time: SystemTime::now(),
            variety_during_dump: 0,
        }
    }

    fn start_dump_thread(receiver: Receiver<KvPool>) {
        thread::spawn(move || {
            for kv_pool in receiver {
                kv_pool.dump_to_file();
            }
        });
    }

    fn monitor_and_dump_kv_pool(&mut self) {
        self.variety_during_dump += 1;
        if (SystemTime::now().duration_since(self.last_dump_time).unwrap().as_secs() >= 60 &&
            self.variety_during_dump >= DUMP_VARIETY_PER_MINUTE_THRESHOLD) ||
            self.variety_during_dump >= ONCE_DUMP_VARIETY_THRESHOLD {
            println!("dump database,last_dump_time:{:?},variety_during_dump:{}", self.last_dump_time,
                     self.variety_during_dump);
            info!("dump database,last_dump_time:{:?},variety_during_dump:{}", self.last_dump_time,
                  self.variety_during_dump);
            self.variety_during_dump = 0;
            self.last_dump_time = SystemTime::now();
            // TODO 切换日志
            self.directive_sender.send(self.kv_pool.clone()).unwrap()
        }
    }

    pub fn set(&mut self, directive: Directive) -> Result<(), Box<Error>> {
        if !directive.is_action(DirectiveAction::SET) {
            panic!("DirectiveAction is not SET");
        }
        self.bin_logger.log(&directive)?;
        self.kv_pool.set(directive.index(), directive.key().clone(), directive.value().clone());
        self.monitor_and_dump_kv_pool();
        println!("set, index:{}", directive.index());
        debug!("set, index:{}", directive.index());
        Ok(())
    }

    pub fn get(&self) {}

    pub fn remove(&self) {}

    pub fn execute(kv_pool: &mut KvPool, directive: &Directive) {
        match directive.action() {
            DirectiveAction::SET => {
                kv_pool.set(directive.index(), directive.key().clone(), directive.value().clone());
            }
            DirectiveAction::REMOVE => {}
            DirectiveAction::GET => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use storage::Storage;
    use common::Directive;
    use common::DirectiveAction;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn directive_test() {
        let mut v = vec![1 as u8];
        for i in 1..25 {
            v.push((i % 256) as u8)
        }
        let mut storage = Storage::init();
        for i in 1..70 {
            storage.set(Directive::from(i, DirectiveAction::SET,
                                        vec![1, 2, i as u8], v.clone())).unwrap();
            thread::sleep(Duration::from_millis(100));
        }
    }
}
