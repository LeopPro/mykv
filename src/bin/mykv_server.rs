extern crate mykv;
extern crate jsonrpc_core;
extern crate jsonrpc_http_server;

use jsonrpc_core::*;
use jsonrpc_http_server::*;
use jsonrpc_core::IoHandler;
use jsonrpc_http_server::ServerBuilder;

fn main() {
    let mut io = IoHandler::new();
    io.add_method("say_hello", |_: Params| {
        Ok(Value::String("hello".to_string()))
    });

    let _server = ServerBuilder::new(io)
        .start_http(&"127.0.0.1:3030".parse().unwrap())
        .expect("Unable to start RPC server");

    _server.wait();
}