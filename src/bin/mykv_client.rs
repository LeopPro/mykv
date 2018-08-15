
#[macro_use]
extern crate jsonrpc_client_core;
extern crate jsonrpc_client_http;

use jsonrpc_client_http::HttpTransport;

jsonrpc_client!(pub struct FizzBuzzClient {
    /// Returns the fizz-buzz string for the given number.
    pub fn say_hello(&mut self) -> RpcRequest<String>;
});

fn main() {
    let transport = HttpTransport::new().standalone().unwrap();
    let transport_handle = transport.handle("http://127.0.0.1:3030").unwrap();
    let mut client = FizzBuzzClient::new(transport_handle);
    let result1 = client.say_hello().call().unwrap();
    let result2 = client.say_hello().call().unwrap();
    let result3 = client.say_hello().call().unwrap();

    // Should print "fizz 4 buzz" if the server implemented the service correctly
    println!("{} {} {}", result1, result2, result3);
}