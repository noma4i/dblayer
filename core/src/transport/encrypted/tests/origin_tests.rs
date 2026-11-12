use std::sync::Arc;
use std::time::Duration;

use super::fake_server_tests::FakeServer;
use super::vectors_tests::*;
use super::*;
use crate::runtime::ManualClock;

fn accepted(origin: &str) -> bool {
    let config = EncryptedConfig { origin: origin.to_string(), server_key: public_key_der_b64().to_string(), pin: pin().to_string(), ..crate::transport::encrypted::tests::protocol_tests::config() };
    let clock = Arc::new(ManualClock::new(Duration::from_secs(1)));
    Exchange::new(&config, FakeServer::new(clock.clone()), clock).is_ok()
}

#[test]
fn https_is_accepted_for_any_host_and_http_only_for_development_hosts() {
    let allowed = [
        "https://api.example.com",
        "https://api.example.com:8443/",
        "https://203.0.113.7",
        "http://localhost:3001",
        "http://localhost",
        "http://127.0.0.1:3001",
        "http://127.5.5.5",
        "http://[::1]:3001",
        "http://192.168.4.54:3001",
        "http://10.0.0.5",
        "http://172.16.0.1",
        "http://172.31.255.255",
        "http://169.254.1.1",
        "http://[fe80::1]",
        "http://[fd00::1]:3001",
        "http://mac.local:3001",
        "http://a.b.local",
    ];
    let refused = [
        "http://api.example.com",
        "http://203.0.113.7",
        "http://172.32.0.1",
        "http://172.15.255.255",
        "http://localhost.evil.com",
        "http://127.0.0.1.evil.com",
        "http://localhost@evil.com",
        "http://192.168.1.1@evil.com",
        "http://evil.com#@localhost",
        "http://evil.com#@192.168.1.1",
        "http://0.0.0.0",
        "http://255.255.255.255",
        "http://[::ffff:8.8.8.8]",
        "http://[2001:db8::1]",
        "http://2130706433",
        "http://0x7f.1",
        "http://0177.0.0.1",
        "http://192.168.001.001",
        "http://127.0.0.1.",
        "http://localhost.",
        "http://api.local.evil.com",
        "http://local",
        "http://.local",
        "http://api.local.",
        "http://localhost:",
        "http://localhost:99999",
        "http://localhost:abc",
        "http://localhost/api",
        "https://api.example.com/api",
        "http://localhost?x=1",
        "http://",
        "https://",
        "ftp://localhost",
        "localhost:3001",
        "HTTP://localhost",
    ];
    for origin in allowed {
        assert!(accepted(origin), "{origin}");
    }
    for origin in refused {
        assert!(!accepted(origin), "{origin}");
    }
}
