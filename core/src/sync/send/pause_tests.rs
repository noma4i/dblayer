use super::*;

#[test]
fn independent_databases_spread_send_retries_and_keep_assigned_delays() {
    let clients: Vec<_> = (0..3).map(|_| crate::sync::rig::rig()).collect();
    let delays: Vec<_> = clients.iter().map(|client| Inner::send_backoff(&client.sync.inner, 1).delay(0)).collect();
    assert!(delays.windows(2).any(|pair| pair[0] != pair[1]), "clients repeated together: {delays:?}");
    for (client, delay) in clients.iter().zip(delays) {
        assert_eq!(client.sync.inner.send_backoff(1).delay(0), delay);
    }
}
