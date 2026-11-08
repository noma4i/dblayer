//! boundary_observer_stop_orders (D-177, P-BND-09): a delivery counts only for a live token, and revoking cannot pass a delivery in flight.

use super::*;

fn delivered(registrations: &Registrations, token: u64) -> usize {
    let mut sent = 0;
    registrations.deliver(token, || sent += 1);
    sent
}

#[test]
fn boundary_observer_stop_orders() {
    let registrations = Registrations::default();
    let first = registrations.issue();
    let second = registrations.issue();
    assert_ne!(first, second);
    assert_eq!((delivered(&registrations, first), delivered(&registrations, second), delivered(&registrations, second + 1)), (1, 1, 0));
    registrations.revoke(first);
    assert_eq!((delivered(&registrations, first), delivered(&registrations, second)), (0, 1));
    registrations.revoke_all();
    assert_eq!(delivered(&registrations, second), 0);
    let third = registrations.issue();
    assert_eq!((delivered(&registrations, second), delivered(&registrations, third)), (0, 1), "a token of an ended registration is never issued again");
}

#[test]
fn boundary_observer_stop_orders_a_revoke_cannot_pass_a_delivery_in_flight() {
    let registrations = Registrations::default();
    let token = registrations.issue();
    let mut blocked = false;
    registrations.deliver(token, || blocked = registrations.live.try_write().is_err());
    assert!(blocked, "the stop of the observers waits for the deliveries in flight");
}
