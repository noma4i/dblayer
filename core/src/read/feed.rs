use crate::store::InvalidationRequest;

pub const FEED_MODEL: &str = "moments";
pub const FEED_RELATION: &str = "feed";
pub const MOVE_THRESHOLD_METERS: f64 = 500.0;

const EARTH_RADIUS_METERS: f64 = 6_371_000.0;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Coordinate {
    pub latitude: f64,
    pub longitude: f64,
}

impl Coordinate {
    pub fn distance_meters(self, other: Coordinate) -> f64 {
        let first_latitude = self.latitude.to_radians();
        let second_latitude = other.latitude.to_radians();
        let latitude_delta = (other.latitude - self.latitude).to_radians();
        let longitude_delta = (other.longitude - self.longitude).to_radians();
        let haversine = (latitude_delta / 2.0).sin().powi(2)
            + first_latitude.cos() * second_latitude.cos() * (longitude_delta / 2.0).sin().powi(2);
        EARTH_RADIUS_METERS * 2.0 * haversine.sqrt().atan2((1.0 - haversine).sqrt())
    }
}

pub fn moves_feed(baseline: Option<Coordinate>, next: Coordinate) -> bool {
    baseline.is_none_or(|baseline| baseline.distance_meters(next) >= MOVE_THRESHOLD_METERS)
}

pub fn feed_next_baseline(baseline: Option<Coordinate>, next: Coordinate) -> Coordinate {
    match baseline {
        Some(baseline) if !moves_feed(Some(baseline), next) => baseline,
        _ => next,
    }
}

pub fn feed_invalidation(baseline: Option<Coordinate>, next: Coordinate) -> Vec<InvalidationRequest> {
    if moves_feed(baseline, next) {
        vec![InvalidationRequest::family(FEED_MODEL, FEED_RELATION)]
    } else {
        Vec::new()
    }
}
