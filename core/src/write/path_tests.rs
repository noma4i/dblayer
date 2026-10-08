use super::*;

#[test]
fn path_exposes_value_and_non_empty_segments() {
    let path = Path::new("media.transcodeStatus");
    assert_eq!(path.as_str(), "media.transcodeStatus");
    assert_eq!(path.segments(), ["media", "transcodeStatus"]);
    assert_eq!(path.to_string(), "media.transcodeStatus");
}
