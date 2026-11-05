use serde_json::json;

use super::*;

#[test]
fn threshold_splits_single_from_multipart() {
    assert_eq!(scenario_for(9, 10), UploadScenario::Single);
    assert_eq!(scenario_for(10, 10), UploadScenario::Multipart);
}

#[test]
fn parts_cover_the_file_exactly_with_a_short_tail() {
    assert_eq!(part_count(12, 5), 3);
    assert_eq!(part_count(10, 5), 2);
    assert_eq!(part_count(0, 5), 1);
    let ranges: Vec<(u64, u64)> = (1..=3).map(|number| part_range(12, 5, number)).collect();
    assert_eq!(ranges, [(0, 5), (5, 5), (10, 2)]);
    assert_eq!(ranges.iter().map(|(_, length)| length).sum::<u64>(), 12);
}

#[test]
fn mime_follows_the_extension_and_falls_back() {
    assert_eq!(mime_of("clip.MP4"), "video/mp4");
    assert_eq!(mime_of("a.jpeg"), "image/jpeg");
    assert_eq!(mime_of("noext"), "application/octet-stream");
}

#[test]
fn headers_parse_from_a_map_or_a_list() {
    let map = json!({"Content-Type": "video/mp4"});
    assert_eq!(parse_headers(Some(&map)).unwrap(), [("Content-Type".to_string(), "video/mp4".to_string())]);
    let list = json!([{"name": "x-a", "value": "1"}, {"key": "x-b", "value": "2"}]);
    assert_eq!(parse_headers(Some(&list)).unwrap(), [("x-a".to_string(), "1".to_string()), ("x-b".to_string(), "2".to_string())]);
    assert!(parse_headers(None).unwrap().is_empty());
}

#[test]
fn inv_file_08_no_single_read_of_a_file_is_larger_than_the_single_threshold_or_the_part_size() {
    let config = UploadConfig::default();
    for total in [0, 1, config.multipart_threshold - 1, config.multipart_threshold, 100 * MEBIBYTE, 100 * MEBIBYTE + 1] {
        match scenario_for(total, config.multipart_threshold) {
            UploadScenario::Single => assert!(total < config.multipart_threshold, "a single read holds at most the threshold: {total}"),
            UploadScenario::Multipart => {
                let ranges: Vec<(u64, u64)> = (1..=part_count(total, config.part_size)).map(|number| part_range(total, config.part_size, number)).collect();
                assert!(ranges.iter().all(|(_, length)| *length <= config.part_size), "{total}: a part is larger than the part size");
                assert_eq!(ranges.iter().map(|(_, length)| length).sum::<u64>(), total);
                assert!(ranges.windows(2).all(|pair| pair[0].0 + pair[0].1 == pair[1].0), "{total}: parts overlap or leave a gap");
            }
        }
    }
}
