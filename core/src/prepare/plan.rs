use crate::ffi::DbError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, uniffi::Record)]
pub struct BitrateStep {
    pub from_long_side: u32,
    pub min_bps: u32,
    pub max_bps: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct VideoLimits {
    pub max_long_side: u32,
    pub corridors: Vec<BitrateStep>,
    pub min_frame_rate: u32,
    pub max_frame_rate: u32,
    pub source_bitrate_cap_percent: u32,
    pub audio_bitrate: u32,
    pub audio_channels: u32,
    pub audio_sample_rate: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, uniffi::Record)]
pub struct VideoSource {
    pub width: u32,
    pub height: u32,
    pub rotation_degrees: i32,
    pub frame_rate: f32,
    pub bitrate: Option<u32>,
    pub mp4_h264_aac: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, uniffi::Record)]
pub struct VideoPlan {
    pub reencode: bool,
    pub width: u32,
    pub height: u32,
    pub bitrate: u32,
    pub frame_rate: u32,
    pub rotation_degrees: u32,
}

pub fn default_video_limits() -> VideoLimits {
    let step = |from_long_side, min_bps, max_bps| BitrateStep { from_long_side, min_bps, max_bps };
    VideoLimits {
        max_long_side: 1920,
        corridors: vec![
            step(0, 500_000, 900_000),
            step(720, 700_000, 1_200_000),
            step(960, 900_000, 1_500_000),
            step(1280, 1_200_000, 2_000_000),
            step(1920, 2_000_000, 3_500_000),
        ],
        min_frame_rate: 30,
        max_frame_rate: 60,
        source_bitrate_cap_percent: 95,
        audio_bitrate: 128_000,
        audio_channels: 2,
        audio_sample_rate: 44_100,
    }
}

pub fn plan_video(source: VideoSource, limits: VideoLimits) -> Result<VideoPlan, DbError> {
    check(&limits)?;
    if source.width == 0 || source.height == 0 {
        return Err(DbError::Invalid { detail: "the source video has an empty side".to_string() });
    }
    let rotation = source.rotation_degrees.rem_euclid(360) as u32;
    let (width, height) = if rotation % 180 == 90 { (source.height, source.width) } else { (source.width, source.height) };
    let (even_width, even_height) = (even(f64::from(width)), even(f64::from(height)));
    let (target_width, target_height) = scaled(even_width, even_height, limits.max_long_side);
    let rate = frame_rate(source.frame_rate, &limits);
    let band = corridor(&limits, target_width.max(target_height))?;
    let bitrate = match source.bitrate.filter(|bitrate| *bitrate > 0) {
        None => band.min_bps,
        Some(bitrate) => {
            let bitrate = f64::from(bitrate);
            let pixels = f64::from(target_width) * f64::from(target_height) / (f64::from(even_width) * f64::from(even_height));
            let frames = f64::from(rate) / f64::from((source.frame_rate.round().max(0.0) as u32).max(1));
            let wanted = (bitrate * pixels * frames.max(0.85)).round() as u32;
            let cap = ((bitrate * f64::from(limits.source_bitrate_cap_percent) / 100.0).round() as u32).max(band.min_bps);
            wanted.max(band.min_bps).min(band.max_bps.min(cap))
        }
    };
    let source_band = corridor(&limits, width.max(height))?;
    let as_is = source.mp4_h264_aac
        && rotation == 0
        && width.max(height) <= limits.max_long_side
        && source.frame_rate > 0.0
        && rate_of(source.frame_rate) <= limits.max_frame_rate
        && source.bitrate.is_some_and(|bitrate| bitrate <= source_band.max_bps);
    Ok(VideoPlan { reencode: !as_is, width: target_width, height: target_height, bitrate, frame_rate: rate, rotation_degrees: rotation })
}

pub fn check(limits: &VideoLimits) -> Result<(), DbError> {
    let reason = if limits.max_long_side < 2 {
        "max_long_side must be at least 2"
    } else if limits.corridors.is_empty() {
        "corridors must not be empty"
    } else if limits.corridors.iter().any(|step| step.min_bps > step.max_bps) {
        "a corridor has min_bps above max_bps"
    } else if limits.min_frame_rate == 0 || limits.min_frame_rate > limits.max_frame_rate {
        "frame rates must satisfy 0 < min <= max"
    } else {
        return Ok(());
    };
    Err(DbError::Invalid { detail: reason.to_string() })
}

fn even(value: f64) -> u32 {
    let rounded = (value.round() as u32).max(2);
    rounded - rounded % 2
}

fn scaled(width: u32, height: u32, max_long_side: u32) -> (u32, u32) {
    let long_side = width.max(height);
    if long_side <= max_long_side {
        return (width, height);
    }
    let scale = f64::from(max_long_side) / f64::from(long_side);
    (even(f64::from(width) * scale), even(f64::from(height) * scale))
}

fn rate_of(frame_rate: f32) -> u32 {
    frame_rate.round().max(0.0) as u32
}

fn frame_rate(frame_rate: f32, limits: &VideoLimits) -> u32 {
    match rate_of(frame_rate) {
        0 => limits.min_frame_rate,
        rounded => rounded.min(limits.max_frame_rate),
    }
}

fn corridor(limits: &VideoLimits, long_side: u32) -> Result<BitrateStep, DbError> {
    let steps = &limits.corridors;
    steps
        .iter()
        .filter(|step| step.from_long_side <= long_side)
        .max_by_key(|step| step.from_long_side)
        .or_else(|| steps.iter().min_by_key(|step| step.from_long_side))
        .copied()
        .ok_or_else(|| DbError::Invalid { detail: "corridors must not be empty".to_string() })
}
