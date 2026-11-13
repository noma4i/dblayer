use super::photo::{photo_turn, PhotoTurn};

const MAX_PIXELS: u32 = 16_384;

#[derive(Debug, Clone, Copy, PartialEq, Eq, uniffi::Record)]
pub struct PixelSize {
    pub width: u32,
    pub height: u32,
}

fn decode_cost(width: u32, height: u32) -> Option<u64> {
    if width == 0 || height == 0 || width > MAX_PIXELS || height > MAX_PIXELS {
        return None;
    }
    Some(u64::from(width) * u64::from(height) * 4)
}

#[derive(Debug, Clone, Copy, PartialEq, uniffi::Record)]
pub struct CropFractions {
    pub x: f64,
    pub y: f64,
    pub width: f64,
    pub height: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, uniffi::Record)]
pub struct PhotoEdit {
    pub max_size: Option<PixelSize>,
    pub enlarge: bool,
    pub crop: Option<CropFractions>,
    pub output_size: Option<PixelSize>,
    pub mirror: bool,
    pub rotation_degrees: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, uniffi::Record)]
pub struct PixelRect {
    pub x: u32,
    pub y: u32,
    pub width: u32,
    pub height: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, uniffi::Record)]
pub struct PhotoPlan {
    pub turn: PhotoTurn,
    pub upright: PixelSize,
    pub crop: PixelRect,
    pub mirror: bool,
    pub output: PixelSize,
    pub needs_draw: bool,
    pub sample: u32,
    pub decode: PixelSize,
    pub thumbnail_side: u32,
    pub cost: u64,
}

pub fn photo_plan(width: u32, height: u32, exif_orientation: u32, edit: PhotoEdit) -> Option<PhotoPlan> {
    decode_cost(width, height)?;
    if !edit.rotation_degrees.is_multiple_of(90) {
        return None;
    }
    let turn = turned_by(photo_turn(exif_orientation), edit.rotation_degrees);
    let turned = turn.rotation_degrees != 0 || turn.flip_horizontal;
    let (upright_width, upright_height) = if turn.rotation_degrees % 180 == 90 { (height, width) } else { (width, height) };
    let crop = match edit.crop {
        None => PixelRect { x: 0, y: 0, width: upright_width, height: upright_height },
        Some(fractions) => {
            let (x, width) = span(fractions.x, fractions.width, upright_width)?;
            let (y, height) = span(fractions.y, fractions.height, upright_height)?;
            PixelRect { x, y, width, height }
        }
    };
    let output = match (edit.output_size, edit.max_size) {
        (Some(exact), _) => exact,
        (None, Some(limit)) => fitted(crop.width, crop.height, limit, edit.enlarge)?,
        (None, None) => PixelSize { width: crop.width, height: crop.height },
    };
    if output.width == 0 || output.height == 0 {
        return None;
    }
    let whole = crop.width == upright_width && crop.height == upright_height;
    let needs_draw = !whole || edit.mirror || output.width != crop.width || output.height != crop.height;
    let (sample, thumbnail_side) = sampling(width, height, crop, output);
    let decode = PixelSize { width: width.div_ceil(sample), height: height.div_ceil(sample) };
    let decoded = decode_cost(decode.width, decode.height)?;
    let kept = if turned { decoded * 2 } else { decoded };
    let cost = if needs_draw { kept + decode_cost(output.width, output.height)? } else { kept };
    Some(PhotoPlan { turn, upright: PixelSize { width: upright_width, height: upright_height }, crop, mirror: edit.mirror, output, needs_draw, sample, decode, thumbnail_side, cost })
}

fn sampling(width: u32, height: u32, crop: PixelRect, output: PixelSize) -> (u32, u32) {
    let mut sample = 1;
    while crop.width / (sample * 2) >= output.width && crop.height / (sample * 2) >= output.height {
        sample *= 2;
    }
    let (numerator, denominator) = if u64::from(output.width) * u64::from(crop.height) >= u64::from(output.height) * u64::from(crop.width) {
        (output.width, crop.width)
    } else {
        (output.height, crop.height)
    };
    let long = u64::from(width.max(height));
    let side = (long * u64::from(numerator)).div_ceil(u64::from(denominator)).clamp(1, long);
    (sample, side as u32)
}

fn turned_by(turn: PhotoTurn, degrees: u32) -> PhotoTurn {
    let extra = degrees % 360;
    let added = if turn.flip_horizontal { (360 - extra) % 360 } else { extra };
    PhotoTurn { rotation_degrees: (turn.rotation_degrees + added) % 360, flip_horizontal: turn.flip_horizontal }
}

fn span(start: f64, length: f64, total: u32) -> Option<(u32, u32)> {
    if !start.is_finite() || !length.is_finite() {
        return None;
    }
    let edge = |fraction: f64| (fraction.clamp(0.0, 1.0) * f64::from(total)).round() as u32;
    let (first, last) = (edge(start), edge(start + length));
    (last > first).then_some((first, last - first))
}

fn fitted(width: u32, height: u32, limit: PixelSize, enlarge: bool) -> Option<PixelSize> {
    if limit.width == 0 || limit.height == 0 {
        return None;
    }
    let fit = (f64::from(limit.width) / f64::from(width)).min(f64::from(limit.height) / f64::from(height));
    let scale = if enlarge { fit } else { fit.min(1.0) };
    let side = |value: u32, most: u32| ((f64::from(value) * scale).round() as u32).clamp(1, most);
    let (widest, tallest) = if enlarge { (limit.width, limit.height) } else { (limit.width.min(width), limit.height.min(height)) };
    Some(PixelSize { width: side(width, widest), height: side(height, tallest) })
}
