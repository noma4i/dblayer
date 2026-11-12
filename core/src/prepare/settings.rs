use super::{check, CropFractions, PhotoEdit, PixelSize};
use crate::ffi::upload_wire::WirePrepare;
use crate::ffi::DbError;

fn invalid(detail: &str) -> DbError { DbError::Invalid { detail: detail.to_string() } }

fn side_set(size: Option<PixelSize>) -> bool { size.is_none_or(|size| size.width > 0 && size.height > 0) }

fn fraction(value: f64) -> bool { (0.0..=1.0).contains(&value) }

fn crop_set(crop: Option<CropFractions>) -> bool {
    crop.is_none_or(|crop| fraction(crop.x) && fraction(crop.y) && fraction(crop.width) && fraction(crop.height) && crop.width > 0.0 && crop.height > 0.0)
}

fn edit_set(edit: &PhotoEdit) -> Result<(), DbError> {
    if !side_set(edit.max_size) || !side_set(edit.output_size) { return Err(invalid("a size of the image edit has a side of zero")); }
    if !crop_set(edit.crop) { return Err(invalid("the crop of the image edit is outside 0..1")); }
    if !matches!(edit.rotation_degrees, 0 | 90 | 180 | 270) { return Err(invalid("the rotation of the image edit is not 0, 90, 180 or 270")); }
    Ok(())
}

/// The settings of a prepare step the core can plan from: a refusal at the acceptance of the transfer.
pub fn check_settings(prepare: &WirePrepare) -> Result<(), DbError> {
    match prepare {
        WirePrepare::Video { limits, .. } => limits.as_ref().map_or(Ok(()), check),
        WirePrepare::Image { edit, quality, .. } => {
            if quality.is_some_and(|quality| !fraction(quality)) { return Err(invalid("the image quality is outside 0..1")); }
            edit_set(edit)
        }
    }
}
