use super::{photo_plan, plan_video, PhotoEdit, PixelSize};
use crate::ffi::upload_wire::WirePrepare;
use crate::ffi::ProbeOutcome;

/// The width and height a user sees of the file that goes to the network (D-413): the result of the prepare step, or the source turned upright when nothing encodes it; `None` when the probe failed or does not fit the settings.
pub fn shown_size(prepare: Option<&WirePrepare>, probed: &ProbeOutcome, size: u64) -> Option<PixelSize> {
    match (prepare, probed) {
        (Some(WirePrepare::Image { edit, .. }), ProbeOutcome::Image { width, height, exif_orientation, .. }) => photo_plan(*width, *height, *exif_orientation, *edit).map(|plan| plan.output),
        (None, ProbeOutcome::Image { width, height, exif_orientation, .. }) => {
            let edit = PhotoEdit { max_size: None, enlarge: false, crop: None, output_size: None, mirror: false, rotation_degrees: 0 };
            photo_plan(*width, *height, *exif_orientation, edit).map(|plan| plan.upright)
        }
        (Some(WirePrepare::Video { limits, mirror, .. }), ProbeOutcome::Video { source, .. }) => {
            let plan = plan_video(*source, size, limits.clone().unwrap_or_else(super::default_video_limits)).ok()?;
            if plan.reencode || *mirror { return Some(PixelSize { width: plan.width, height: plan.height }); }
            Some(upright(source.width, source.height, source.rotation_degrees))
        }
        (None, ProbeOutcome::Video { source, .. }) => Some(upright(source.width, source.height, source.rotation_degrees)),
        _ => None,
    }
}

fn upright(width: u32, height: u32, rotation_degrees: i32) -> PixelSize {
    if rotation_degrees.rem_euclid(180) == 90 { PixelSize { width: height, height: width } } else { PixelSize { width, height } }
}
