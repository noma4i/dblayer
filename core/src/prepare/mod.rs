//! The pure rules of the prepare step: what a video or an image becomes before it is uploaded.

mod encode;
mod photo;
mod photo_plan;
mod plan;
mod settings;
mod settings;
mod video_metadata;

pub use encode::ImageFormat;
pub use photo::{jpeg_quality, photo_turn, PhotoTurn};
pub use photo_plan::{photo_plan, CropFractions, PhotoEdit, PhotoPlan, PixelRect, PixelSize};
pub use plan::{check, default_video_limits, plan_video, BitrateStep, VideoLimits, VideoPlan, VideoSource};
pub use settings::check_settings;
pub use settings::check_settings;
pub use video_metadata::video_metadata_to_strip;

#[cfg(test)]
mod tests;
