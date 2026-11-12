//! The rows of `sync_upload_prepare` and `sync_upload_prepare_step`: the prepare settings of a transfer, one row of columns per transfer.

use std::collections::BTreeMap;

use rusqlite::{named_params, Connection};

use crate::ffi::upload_wire::{CannotDecode, WirePrepare};
use crate::prepare::{BitrateStep, CropFractions, ImageFormat, PhotoEdit, PixelSize, VideoLimits};

use super::StoreError;

fn id_of(transfer_id: u64) -> Result<i64, StoreError> {
    i64::try_from(transfer_id).map_err(|_| StoreError::Constraint("transfer id is too large".into()))
}

fn format_text(format: ImageFormat) -> &'static str {
    match format {
        ImageFormat::Jpeg => "jpeg",
        ImageFormat::Png => "png",
    }
}

fn cannot_decode_text(choice: CannotDecode) -> &'static str {
    match choice {
        CannotDecode::Fail => "fail",
        CannotDecode::UploadSource => "upload_source",
    }
}

pub(super) fn put_prepare(connection: &Connection, transfer_id: u64, prepare: &WirePrepare) -> Result<(), StoreError> {
    let id = id_of(transfer_id)?;
    remove_prepare(connection, transfer_id)?;
    match prepare {
        WirePrepare::Video { limits, mirror, on_cannot_decode, max_bytes } => {
            connection.execute(include_str!("sql/prepare_rows/put_prepare.sql"), named_params! {
                ":transfer_id": id, ":kind": "video", ":mirror": mirror,
                ":max_long_side": limits.as_ref().map(|limits| limits.max_long_side), ":min_frame_rate": limits.as_ref().map(|limits| limits.min_frame_rate),
                ":max_frame_rate": limits.as_ref().map(|limits| limits.max_frame_rate), ":source_bitrate_cap_percent": limits.as_ref().map(|limits| limits.source_bitrate_cap_percent),
                ":audio_bitrate": limits.as_ref().map(|limits| limits.audio_bitrate), ":audio_channels": limits.as_ref().map(|limits| limits.audio_channels), ":audio_sample_rate": limits.as_ref().map(|limits| limits.audio_sample_rate),
                ":max_width": None::<u32>, ":max_height": None::<u32>, ":enlarge": None::<bool>, ":crop_x": None::<f64>, ":crop_y": None::<f64>, ":crop_width": None::<f64>, ":crop_height": None::<f64>,
                ":output_width": None::<u32>, ":output_height": None::<u32>, ":rotation_degrees": None::<u32>, ":quality": None::<f64>, ":format": None::<&str>, ":on_cannot_decode": cannot_decode_text(*on_cannot_decode), ":max_bytes": max_bytes.map(|limit| i64::try_from(limit).unwrap_or(i64::MAX)),
            })?;
            for step in limits.iter().flat_map(|limits| &limits.corridors) {
                connection.execute(include_str!("sql/prepare_rows/put_step.sql"), (id, step.from_long_side, step.min_bps, step.max_bps))?;
            }
        }
        WirePrepare::Image { edit, quality, format, max_bytes } => {
            connection.execute(include_str!("sql/prepare_rows/put_prepare.sql"), named_params! {
                ":transfer_id": id, ":kind": "image", ":mirror": edit.mirror,
                ":max_long_side": None::<u32>, ":min_frame_rate": None::<u32>, ":max_frame_rate": None::<u32>, ":source_bitrate_cap_percent": None::<u32>,
                ":audio_bitrate": None::<u32>, ":audio_channels": None::<u32>, ":audio_sample_rate": None::<u32>,
                ":max_width": edit.max_size.map(|size| size.width), ":max_height": edit.max_size.map(|size| size.height), ":enlarge": edit.enlarge,
                ":crop_x": edit.crop.map(|crop| crop.x), ":crop_y": edit.crop.map(|crop| crop.y), ":crop_width": edit.crop.map(|crop| crop.width), ":crop_height": edit.crop.map(|crop| crop.height),
                ":output_width": edit.output_size.map(|size| size.width), ":output_height": edit.output_size.map(|size| size.height), ":rotation_degrees": edit.rotation_degrees,
                ":quality": quality, ":format": format_text(*format), ":on_cannot_decode": None::<&str>, ":max_bytes": max_bytes.map(|limit| i64::try_from(limit).unwrap_or(i64::MAX)),
            })?;
        }
    }
    Ok(())
}

pub(super) fn remove_prepare(connection: &Connection, transfer_id: u64) -> Result<(), StoreError> {
    let id = id_of(transfer_id)?;
    connection.execute(include_str!("sql/prepare_rows/remove_prepare.sql"), [id])?;
    connection.execute(include_str!("sql/prepare_rows/remove_step.sql"), [id])?;
    Ok(())
}

/// The settings of the transfer under `[model, row_id, action]`, removed before its `sync_upload` row goes.
pub(super) fn remove_prepare_of(connection: &Connection, key: [&str; 3]) -> Result<(), StoreError> {
    connection.execute(include_str!("sql/prepare_rows/remove_prepare_of_key.sql"), key)?;
    connection.execute(include_str!("sql/prepare_rows/remove_step_of_key.sql"), key)?;
    Ok(())
}

/// The settings whose transfer row is gone: after `sync_upload` is cleared.
pub(crate) fn clear_prepare_without_upload(connection: &Connection) -> rusqlite::Result<()> {
    connection.execute(include_str!("sql/prepare_rows/clear_prepare_without_upload.sql"), [])?;
    connection.execute(include_str!("sql/prepare_rows/clear_step_without_upload.sql"), [])?;
    Ok(())
}

/// The settings of every transfer by its id.
pub(crate) fn read_prepares(connection: &Connection) -> rusqlite::Result<BTreeMap<i64, WirePrepare>> {
    let mut corridors: BTreeMap<i64, Vec<BitrateStep>> = BTreeMap::new();
    let mut statement = connection.prepare(include_str!("sql/prepare_rows/read_step.sql"))?;
    for step in statement.query_map([], |row| Ok((row.get::<_, i64>(0)?, BitrateStep { from_long_side: row.get(1)?, min_bps: row.get(2)?, max_bps: row.get(3)? })))? {
        let (id, step) = step?;
        corridors.entry(id).or_default().push(step);
    }
    let mut statement = connection.prepare(include_str!("sql/prepare_rows/read_prepare.sql"))?;
    let rows = statement.query_map([], |row| {
        let id: i64 = row.get(0)?;
        let (kind, mirror): (String, bool) = (row.get(1)?, row.get(2)?);
        if kind == "video" {
            let limits = row.get::<_, Option<u32>>(3)?.map(|max_long_side| Ok::<_, rusqlite::Error>(VideoLimits {
                max_long_side, corridors: corridors.remove(&id).unwrap_or_default(), min_frame_rate: row.get(4)?, max_frame_rate: row.get(5)?,
                source_bitrate_cap_percent: row.get(6)?, audio_bitrate: row.get(7)?, audio_channels: row.get(8)?, audio_sample_rate: row.get(9)?,
            })).transpose()?;
            let on_cannot_decode = if row.get::<_, Option<String>>(22)?.as_deref() == Some("upload_source") { CannotDecode::UploadSource } else { CannotDecode::Fail };
            return Ok((id, WirePrepare::Video { limits, mirror, on_cannot_decode, max_bytes: row.get::<_, Option<i64>>(23)?.map(|limit| limit as u64) }));
        }
        let size = |width: usize, height: usize| -> rusqlite::Result<Option<PixelSize>> {
            Ok(row.get::<_, Option<u32>>(width)?.zip(row.get::<_, Option<u32>>(height)?).map(|(width, height)| PixelSize { width, height }))
        };
        let crop = match row.get::<_, Option<f64>>(13)? {
            Some(x) => Some(CropFractions { x, y: row.get(14)?, width: row.get(15)?, height: row.get(16)? }),
            None => None,
        };
        let edit = PhotoEdit { max_size: size(10, 11)?, enlarge: row.get::<_, Option<bool>>(12)?.unwrap_or(false), crop, output_size: size(17, 18)?, mirror, rotation_degrees: row.get::<_, Option<u32>>(19)?.unwrap_or(0) };
        let format = if row.get::<_, Option<String>>(21)?.as_deref() == Some("png") { ImageFormat::Png } else { ImageFormat::Jpeg };
        Ok((id, WirePrepare::Image { edit, quality: row.get(20)?, format, max_bytes: row.get::<_, Option<i64>>(23)?.map(|limit| limit as u64) }))
    })?;
    rows.collect()
}
