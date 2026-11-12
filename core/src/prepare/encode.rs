/// The file format of an encoded image.
#[derive(Debug, Clone, Copy, PartialEq, Eq, uniffi::Enum)]
pub enum ImageFormat {
    Jpeg,
    Png,
}
