use async_trait::async_trait;

use super::database::DatabaseSut;
use super::model::{Owner, Property, RefModel, Sut};
use super::value::Input;

pub mod alphabet;
pub mod properties;

#[cfg(test)]
mod tests;

/// The whole library on the stand in symbolic mode, with the properties it is asked for and no reference model.
pub struct LibraryOwner {
    pub properties: Vec<Property>,
}

#[async_trait]
impl Owner for LibraryOwner {
    fn name(&self) -> &'static str {
        "library"
    }

    fn alphabet(&self) -> Vec<Input> {
        alphabet::alphabet()
    }

    fn model(&self) -> Option<Box<dyn RefModel>> {
        None
    }

    async fn real(&self) -> Box<dyn Sut> {
        Box::new(DatabaseSut::symbolic())
    }

    fn properties(&self) -> Vec<Property> {
        self.properties.clone()
    }
}
