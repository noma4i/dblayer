use async_trait::async_trait;

use super::database::DatabaseSut;
use super::model::{Owner, Property, RefModel, Sut};
use super::value::Input;

pub mod alphabet;
pub mod properties;
pub mod reference;

#[cfg(test)]
mod tests;

/// The whole library on the stand in symbolic mode, with the properties it is asked for and, when `reference` is set, the reference model of `reference/`.
pub struct LibraryOwner {
    pub properties: Vec<Property>,
    pub reference: bool,
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
        self.reference.then(|| Box::new(reference::LibraryRef::default()) as Box<dyn RefModel>)
    }

    async fn real(&self) -> Box<dyn Sut> {
        Box::new(DatabaseSut::symbolic_hiding(if self.reference { vec!["subscription"] } else { Vec::new() }))
    }

    fn properties(&self) -> Vec<Property> {
        self.properties.clone()
    }
}
