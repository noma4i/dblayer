

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UploadOperation {
    pub name: String,
    pub root_path: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MultipartOperations {
    pub initiate: UploadOperation,
    pub presign: UploadOperation,
    pub complete: UploadOperation,
    pub abort: UploadOperation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UploadOperations {
    pub create_blob: Option<UploadOperation>,
    pub multipart: Option<MultipartOperations>,
}

impl UploadOperation {
    pub fn new(name: &str) -> Self {
        Self { name: name.to_string(), root_path: None }
    }

    pub fn rooted(name: &str, root_path: &str) -> Self {
        Self { name: name.to_string(), root_path: Some(root_path.to_string()) }
    }
}

impl UploadOperations {
    fn declared(&self) -> impl Iterator<Item = &UploadOperation> {
        let multipart = self.multipart.iter().flat_map(|steps| [&steps.initiate, &steps.presign, &steps.complete, &steps.abort]);
        self.create_blob.iter().chain(multipart)
    }

    pub fn root_of(&self, name: &str) -> Option<&str> {
        self.declared().find(|operation| operation.name == name).and_then(|operation| operation.root_path.as_deref())
    }

    pub fn check(&self, registered: impl Fn(&str) -> bool) -> Result<(), String> {
        if self.create_blob.is_none() && self.multipart.is_none() {
            return Err("upload operations declare neither createBlob nor multipart".into());
        }
        let declared: Vec<_> = self.declared().collect();
        for (index, operation) in declared.iter().enumerate() {
            if declared[..index].iter().any(|earlier| earlier.name == operation.name && earlier.root_path != operation.root_path) {
                return Err(format!("upload operation {} is declared with different rootPath values", operation.name));
            }
        }
        match declared.iter().find(|operation| !registered(&operation.name)) {
            Some(operation) => Err(format!("upload operation {} is not registered", operation.name)),
            None => Ok(()),
        }
    }

    #[cfg(test)]
    pub fn empty() -> Self {
        Self { create_blob: None, multipart: None }
    }

    #[cfg(test)]
    pub fn create_blob(mut self, name: &str) -> Self {
        self.create_blob = Some(UploadOperation::new(name));
        self
    }

    #[cfg(test)]
    pub fn multipart(mut self, initiate: &str, presign: &str, complete: &str, abort: &str) -> Self {
        self.multipart = Some(MultipartOperations {
            initiate: UploadOperation::new(initiate),
            presign: UploadOperation::new(presign),
            complete: UploadOperation::new(complete),
            abort: UploadOperation::new(abort),
        });
        self
    }
}
