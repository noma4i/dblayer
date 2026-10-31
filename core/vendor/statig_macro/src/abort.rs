use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};

macro_rules! abort {
    ($error:expr) => {
        $crate::abort::raise($error)
    };
    ($spanned:expr, $message:expr) => {
        $crate::abort::raise(::syn::Error::new_spanned(&$spanned, $message))
    };
    ($spanned:expr, $message:expr; help = $help:expr) => {
        $crate::abort::raise(::syn::Error::new_spanned(
            &$spanned,
            format!("{}\n  = help: {}", $message, $help),
        ))
    };
}

pub fn raise(error: syn::Error) -> ! {
    resume_unwind(Box::new(error))
}

pub fn expand(body: impl FnOnce() -> proc_macro2::TokenStream) -> proc_macro2::TokenStream {
    match catch_unwind(AssertUnwindSafe(body)) {
        Ok(tokens) => tokens,
        Err(payload) => match payload.downcast::<syn::Error>() {
            Ok(error) => error.to_compile_error(),
            Err(other) => resume_unwind(other),
        },
    }
}
