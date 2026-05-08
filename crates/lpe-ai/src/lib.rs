mod provider;

pub use crate::provider::{
    summarize_projection, InferenceRequest, InferenceResponse, LocalModelDescriptor,
    LocalModelProvider, ModelCapability, StubLocalModelProvider,
};
