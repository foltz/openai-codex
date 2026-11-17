//! Captured gateway state shared with primary owner-pinned authentication.
//! Resolution and enforcement remain in the actual gateway provider.

/// Token-free gateway state captured with the actual selected credential.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GatewayAdmission {
    NotConfigured,
    Unbound,
    Bound(crate::OAuthSessionBinding),
    ExportBound(GatewayExportBinding),
}

/// Token-free identity of one observed command-export cache entry. The issuing
/// wrapper retains owner/policy proof; this value alone grants no authority.
#[derive(Clone)]
pub struct GatewayExportBinding {
    source: std::sync::Arc<()>,
    generation: u64,
}
impl GatewayExportBinding {
    pub fn new(source: std::sync::Arc<()>, generation: u64) -> Self {
        Self { source, generation }
    }
    pub fn matches(&self, source: &std::sync::Arc<()>, generation: u64) -> bool {
        std::sync::Arc::ptr_eq(&self.source, source) && self.generation == generation
    }
}
impl PartialEq for GatewayExportBinding {
    fn eq(&self, other: &Self) -> bool {
        self.matches(&other.source, other.generation)
    }
}
impl Eq for GatewayExportBinding {}
impl std::fmt::Debug for GatewayExportBinding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GatewayExportBinding")
            .field("generation", &self.generation)
            .finish_non_exhaustive()
    }
}
