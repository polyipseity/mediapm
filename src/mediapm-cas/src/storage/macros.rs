//! Internal macros for implementing CAS traits on wrapper types.

macro_rules! impl_cas_wrapper_traits {
    ($ty:ty) => {
        #[async_trait::async_trait]
        impl $crate::api::CasApi for $ty {
            async fn put(
                &self,
                data: ::bytes::Bytes,
            ) -> Result<$crate::hash::Hash, $crate::error::CasError> {
                self.0.put(data).await
            }

            async fn get(
                &self,
                hash: $crate::hash::Hash,
            ) -> Result<::bytes::Bytes, $crate::error::CasError> {
                self.0.get(hash).await
            }

            async fn stat(
                &self,
                hash: $crate::hash::Hash,
            ) -> Result<$crate::api::ObjectMeta, $crate::error::CasError> {
                self.0.stat(hash).await
            }

            async fn delete(
                &self,
                hash: $crate::hash::Hash,
            ) -> Result<(), $crate::error::CasError> {
                self.0.delete(hash).await
            }
        }

        #[async_trait::async_trait]
        impl $crate::api::CasMaintenanceApi for $ty {
            async fn run_maintenance_cycle(
                &self,
            ) -> Result<$crate::api::OptimizeReport, $crate::error::CasError> {
                self.0.run_maintenance_cycle().await
            }

            async fn prune_constraints(
                &self,
            ) -> Result<$crate::api::PruneReport, $crate::error::CasError> {
                self.0.prune_constraints().await
            }

            async fn list_hashes(
                &self,
            ) -> Result<Vec<$crate::hash::Hash>, $crate::error::CasError> {
                self.0.list_hashes().await
            }
        }

        #[async_trait::async_trait]
        impl $crate::api::ConstraintApi for $ty {
            async fn set_constraint(
                &self,
                target: $crate::hash::Hash,
                bases: ::std::collections::BTreeSet<$crate::hash::Hash>,
            ) -> Result<(), $crate::error::CasError> {
                self.0.set_constraint(target, bases).await
            }

            async fn get_constraint(
                &self,
                target: $crate::hash::Hash,
            ) -> Result<::std::collections::BTreeSet<$crate::hash::Hash>, $crate::error::CasError>
            {
                self.0.get_constraint(target).await
            }

            async fn patch_constraint(
                &self,
                target: $crate::hash::Hash,
                patch: $crate::api::ConstraintPatch,
            ) -> Result<(), $crate::error::CasError> {
                self.0.patch_constraint(target, patch).await
            }
        }
    };
}
