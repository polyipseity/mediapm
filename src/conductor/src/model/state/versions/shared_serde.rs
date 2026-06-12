//! Shared serde implementations for versioned state types.
//!
//! V1 and V2 [`OutputSaveMode`] custom serialization and [`ToolMetadata`]
//! custom deserialization are structurally identical across versions except
//! for the concrete type names. These macros generate the per-version impls
//! from a single definition.

/// Generates [`Serialize`] and [`Deserialize`] for a version-specific
/// `OutputSaveMode` enum.
///
/// `$type_name`: concrete type to implement (e.g. `OutputSaveModeV1`).
/// `$visitor_name`: unique visitor struct name (e.g. `OutputSaveModeV1Visitor`).
#[macro_export]
macro_rules! impl_output_save_mode_serde {
    ($type_name:ident, $visitor_name:ident) => {
        impl ::serde::Serialize for $type_name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: ::serde::Serializer,
            {
                match self {
                    Self::Bool(value) => serializer.serialize_bool(*value),
                    Self::Full => serializer.serialize_str("full"),
                }
            }
        }

        impl<'de> ::serde::Deserialize<'de> for $type_name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: ::serde::Deserializer<'de>,
            {
                struct $visitor_name;

                impl<'de> ::serde::de::Visitor<'de> for $visitor_name {
                    type Value = $type_name;

                    fn expecting(&self, formatter: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                        formatter.write_str(
                            "a boolean save mode, or one of the strings \"full\", \"saved\", \"unsaved\"",
                        )
                    }

                    fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E>
                    where
                        E: ::serde::de::Error,
                    {
                        Ok($type_name::Bool(value))
                    }

                    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
                    where
                        E: ::serde::de::Error,
                    {
                        if value.eq_ignore_ascii_case("full") {
                            return Ok($type_name::Full);
                        }

                        if value.eq_ignore_ascii_case("saved")
                            || value.eq_ignore_ascii_case("true")
                        {
                            return Ok($type_name::Bool(true));
                        }

                        if value.eq_ignore_ascii_case("unsaved")
                            || value.eq_ignore_ascii_case("false")
                        {
                            return Ok($type_name::Bool(false));
                        }

                        Err(E::invalid_value(::serde::de::Unexpected::Str(value), &self))
                    }

                    fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
                    where
                        E: ::serde::de::Error,
                    {
                        self.visit_str(&value)
                    }
                }

                deserializer.deserialize_any($visitor_name)
            }
        }
    };
}

/// Generates custom [`Deserialize`] for a version-specific `ToolMetadata` enum.
///
/// `$metadata_type`: concrete enum type (e.g. `ToolMetadataV1`).
/// `$builtin_kind_type`: concrete builtin metadata kind type (e.g. `BuiltinMetadataKindV1`).
/// `$wire_type`: unique inner wire-struct name (e.g. `BuiltinMetadataWireV1`).
#[macro_export]
macro_rules! impl_tool_metadata_deserialize {
    ($metadata_type:ident, $builtin_kind_type:ident, $wire_type:ident) => {
        impl<'de> ::serde::Deserialize<'de> for $metadata_type {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: ::serde::Deserializer<'de>,
            {
                /// Wire shape for strict builtin metadata decoding.
                #[derive(Debug, Clone, PartialEq, Eq, ::serde::Deserialize)]
                #[serde(deny_unknown_fields)]
                struct $wire_type {
                    /// Builtin kind marker.
                    kind: $builtin_kind_type,
                    /// Builtin name.
                    name: String,
                    /// Builtin semantic version.
                    version: String,
                }

                let value = ::serde_json::Value::deserialize(deserializer)?;
                let kind =
                    value.get("kind").and_then(::serde_json::Value::as_str).ok_or_else(|| {
                        ::serde::de::Error::custom("tool metadata must define string field 'kind'")
                    })?;

                match kind {
                    "builtin" => {
                        let builtins: $wire_type =
                            ::serde_json::from_value(value).map_err(::serde::de::Error::custom)?;
                        Ok($metadata_type::Builtin {
                            kind: builtins.kind,
                            name: builtins.name,
                            version: builtins.version,
                        })
                    }
                    "executable" => {
                        let spec: $crate::model::config::ToolSpec =
                            ::serde_json::from_value(value).map_err(::serde::de::Error::custom)?;
                        match spec.kind {
                            $crate::model::config::ToolKindSpec::Executable { .. } => {
                                Ok($metadata_type::Executable(spec))
                            }
                            $crate::model::config::ToolKindSpec::Builtin { .. } => {
                                Err(::serde::de::Error::custom(
                                    "executable metadata must decode to executable tool kind",
                                ))
                            }
                        }
                    }
                    other => Err(::serde::de::Error::custom(format!(
                        "unsupported tool metadata kind '{other}'"
                    ))),
                }
            }
        }
    };
}
