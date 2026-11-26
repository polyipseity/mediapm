//! Pure feature-gate resolution for login-gated media-tagger services.
//!
//! The gate answers one question at the point of need: may this run use a
//! service that depends on a login? It never probes login state up front, and
//! when the service flag is disabled it never consults login state at all.

/// What a step needs from a login-gated service at the point of use.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Need {
    /// The step does not need the service.
    None,
    /// The step can succeed without the service; dropping it is logged.
    Optional(&'static str),
    /// The step cannot succeed without the service; denial is an error.
    Required(&'static str),
}

/// Whether a login-gated service currently has usable credentials.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthState {
    /// Credentials are present and usable.
    LoggedIn,
    /// Credentials are absent or unusable.
    LoggedOut,
}

/// The decision a gate makes for one need.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GateOutcome {
    /// The service may be used.
    Use,
    /// An optional want was dropped because the service flag is disabled.
    Dropped {
        /// The optional field that was not fetched.
        field: &'static str,
        /// The service whose flag disabled the field.
        service: &'static str,
    },
    /// The need cannot be satisfied.
    Fail {
        /// A user-facing message naming the service and the cause.
        message: String,
    },
}

/// Service-specific labels used to format gate error messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GateLabels {
    /// Human-readable service name, for example `MusicBrainz`.
    pub service: &'static str,
    /// The CLI/config flag that enables logged-in features for the service.
    pub disabled_flag: &'static str,
    /// Service-specific hint explaining how to provide credentials.
    pub missing_login_hint: &'static str,
}

/// Resolves whether a login-gated service may be used.
///
/// `auth` is consulted only when `flag` is `true` and `need` is not [`Need::None`].
/// When the flag is `false`, login state is intentionally unobservable.
#[must_use]
pub fn resolve_gate(
    flag: bool,
    need: Need,
    labels: GateLabels,
    auth: impl FnOnce() -> AuthState,
) -> GateOutcome {
    match need {
        Need::None => GateOutcome::Use,
        Need::Optional(field) if !flag => GateOutcome::Dropped { field, service: labels.service },
        Need::Required(field) if !flag => GateOutcome::Fail {
            message: format!(
                "{field} is required but logged-in features for {service} are disabled ({disabled_flag}=false)",
                service = labels.service,
                disabled_flag = labels.disabled_flag,
            ),
        },
        Need::Optional(field) | Need::Required(field) => match auth() {
            AuthState::LoggedIn => GateOutcome::Use,
            AuthState::LoggedOut => {
                let wanted_or_required = match need {
                    Need::Optional(_) => "wanted",
                    Need::Required(_) | Need::None => "required",
                };
                GateOutcome::Fail {
                    message: format!(
                        "{field} is {wanted_or_required} but {hint}",
                        hint = labels.missing_login_hint,
                    ),
                }
            }
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    const TEST_LABELS: GateLabels = GateLabels {
        service: "TestService",
        disabled_flag: "--enable-logged-in-features-testservice",
        missing_login_hint: "no TestService login is available (run 'mediapm auth testservice login')",
    };

    /// Builds a panic-on-call auth probe so tests can assert it was not consulted.
    fn auth_must_not_be_called() -> impl FnOnce() -> AuthState {
        || panic!("login state must not be consulted")
    }

    /// Builds an auth probe that records whether it was called exactly once.
    fn counting_auth(state: AuthState, called: &Cell<bool>) -> impl FnOnce() -> AuthState + '_ {
        move || {
            assert!(!called.replace(true), "auth must be called at most once");
            state
        }
    }

    #[test]
    fn need_none_returns_use_and_does_not_consult_auth() {
        let outcome = resolve_gate(true, Need::None, TEST_LABELS, auth_must_not_be_called());
        assert_eq!(outcome, GateOutcome::Use);
    }

    #[test]
    fn flag_true_optional_logged_in_uses_service() {
        let called = Cell::new(false);
        let outcome = resolve_gate(
            true,
            Need::Optional("user-genres"),
            TEST_LABELS,
            counting_auth(AuthState::LoggedIn, &called),
        );
        assert_eq!(outcome, GateOutcome::Use);
        assert!(called.get(), "auth must be called when flag is true and need exists");
    }

    #[test]
    fn flag_true_optional_logged_out_fails_with_wanted_message() {
        let called = Cell::new(false);
        let outcome = resolve_gate(
            true,
            Need::Optional("user-genres"),
            TEST_LABELS,
            counting_auth(AuthState::LoggedOut, &called),
        );
        assert_eq!(
            outcome,
            GateOutcome::Fail {
                message: "user-genres is wanted but no TestService login is available (run 'mediapm auth testservice login')"
                    .to_string(),
            }
        );
        assert!(called.get());
    }

    #[test]
    fn flag_true_required_logged_in_uses_service() {
        let called = Cell::new(false);
        let outcome = resolve_gate(
            true,
            Need::Required("AcoustID identification"),
            TEST_LABELS,
            counting_auth(AuthState::LoggedIn, &called),
        );
        assert_eq!(outcome, GateOutcome::Use);
        assert!(called.get());
    }

    #[test]
    fn flag_true_required_logged_out_fails_with_required_message() {
        let called = Cell::new(false);
        let outcome = resolve_gate(
            true,
            Need::Required("AcoustID identification"),
            TEST_LABELS,
            counting_auth(AuthState::LoggedOut, &called),
        );
        assert_eq!(
            outcome,
            GateOutcome::Fail {
                message: "AcoustID identification is required but no TestService login is available (run 'mediapm auth testservice login')"
                    .to_string(),
            }
        );
        assert!(called.get());
    }

    #[test]
    fn flag_false_optional_logged_in_drops_without_consulting_auth() {
        let outcome = resolve_gate(
            false,
            Need::Optional("user-genres"),
            TEST_LABELS,
            auth_must_not_be_called(),
        );
        assert_eq!(outcome, GateOutcome::Dropped { field: "user-genres", service: "TestService" });
    }

    #[test]
    fn flag_false_optional_logged_out_drops_without_consulting_auth() {
        let outcome = resolve_gate(
            false,
            Need::Optional("user-genres"),
            TEST_LABELS,
            auth_must_not_be_called(),
        );
        assert_eq!(outcome, GateOutcome::Dropped { field: "user-genres", service: "TestService" });
    }

    #[test]
    fn flag_false_required_logged_in_fails_without_consulting_auth() {
        let outcome = resolve_gate(
            false,
            Need::Required("AcoustID identification"),
            TEST_LABELS,
            auth_must_not_be_called(),
        );
        assert_eq!(
            outcome,
            GateOutcome::Fail {
                message: "AcoustID identification is required but logged-in features for TestService are disabled (--enable-logged-in-features-testservice=false)"
                    .to_string(),
            }
        );
    }

    #[test]
    fn flag_false_required_logged_out_fails_without_consulting_auth() {
        let outcome = resolve_gate(
            false,
            Need::Required("AcoustID identification"),
            TEST_LABELS,
            auth_must_not_be_called(),
        );
        assert_eq!(
            outcome,
            GateOutcome::Fail {
                message: "AcoustID identification is required but logged-in features for TestService are disabled (--enable-logged-in-features-testservice=false)"
                    .to_string(),
            }
        );
    }

    #[test]
    fn need_none_with_flag_false_does_not_consult_auth() {
        let outcome = resolve_gate(false, Need::None, TEST_LABELS, auth_must_not_be_called());
        assert_eq!(outcome, GateOutcome::Use);
    }

    #[test]
    fn acoustid_missing_key_message_names_service_and_hint() {
        let labels = GateLabels {
            service: "AcoustID",
            disabled_flag: "--enable-logged-in-features-acoustid",
            missing_login_hint: "no AcoustID API key is available (set --acoustid-api-key or ACOUSTID_API_KEY)",
        };
        let outcome = resolve_gate(true, Need::Required("AcoustID identification"), labels, || {
            AuthState::LoggedOut
        });
        assert_eq!(
            outcome,
            GateOutcome::Fail {
                message: "AcoustID identification is required but no AcoustID API key is available (set --acoustid-api-key or ACOUSTID_API_KEY)"
                    .to_string(),
            }
        );
    }

    #[test]
    fn musicbrainz_wanted_message_is_distinguishable_from_required() {
        let labels = GateLabels {
            service: "MusicBrainz",
            disabled_flag: "--enable-logged-in-features-musicbrainz",
            missing_login_hint: "no MusicBrainz login is available (run 'mediapm auth musicbrainz login')",
        };
        let wanted =
            resolve_gate(true, Need::Optional("user genres"), labels, || AuthState::LoggedOut);
        let required =
            resolve_gate(true, Need::Required("user genres"), labels, || AuthState::LoggedOut);
        assert!(matches!(wanted, GateOutcome::Fail { message } if message.contains("wanted but")));
        assert!(
            matches!(required, GateOutcome::Fail { message } if message.contains("required but"))
        );
    }
}
