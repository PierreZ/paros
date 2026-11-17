//! The one table of [`DriverTunables`](crate::DriverTunables).
//!
//! A tunable is one row of the `driver_tunables!` table in `lib.rs`: its
//! doc, its name, its type, its default and its floor. The macro generates
//! the struct field, the default, the floor check and the override entry
//! that `parosd` reads ([`TunableField`]) from that one row, so a new tunable
//! is declared in one place.

use std::time::Duration;

/// One field of [`DriverTunables`](crate::DriverTunables), as its row in the
/// table declares it: what an operator's override needs.
#[derive(Clone, Copy)]
pub struct TunableField {
    /// The field, by name.
    pub name: &'static str,
    /// The field is a duration and its raw value is milliseconds; otherwise
    /// the raw value is a count.
    pub millis: bool,
    /// Stores a raw value in the field. A count too large for the field's
    /// type saturates at the type's maximum, and so does a value above the
    /// row's `max`.
    pub set: fn(&mut crate::DriverTunables, u64),
}

impl std::fmt::Debug for TunableField {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TunableField")
            .field("name", &self.name)
            .field("millis", &self.millis)
            .finish_non_exhaustive()
    }
}

/// A type a tunable can have, and how its value reads as one `u64`: a count,
/// or milliseconds for a duration.
pub(crate) trait Knob: Copy {
    /// The raw value is milliseconds.
    const MILLIS: bool;
    /// The value as a raw `u64`, saturated at `u64::MAX`.
    fn raw(self) -> u64;
    /// A raw value as the type, saturated at the type's maximum.
    fn from_raw(raw: u64) -> Self;
}

impl Knob for Duration {
    const MILLIS: bool = true;
    fn raw(self) -> u64 {
        millis(self)
    }
    fn from_raw(raw: u64) -> Self {
        Duration::from_millis(raw)
    }
}

impl Knob for u64 {
    const MILLIS: bool = false;
    fn raw(self) -> u64 {
        self
    }
    fn from_raw(raw: u64) -> Self {
        raw
    }
}

impl Knob for u32 {
    const MILLIS: bool = false;
    fn raw(self) -> u64 {
        u64::from(self)
    }
    fn from_raw(raw: u64) -> Self {
        u32::try_from(raw).unwrap_or(u32::MAX)
    }
}

impl Knob for usize {
    const MILLIS: bool = false;
    fn raw(self) -> u64 {
        u64::try_from(self).unwrap_or(u64::MAX)
    }
    fn from_raw(raw: u64) -> Self {
        usize::try_from(raw).unwrap_or(usize::MAX)
    }
}

/// A duration in milliseconds, saturated at `u64::MAX`: the unit of a
/// duration's floor.
pub(crate) fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

/// Declares [`DriverTunables`](crate::DriverTunables) from one row per
/// field:
///
/// ```text
/// /// doc
/// name: Type = default, floor |t| expr;
/// name: Type = default, floor |t| expr, max expr;
/// ```
///
/// The floor is a `u64` (milliseconds for a duration), and `t` is the
/// profile under check, for a floor that follows another field. A `max`
/// caps an override. The macro generates the struct, `baseline()` (the
/// defaults), `floor_table()` (each field's name, raw value and floor, in
/// row order) and `FIELDS` (the overrides).
macro_rules! driver_tunables {
    (
        $(#[$meta:meta])*
        pub struct $name:ident {
            $(
                $(#[$field_meta:meta])*
                $field:ident : $ty:ty = $default:expr,
                floor |$t:pat_param| $floor:expr
                $(, max $max:expr)?
                ;
            )*
        }
    ) => {
        $(#[$meta])*
        pub struct $name {
            $(
                $(#[$field_meta])*
                pub $field: $ty,
            )*
        }

        impl $name {
            /// How many tunables the table declares.
            pub const COUNT: usize = [$(stringify!($field)),*].len();

            /// Every tunable, in the table's order, with how an override
            /// sets it.
            pub const FIELDS: [$crate::TunableField; Self::COUNT] = [
                $(
                    $crate::TunableField {
                        name: stringify!($field),
                        millis: <$ty as $crate::table::Knob>::MILLIS,
                        set: |tunables, raw| {
                            $(let raw = raw.min($max);)?
                            tunables.$field = <$ty as $crate::table::Knob>::from_raw(raw);
                        },
                    },
                )*
            ];

            /// The defaults of the table: the simulation's baseline.
            fn baseline() -> Self {
                Self {
                    $($field: $default,)*
                }
            }

            /// Every field: its name, its raw value and its floor, in the
            /// table's order.
            fn floor_table(&self) -> [(&'static str, u64, u64); Self::COUNT] {
                [
                    $(
                        (
                            stringify!($field),
                            $crate::table::Knob::raw(self.$field),
                            {
                                let $t = self;
                                $floor
                            },
                        ),
                    )*
                ]
            }
        }
    };
}
