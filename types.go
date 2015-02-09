package crate

// Crate column types id
// As listed in the documentation, see: https://crate.io/docs/stable/sql/rest.html#column-types
const (
	TypeNull         = 0
	TypeNotSupported = 1
	TypeByte         = 2
	TypeBoolean      = 3
	TypeString       = 4
	TypeIp           = 5
	TypeDouble       = 6
	TypeFloat        = 7
	TypeShort        = 8
	TypeInteger      = 9
	TypeLong         = 10
	TypeTimestamp    = 11
	TypeObject       = 12
	TypeGeoPoint     = 13
	TypeArray        = 100
	TypeSet          = 101
)
