package common

import (
	"regexp"
)

// To map regular expression with pattern type
var RegExTypeMap = map[string]string{
	"json_path": "/[0-9]+/",
}

// To map compiled regular expression with pattern type
var CompiledRegExMap = make(map[string]*regexp.Regexp)

const (
	KEY        = "key"
	KEY_PREFIX = "TEST"
	KEYSEP     = "::"
)

// DB error strings
const (
	DOCUMENT_NOT_FOUND   = "document not found"
	CONNECTION_SHUT_DOWN = "connection shut down"
	TIME_OUT             = "unambiguous timeout"
	PATH_NOT_EXISTS      = "sub-document error at index 0: path not found"
	SUB_DOCUMENT_ERROR   = "sub-document error"
	MULTI_LOOKUP_FAILED  = "could not execute one or more multi lookups or mutations"
)

// Patch operation consts
const (
	ADD     = "add"
	COPY    = "copy"
	MOVE    = "move"
	REMOVE  = "remove"
	REPLACE = "replace"
	TEST    = "test"
)

// Symbolic Constants
const (
	KEY_FARWARD_SLASH = "/"
	QUESTION_MARK     = "?"
	SPACE             = " "
	HYPHEN            = "-"
	TAB               = "\t"
	ALL               = "*"
	PIPE              = "|"
)

// General String constants
const (
	INVALID = "Invalid"
)

// Net Error constants
const (
	CONN_REFUSED = "connection refused"
)

// JSON related consts
const (
	OPENARRAY  = "["
	CLOSEARRAY = "]"
	OPENBRACE  = "{"
	CLOSEBRACE = "}"
	COLON      = ":"
	COMMA      = ","
	FSLASH     = "/"
	DOT        = "."
	JSON_PATH  = "json_path"
	EMPTYJSON  = "{}"
	EMPTYARRAY = "[]"
	HYPEN      = "-"
	QUOTE      = '"'
	S_QUOTE    = "'"
	ACUTE      = "`"
	EQUALTO    = "="
	ASTERISK   = "*"
)

// Return Value Consts
const (
	NIL = "nil"
)

// For UE data value
const (
	TRUE  = "true"
	FALSE = "false"
)

// Numeric consts
const (
	ZERO = iota
	ONE
	TWO
	THREE
	FOUR
	FIVE
)

// HTTP method const
const (
	PUT    = "PUT"
	PATCH  = "PATCH"
	DELETE = "DELETE"
	GET    = "GET"
	POST   = "POST"
)

// DB Commands
const (
	UPSERT       = "upsert"
	INSERT       = "insert"
	REPLACE_OP   = "replace"
	BULK_INSERT  = "bulk-insert"
	BULK_UPSERT  = "bulk-upsert"
	BULK_GET     = "bulk-get"
	BULK_DELETE  = "bulk-delete"
	BULK_REPLACE = "bulk-replace"
	DB_GET       = "get"
	DB_DELETE    = "delete"
	DB_PATCH     = "patch"
)
