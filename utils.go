package sqliteq

import (
	"fmt"
	"strings"
)

// Applies quotes to an identifier escaping any internal quotes.
// See: https://www.sqlite.org/lang_keywords.html
func quoteIdent(name string) string {
	// Replace quotes with dobule quotes
	escaped := strings.ReplaceAll(name, `"`, `""`)
	return `"` + escaped + `"`
}

// foldName lower-cases ASCII letters only, matching how SQLite compares
// table names, so names that share a table share a handle.
func foldName(name string) string {
	return strings.Map(func(r rune) rune {
		if r >= 'A' && r <= 'Z' {
			return r + 'a' - 'A'
		}
		return r
	}, name)
}

// validName checks a queue name for the folder layout, where it becomes a
// file name: only A-Z a-z 0-9 . _ -, and not SQLite's reserved sqlite_ prefix.
func validName(name string) error {
	if name == "" {
		return fmt.Errorf("sqliteq: queue name must not be empty")
	}
	if strings.HasPrefix(strings.ToLower(name), "sqlite_") {
		return fmt.Errorf("sqliteq: queue name %q must not start with sqlite_", name)
	}
	for _, c := range name {
		ok := c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '.' || c == '_' || c == '-'
		if !ok {
			return fmt.Errorf("sqliteq: queue name %q may only contain A-Z a-z 0-9 . _ - (or use WithSingleFile)", name)
		}
	}
	return nil
}
