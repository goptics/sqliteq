package sqliteq

import "strings"

// Applies quotes to an identifier escaping any internal quotes.
// See: https://www.sqlite.org/lang_keywords.html
func quoteIdent(name string) string {
	// Replace quotes with dobule quotes
	escaped := strings.ReplaceAll(name, `"`, `""`)
	return `"` + escaped + `"`
}
