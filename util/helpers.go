package util

import "strings"

func ContainsKeyword(text, keyword string) bool {
	return len(text) > 0 && (StringContains(text, keyword))
}

func StringContains(text, keyword string) bool {
	return strings.Contains(strings.ToLower(text), strings.ToLower(keyword))
}
