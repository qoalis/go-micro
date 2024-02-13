package ids

import (
	"github.com/qoalis/go-micro/util/h"
	"github.com/rs/xid"
	"strings"
)

func NewId(prefix string) string {
	guid := xid.New()
	if h.IsNotEmpty(prefix) && !strings.HasSuffix(prefix, "_") && !strings.HasSuffix(prefix, "-") {
		prefix += "_"
	}
	return prefix + guid.String()
}

func NewIdPtr(prefix string) *string {
	value := NewId(prefix)
	return &value
}
