package gobptree

import (
	"fmt"
	"os"
	"time"
)

// Use a time format that is both RFC-3339 and ISO-8601 compliant:
// %Y-%M-%DT%h:%m:%sZ
const time_format = "2006-01-02T15:04:05Z07:00"

func newDebug(ok bool, f string, a ...any) func(string, ...any) {
	if ok {
		prefix := fmt.Sprintf(f, a...)
		return func(f string, a ...any) {
			now := time.Now().Format(time_format)
			content := fmt.Sprintf(f, a...)
			fmt.Fprintf(os.Stderr, "%s %s: %s", now, prefix, content)
		}
	}
	return func(string, ...any) {}
}
