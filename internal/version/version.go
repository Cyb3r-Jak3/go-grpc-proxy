package version

import (
	"fmt"
	"runtime/debug"
)

var (
	Version       = "dev"
	Date          = "unknown"
	Commit        = "unknown"
	VersionString = fmt.Sprintf("%s (built %s)", Version, Date)
)

func init() {
	if buildInfo, available := debug.ReadBuildInfo(); available {
		VersionString = fmt.Sprintf("%s (built %s with %s)", Version, Date, buildInfo.GoVersion)
	}
}
