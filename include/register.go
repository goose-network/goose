// Package include blank-imports every built-in outbound plugin so their
// init() registrations run. External plugin repos are added the same way:
// import _ "github.com/example/goose-plugin-foo" here (or in the main
// package). This mirrors sing-box's include/registry and Xray's distro/all.
package include

import (
	_ "github.com/goose-network/goose-plugin-mihomo"
	_ "github.com/goose-network/goose-plugin-psiphon"
	_ "github.com/goose-network/goose-plugin-singbox"
	_ "github.com/goose-network/goose/plugins/direct"
	_ "github.com/goose-network/goose/plugins/http"
	_ "github.com/goose-network/goose/plugins/socks5"
	_ "github.com/goose-network/goose/plugins/subscription"
)
