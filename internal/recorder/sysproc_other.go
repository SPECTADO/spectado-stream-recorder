//go:build !linux

package recorder

import "os/exec"

// setSysProcAttr is a no-op on platforms without Pdeathsig.
func setSysProcAttr(cmd *exec.Cmd) {}
