//go:build linux

package recorder

import (
	"os/exec"
	"syscall"
)

// setSysProcAttr makes the kernel SIGKILL the child if the recorder dies
// unexpectedly (OOM kill, crash), so no orphaned ffmpeg keeps running.
func setSysProcAttr(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGKILL}
}
