/*
Copyright 2018 The Kubernetes Authors.
Copyright 2022 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package csimounter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	sidecarmounter "github.com/googlecloudplatform/gcs-fuse-csi-driver/pkg/sidecar_mounter"
	"github.com/googlecloudplatform/gcs-fuse-csi-driver/pkg/util"
	"github.com/googlecloudplatform/gcs-fuse-csi-driver/pkg/webhook"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"
	"k8s.io/mount-utils"
)

const (
	// Note: These masks now affect BOTH User and Group permissions
	rwMask        os.FileMode = 0660 // User RW, Group RW
	roMask        os.FileMode = 0440 // User R,  Group R
	execMask      os.FileMode = 0110 // User X,  Group X
	targetFsGroup             = 65534
)

const (
	socketName                       = "socket"
	readAheadKBMountFlagRegexPattern = "^read_ahead_kb=(.+)$"
	readAheadKBMountFlag             = "read_ahead_kb"
	fileCacheDestPathFmt             = "/var/lib/kubelet/pods/%s/volumes/kubernetes.io~empty-dir/gke-gcsfuse-cache/mount"
)

var readAheadKBMountFlagRegex = regexp.MustCompile(readAheadKBMountFlagRegexPattern)

// Mounter provides the Cloud Storage FUSE CSI implementation of mount.Interface
// for the linux platform.
type Mounter struct {
	mount.MounterForceUnmounter
	mux                 sync.Mutex
	fuseSocketDir       string
	sourceBindMountPath string
	targetBindMountPath string
}

// New returns a mount.MounterForceUnmounter for the current system.
// It provides options to override the default mounter behavior.
// mounterPath allows using an alternative to `/bin/mount` for mounting.
func New(mounterPath, fuseSocketDir string) (mount.Interface, error) {
	m, ok := mount.New(mounterPath).(mount.MounterForceUnmounter)
	if !ok {
		return nil, errors.New("failed to cast mounter to MounterForceUnmounter")
	}

	return &Mounter{
		m,
		sync.Mutex{},
		fuseSocketDir,
		"",
		"",
	}, nil
}

func applyTopLevelFsGroup(rootdir string, fsgroup int) error {
	info, err := os.Lstat(rootdir)
	if err != nil {
		return fmt.Errorf("failed to stat path %s, err: %v", rootdir, err)
	}

	klog.Infof("fileinfo for dir %s:%v", rootdir, info)
	err = os.Lchown(rootdir, -1, fsgroup)
	if err != nil {
		log.Printf("Warning: Lchown failed for %q: %v\n", rootdir, err)
	}

	// Verification
	postLchownInfo, statErr := os.Lstat(rootdir)
	if statErr != nil {
		log.Printf("Warning: Lstat failed after Lchown attempt for %q: %v\n", rootdir, statErr)
	} else {
		sysStat, ok := postLchownInfo.Sys().(*syscall.Stat_t)
		if !ok {
			log.Printf("Warning: Could not get syscall.Stat_t for %q after Lchown to verify GID.\n", rootdir)
		} else {
			currentGID := sysStat.Gid
			log.Printf("Info: Current GID for %q after Lchown attempt is %d (Target was %d)\n", rootdir, currentGID, fsgroup)
		}
	}

	mask := rwMask
	mask |= os.ModeSetgid
	mask |= execMask
	newMode := info.Mode() | mask
	err = os.Chmod(rootdir, newMode)
	if err != nil {
		log.Printf("Warning: Chmod failed for %q: %v\n", rootdir, err)
	}
	return nil
}

func (m *Mounter) Mount(source string, target string, fstype string, options []string) error {
	m.mux.Lock()
	defer m.mux.Unlock()

	csiMountOptions, sidecarMountOptions, sysfsBDI, bindFileCacheSourcePathCandidate, err := prepareMountOptions(options)
	if err != nil {
		return err
	}

	// Prepare sidecar mounter MountConfig
	mc := sidecarmounter.MountConfig{
		BucketName: source,
		Options:    sidecarMountOptions,
	}
	klog.Infof("final sidecar MountConfig to be sent from node driver %v", mc)
	msg, err := json.Marshal(mc)
	if err != nil {
		return fmt.Errorf("failed to marshal sidecar mounter MountConfig %v: %w", mc, err)
	}

	podID, volumeName, _ := util.ParsePodIDVolumeFromTargetpath(target)
	logPrefix := fmt.Sprintf("[Pod %v, Volume %v, Bucket %v]", podID, volumeName, source)

	if bindFileCacheSourcePathCandidate != "" {
		targetPath := fmt.Sprintf(fileCacheDestPathFmt, podID)
		err := os.MkdirAll(targetPath, 0750)
		if err != nil {
			return fmt.Errorf("failed to create directory '%s': %w", targetPath, err)
		}
		klog.Infof("file cache bind mount path %s create success, for source path %s", targetPath, bindFileCacheSourcePathCandidate)
		info, err := os.Stat(targetPath)
		if err != nil {
			klog.Errorf("stat check for %s failed with error %v", targetPath, err)
		} else {
			klog.Infof("stat check for %s : %v", targetPath, info)
		}

		info, err = os.Stat(bindFileCacheSourcePathCandidate)
		if err != nil {
			klog.Errorf("stat check for %s failed with error %v", bindFileCacheSourcePathCandidate, err)
		} else {
			klog.Infof("stat check for %s : %v", bindFileCacheSourcePathCandidate, info)
		}

		// save context required for unpublish
		m.sourceBindMountPath = bindFileCacheSourcePathCandidate
		m.targetBindMountPath = targetPath

		fstype := "ext4"
		options := []string{"bind"}
		err = m.MountSensitiveWithoutSystemdWithMountFlags(bindFileCacheSourcePathCandidate, targetPath, fstype, options, nil, []string{})
		if err != nil {
			klog.Warningf("mount failed with error %v", err)
		} else {
			klog.Infof("bind mount of %s to %s success", bindFileCacheSourcePathCandidate, targetPath)
		}

		// Changing group ownership
		err = applyTopLevelFsGroup(targetPath, targetFsGroup)
		if err != nil {
			return fmt.Errorf("applyTopLevelFsGroup failed with error %v", err)
		}
	}

	klog.V(4).Infof("%v opening the device /dev/fuse", logPrefix)
	fd, err := syscall.Open("/dev/fuse", syscall.O_RDWR, 0o644)
	if err != nil {
		return fmt.Errorf("failed to open the device /dev/fuse: %w", err)
	}
	csiMountOptions = append(csiMountOptions, fmt.Sprintf("fd=%v", fd))

	klog.V(4).Infof("%v mounting the fuse filesystem", logPrefix)
	err = m.MountSensitiveWithoutSystemdWithMountFlags(source, target, fstype, csiMountOptions, nil, []string{"--internal-only"})
	if err != nil {
		return fmt.Errorf("failed to mount the fuse filesystem: %w", err)
	}

	if len(sysfsBDI) != 0 {
		go func() {
			// updateSysfsConfig may hang until the file descriptor (fd) is either consumed or canceled.
			// It will succeed once dfuse finishes the mount process, or it will fail if dfuse fails
			// or the mount point is cleaned up due to mounting failures.
			if err := updateSysfsConfig(target, sysfsBDI); err != nil {
				klog.Errorf("%v failed to update kernel parameters: %v", logPrefix, err)
			}
		}()
	}

	listener, err := m.createSocket(target, logPrefix)
	if err != nil {
		// If mount failed at this step,
		// cleanup the mount point and allow the CSI driver NodePublishVolume to retry.
		klog.Warningf("%v failed to create socket, clean up the mount point", logPrefix)

		syscall.Close(fd)
		if m.UnmountWithForce(target, time.Second*5) != nil {
			klog.Warningf("%v failed to clean up the mount point", logPrefix)
		}

		return err
	}

	// Close the listener and fd after 1 hour timeout
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	go func() {
		<-ctx.Done()
		klog.V(4).Infof("%v closing the socket and fd", logPrefix)
		listener.Close()
		syscall.Close(fd)
	}()

	// Asynchronously waiting for the sidecar container to connect to the listener
	go startAcceptConn(listener, logPrefix, msg, fd, cancel)

	return nil
}

// updateSysfsConfig modifies the kernel page cache settings based on the read_ahead_kb provided in the mountOption,
// and verifies that the values are successfully updated after the operation completes.
func updateSysfsConfig(targetMountPath string, sysfsBDI map[string]int64) error {
	// Command will hang until mount completes.
	cmd := exec.Command("mountpoint", "-d", targetMountPath)
	output, err := cmd.CombinedOutput()
	if err != nil {
		klog.Errorf("Error executing mountpoint command on target path %s: %v", targetMountPath, err)
		var exitError *exec.ExitError
		if errors.As(err, &exitError) {
			klog.Errorf("Exit code: %d", exitError.ExitCode())
		}

		return err
	}

	targetDevice := strings.TrimSpace(string(output))
	klog.Infof("Output of mountpoint for target mount path %s: %s", targetMountPath, output)

	for key, value := range sysfsBDI {
		// Update the target value.
		sysfsBDIPath := filepath.Join("/sys/class/bdi/", targetDevice, key)
		file, err := os.OpenFile(sysfsBDIPath, os.O_WRONLY|os.O_TRUNC, 0o644)
		if err != nil {
			return fmt.Errorf("failed to open file %q: %w", sysfsBDIPath, err)
		}
		defer file.Close()

		_, err = file.WriteString(fmt.Sprintf("%d\n", value))
		if err != nil {
			return fmt.Errorf("failed to write to file %q: %w", "echo", err)
		}

		klog.Infof("Updated %s to %d", sysfsBDIPath, value)
	}

	return nil
}

func (m *Mounter) UnmountWithForce(target string, umountTimeout time.Duration) error {
	m.cleanupSocket(target)

	err := m.MounterForceUnmounter.UnmountWithForce(m.targetBindMountPath, umountTimeout)
	if err != nil {
		klog.Errorf("UnmountWithForce of path %s , err: %v", m.targetBindMountPath, err)
	}
	return m.MounterForceUnmounter.UnmountWithForce(target, umountTimeout)
}

func (m *Mounter) Unmount(target string) error {
	m.cleanupSocket(target)

	return m.MounterForceUnmounter.Unmount(target)
}

func (m *Mounter) createSocket(target string, logPrefix string) (net.Listener, error) {
	klog.V(4).Infof("%v passing the descriptor", logPrefix)

	// Prepare the temp emptyDir path
	emptyDirBasePath, err := util.PrepareEmptyDir(target, true)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare emptyDir path: %w", err)
	}

	// Create socket base path.
	// Need to create symbolic link of emptyDirBasePath to socketBasePath,
	// because the socket absolute path is longer than 104 characters,
	// which will cause "bind: invalid argument" errors.
	socketBasePath := util.GetSocketBasePath(target, m.fuseSocketDir)
	if err := os.Symlink(emptyDirBasePath, socketBasePath); err != nil && !os.IsExist(err) {
		return nil, fmt.Errorf("failed to create symbolic link to path %q: %w", socketBasePath, err)
	}

	klog.V(4).Infof("%v create a listener using the socket", logPrefix)
	l, err := net.Listen("unix", filepath.Join(socketBasePath, socketName))
	if err != nil {
		return nil, fmt.Errorf("failed to create a listener using the socket: %w", err)
	}

	// Change the socket ownership
	targetSocketPath := filepath.Join(emptyDirBasePath, socketName)
	if err = os.Chown(filepath.Dir(emptyDirBasePath), webhook.NobodyUID, webhook.NobodyGID); err != nil {
		return nil, fmt.Errorf("failed to change ownership on base of emptyDirBasePath: %w", err)
	}
	if err = os.Chown(emptyDirBasePath, webhook.NobodyUID, webhook.NobodyGID); err != nil {
		return nil, fmt.Errorf("failed to change ownership on emptyDirBasePath: %w", err)
	}
	if err = os.Chown(targetSocketPath, webhook.NobodyUID, webhook.NobodyGID); err != nil {
		return nil, fmt.Errorf("failed to change ownership on targetSocketPath: %w", err)
	}

	if _, err = os.Stat(targetSocketPath); err != nil {
		return nil, fmt.Errorf("failed to verify the targetSocketPath: %w", err)
	}

	return l, nil
}

func (m *Mounter) cleanupSocket(target string) {
	socketBasePath := util.GetSocketBasePath(target, m.fuseSocketDir)
	socketPath := filepath.Join(socketBasePath, socketName)
	if err := syscall.Unlink(socketPath); err != nil {
		if !os.IsNotExist(err) {
			klog.Errorf("failed to clean up socket %q: %v", socketPath, err)
		}
	}

	if err := os.Remove(socketBasePath); err != nil {
		if !os.IsNotExist(err) {
			klog.Errorf("failed to clean up socket base path %q: %v", socketBasePath, err)
		}
	}
}

func startAcceptConn(l net.Listener, logPrefix string, msg []byte, fd int, cancel context.CancelFunc) {
	defer cancel()

	klog.V(4).Infof("%v start to accept connections to the listener.", logPrefix)
	a, err := l.Accept()
	if err != nil {
		klog.Errorf("%v failed to accept connections to the listener: %v", logPrefix, err)

		return
	}
	defer a.Close()

	klog.V(4).Infof("%v start to send file descriptor and mount options", logPrefix)
	if err = util.SendMsg(a, fd, msg); err != nil {
		klog.Errorf("%v failed to send file descriptor and mount options: %v", logPrefix, err)
	}

	klog.V(4).Infof("%v exiting the listener goroutine.", logPrefix)
}

func prepareMountOptions(options []string) ([]string, []string, map[string]int64, string, error) {
	allowedOptions := map[string]bool{
		"exec":    true,
		"noexec":  true,
		"atime":   true,
		"noatime": true,
		"sync":    true,
		"async":   true,
		"dirsync": true,
	}

	csiMountOptions := []string{
		"nodev",
		"nosuid",
		"allow_other",
		"default_permissions",
		"rootmode=40000",
		fmt.Sprintf("user_id=%d", os.Getuid()),
		fmt.Sprintf("group_id=%d", os.Getgid()),
	}

	// users may pass options that should be used by Linux mount(8),
	// filter out these options and not pass to the sidecar mounter.
	validMountOptions := []string{"rw", "ro"}
	optionSet := sets.NewString(options...)
	for _, o := range validMountOptions {
		if optionSet.Has(o) {
			csiMountOptions = append(csiMountOptions, o)
			optionSet.Delete(o)
		}
	}

	var bindFileCacheSourcePathCandidate string
	sysfsBDI := make(map[string]int64)
	for _, o := range optionSet.List() {
		if strings.HasPrefix(o, "o=") {
			v := o[2:]
			if allowedOptions[v] {
				csiMountOptions = append(csiMountOptions, v)
			} else {
				klog.Warningf("got invalid mount option %q. Will discard invalid options and continue to mount.", v)
			}
			optionSet.Delete(o)
		}

		if readAheadKB := readAheadKBMountFlagRegex.FindStringSubmatch(o); len(readAheadKB) == 2 {
			// There is only one matching pattern in readAheadKBMountFlagRegex
			// If found, it will be at index 1
			readAheadKBInt, err := strconv.ParseInt(readAheadKB[1], 10, 0)
			if err != nil {
				return nil, nil, nil, "", fmt.Errorf("invalid read_ahead_kb mount flag %q: %w", o, err)
			}
			if readAheadKBInt < 0 {
				return nil, nil, nil, "", fmt.Errorf("invalid negative value for read_ahead_kb mount flag: %q", o)
			}
			sysfsBDI[readAheadKBMountFlag] = readAheadKBInt
			optionSet.Delete(o)
		}
		prefix := "internal-file-cache-bind-source-path="
		if strings.HasPrefix(o, prefix) {
			bindFileCacheSourcePathCandidate = strings.TrimPrefix(o, prefix)
			optionSet.Delete(o)
		}
	}

	return csiMountOptions, optionSet.List(), sysfsBDI, bindFileCacheSourcePathCandidate, nil
}
