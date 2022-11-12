/*
Copyright 2022 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"k8s.io/klog"
	sidecarmounter "sigs.k8s.io/gcp-cloud-storage-csi-driver/pkg/sidecar_mounter"
	"sigs.k8s.io/gcp-cloud-storage-csi-driver/pkg/util"
)

var (
	gcsfusePath    = flag.String("gcsfuse-path", "/gcsfuse", "gcsfuse path")
	volumeBasePath = flag.String("volume-base-path", "/tmp/.volumes", "volume base path")
	gracePeriod    = flag.Int("grace-period", 15, "grace period for gcsfuse termination")
	// This is set at compile time
	version = "unknown"
)

func main() {
	klog.InitFlags(nil)
	flag.Parse()

	klog.Infof("Running Google Cloud Storage CSI driver sidecar mounter version %v", version)
	socketPathPattern := *volumeBasePath + "/*/socket"
	socketPathes, err := filepath.Glob(socketPathPattern)
	if err != nil {
		klog.Fatalf("failed to look up socket pathes: %v", err)
	}

	mounter := sidecarmounter.New(*gcsfusePath)
	var wg sync.WaitGroup

	for _, sp := range socketPathes {
		// sleep 1.5 seconds before launch the next gcsfuse to avoid
		// 1. logs from different gcsfuse mixed together.
		// 2. memory usage peak.
		time.Sleep(1500 * time.Millisecond)

		dir := filepath.Dir(sp)
		volumeName := filepath.Base(dir)
		mc := sidecarmounter.MountConfig{
			VolumeName: volumeName,
			TempDir:    filepath.Join(dir, "temp-dir"),
			Stdout:     sidecarmounter.NewStdoutWriter(os.Stdout, volumeName),
			Stderr:     sidecarmounter.NewStderrWriter(os.Stdout, volumeName, filepath.Join(dir, "error")),
		}

		klog.Infof("connecting to socket %q", sp)
		c, err := net.Dial("unix", sp)
		if err != nil {
			errMsg := fmt.Sprintf("failed to connect to the socket %q: %v\n", sp, err)
			mc.Stderr.Write([]byte(errMsg))
			continue
		}
		defer c.Close()

		fd, msg, err := util.RecvMsg(c)
		if err != nil {
			errMsg := fmt.Sprintf("failed to receive mount options from the socket %q: %v\n", sp, err)
			mc.Stderr.Write([]byte(errMsg))
			continue
		}
		mc.FileDescriptor = fd

		if err := json.Unmarshal(msg, &mc); err != nil {
			errMsg := fmt.Sprintf("failed to unmarchal the mount config: %v\n", err)
			mc.Stderr.Write([]byte(errMsg))
			continue
		}

		if mc.BucketName == "" {
			errMsg := fmt.Sprintln("failed to fetch bucket name from CSI driver.")
			mc.Stderr.Write([]byte(errMsg))
			continue
		}

		wg.Add(1)
		go func(mc *sidecarmounter.MountConfig) {
			defer wg.Done()
			cmd, err := mounter.Mount(mc)
			if err != nil {
				errMsg := fmt.Sprintf("failed to mount bucket %q for volume %q: %v\n", mc.BucketName, mc.VolumeName, err)
				mc.Stderr.Write([]byte(errMsg))
				return
			}

			err = cmd.Run()
			if err != nil {
				errMsg := fmt.Sprintf("gcsfuse exited with error: %v\n", err)
				mc.Stderr.Write([]byte(errMsg))
				if mc.DeviceFile != nil {
					if err = mc.DeviceFile.Close(); err != nil {
						klog.Errorf("failed to close the device file for volume %q: %v", mc.VolumeName, err)
					}
				}
			} else {
				klog.Infof("[%v] gcsfuse exited normally.", mc.VolumeName)
			}
		}(&mc)
	}

	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	klog.Info("waiting for termination signals...")
	sig := <-c // blocking the process
	klog.Infof("received signal: %v, sleep %v seconds before terminating gcsfuse processes.", sig, *gracePeriod)
	time.Sleep(time.Duration(*gracePeriod) * time.Second)

	for _, cmd := range mounter.GetCmds() {
		klog.V(4).Infof("killing gcsfue process: %v", cmd)
		err := cmd.Process.Kill()
		if err != nil {
			klog.Errorf("failed to kill process %v with error: %v", cmd, err)
		}
	}

	wg.Wait()
	klog.Info("existing sidecar mounter...")
}
