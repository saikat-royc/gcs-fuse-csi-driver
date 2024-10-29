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

package driver

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	csi "github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/googlecloudplatform/gcs-fuse-csi-driver/pkg/cloud_provider/clientset"
	"github.com/googlecloudplatform/gcs-fuse-csi-driver/pkg/cloud_provider/storage"
	"github.com/googlecloudplatform/gcs-fuse-csi-driver/pkg/util"
	"github.com/googlecloudplatform/gcs-fuse-csi-driver/pkg/webhook"
	"golang.org/x/net/context"
	"golang.org/x/time/rate"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"
	mount "k8s.io/mount-utils"
)

const (
	UmountTimeout = time.Second * 5

	FuseMountType = "fuse"
)

// POC constants
const (
	saName = "test-ksa-ns1"
	saNs   = "ns1"
)

// nodeServer handles mounting and unmounting of GCS FUSE volumes on a node.
type nodeServer struct {
	csi.UnimplementedNodeServer
	driver                *GCSDriver
	storageServiceManager storage.ServiceManager
	mounter               mount.Interface
	volumeLocks           *util.VolumeLocks
	k8sClients            clientset.Interface
	limiter               rate.Limiter
	volumeStateStore      map[string]*volumeState
}

type volumeState struct {
	bucketAccessCheckPassed bool
	// handle token requests from the target pod
	tokenServerStarted bool
}

func newNodeServer(driver *GCSDriver, mounter mount.Interface) csi.NodeServer {
	return &nodeServer{
		driver:                driver,
		storageServiceManager: driver.config.StorageServiceManager,
		mounter:               mounter,
		volumeLocks:           util.NewVolumeLocks(),
		k8sClients:            driver.config.K8sClients,
		limiter:               *rate.NewLimiter(rate.Every(time.Second), 10),
		volumeStateStore:      make(map[string]*volumeState),
	}
}

func (s *nodeServer) NodeGetInfo(_ context.Context, _ *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	return &csi.NodeGetInfoResponse{
		NodeId: s.driver.config.NodeID,
	}, nil
}

func (s *nodeServer) NodeGetCapabilities(_ context.Context, _ *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	return &csi.NodeGetCapabilitiesResponse{
		Capabilities: s.driver.nscap,
	}, nil
}

func isSymLink(path string) (bool, error) {
	fileInfo, err := os.Lstat(path)
	if err != nil {
		return false, err
	}

	if fileInfo.Mode()&os.ModeSymlink != 0 {
		klog.Infof("path %s is a symlink", path)
		return true, nil
	}

	klog.Infof("path %s is a not a symlink", path)
	return false, nil
}

func (s *nodeServer) startTokenServer(ctx context.Context, targetPath, socketDir string, result chan<- error) {
	emptyDirBasePath, err := util.PrepareEmptyDir(targetPath, true)
	if err != nil {
		klog.Errorf("failed to prepare token socket: %v", err)
		result <- err
		return
	}

	socketBasePath := util.GetSocketBasePath(targetPath, socketDir)
	if err := os.Symlink(emptyDirBasePath, socketBasePath); err != nil && !os.IsExist(err) {
		klog.Errorf("failed to create symbolic link to path %q: %v", socketBasePath, err)
		result <- err
		return
	}

	klog.Infof("emptyDirBasePath %s, socketBasePath %s", emptyDirBasePath, socketBasePath)
	if _, err := isSymLink(socketBasePath); err != nil {
		result <- err
		return
	}

	// Create a unix domain socket and listen for incoming connections.
	socketPath := filepath.Join(socketBasePath, "token.sock")
	socket, err := net.Listen("unix", socketPath)
	if err != nil {
		klog.Errorf("failed to create socket %q: %v", socketPath, err)
		result <- err
		return
	}
	klog.Infof("created a listener using the socket path %s", socketPath)
	// Change the socket ownership
	if err = os.Chown(filepath.Dir(emptyDirBasePath), webhook.NobodyUID, webhook.NobodyGID); err != nil {
		result <- fmt.Errorf("failed to change ownership on base of emptyDirBasePath: %w", err)
		return
	}
	if err = os.Chown(emptyDirBasePath, webhook.NobodyUID, webhook.NobodyGID); err != nil {
		result <- fmt.Errorf("failed to change ownership on emptyDirBasePath: %w", err)
		return
	}
	if err = os.Chown(socketPath, webhook.NobodyUID, webhook.NobodyGID); err != nil {
		result <- fmt.Errorf("failed to change ownership on targetSocketPath: %w", err)
		return
	}

	if _, err = os.Stat(socketPath); err != nil {
		result <- fmt.Errorf("failed to verify the targetSocketPath: %w", err)
	}
	klog.Infof("changed ownership of socket path %s", socketPath)

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		_, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		klog.Infof("got request on socket path %s", socketPath)
		queryParams := r.URL.Query()
		for key, values := range queryParams {
			klog.Infof("Query parameter %q: %v", key, values)
		}

		ts := s.driver.config.TokenManager.GetTokenSourceFromK8sServiceAccount(saNs, saName, "")
		token, err := ts.Token()
		if err != nil {
			klog.Errorf("failed to fetch token: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		klog.Infof("for SA %s/%s got token %s", saNs, saName, token.AccessToken)
		// w.WriteHeader(http.StatusOK)     // Set the status code to 200 OK
		// fmt.Fprint(w, token.AccessToken) // Write "ack" to the response body

		// Marshal the oauth2.Token object to JSON
		jsonToken, err := json.Marshal(token)
		if err != nil {
			klog.Errorf("failed to marshal token to JSON: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, string(jsonToken))
	})

	server := http.Server{
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	result <- nil
	if err := server.Serve(socket); err != nil {
		klog.Errorf("failed to start the token server for %q: %v", socketPath, err)
	}
}

func (s *nodeServer) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	// Rate limit NodePublishVolume calls to avoid kube API throttling.
	if err := s.limiter.Wait(ctx); err != nil {
		return nil, status.Errorf(codes.Aborted, "NodePublishVolume request is aborted due to rate limit: %v", err)
	}

	// Validate arguments
	targetPath, bucketName, fuseMountOptions, skipBucketAccessCheck, disableMetricsCollection, err := parseRequestArguments(req)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	klog.V(6).Infof("NodePublishVolume on volume %q has skipBucketAccessCheck %t", bucketName, skipBucketAccessCheck)

	if err := s.driver.validateVolumeCapabilities([]*csi.VolumeCapability{req.GetVolumeCapability()}); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	// Acquire a lock on the target path instead of volumeID, since we do not want to serialize multiple node publish calls on the same volume.
	if acquired := s.volumeLocks.TryAcquire(targetPath); !acquired {
		return nil, status.Errorf(codes.Aborted, util.VolumeOperationAlreadyExistsFmt, targetPath)
	}
	defer s.volumeLocks.Release(targetPath)

	vc := req.GetVolumeContext()

	// Check if the given Service Account has the access to the GCS bucket, and the bucket exists.
	// skip check if it has ever succeeded
	if bucketName != "_" && !skipBucketAccessCheck {
		// Use target path as an volume identifier because it corresponds to Pods and volumes.
		// Pods may belong to different namespaces and would need their own access check.
		vs, ok := s.volumeStateStore[targetPath]
		if !ok {
			s.volumeStateStore[targetPath] = &volumeState{}
			vs = s.volumeStateStore[targetPath]
		}

		if !vs.bucketAccessCheckPassed {
			storageService, err := s.prepareStorageService(ctx, vc)
			if err != nil {
				return nil, status.Errorf(codes.Unauthenticated, "failed to prepare storage service: %v", err)
			}
			defer storageService.Close()

			if exist, err := storageService.CheckBucketExists(ctx, &storage.ServiceBucket{Name: bucketName}); !exist {
				return nil, status.Errorf(storage.ParseErrCode(err), "failed to get GCS bucket %q: %v", bucketName, err)
			}

			vs.bucketAccessCheckPassed = true
		}
	}

	// Check if the sidecar container was injected into the Pod
	pod, err := s.k8sClients.GetPod(vc[VolumeContextKeyPodNamespace], vc[VolumeContextKeyPodName])
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "failed to get pod: %v", err)
	}

	// Since the webhook mutating ordering is not definitive,
	// the sidecar position is not checked in the ValidatePodHasSidecarContainerInjected func.
	shouldInjectedByWebhook := strings.ToLower(pod.Annotations[webhook.GcsFuseVolumeEnableAnnotation]) == util.TrueStr
	sidecarInjected, isInitContainer := webhook.ValidatePodHasSidecarContainerInjected(pod)
	if !sidecarInjected {
		if shouldInjectedByWebhook {
			return nil, status.Error(codes.Internal, "the webhook failed to inject the sidecar container into the Pod spec")
		}

		return nil, status.Error(codes.FailedPrecondition, "failed to find the sidecar container in Pod spec")
	}

	// Register metrics collecter.
	// It is idempotent to register the same collector in node republish calls.
	if s.driver.config.MetricsManager != nil && !disableMetricsCollection {
		klog.V(6).Infof("NodePublishVolume enabling metrics collector for target path %q", targetPath)
		s.driver.config.MetricsManager.RegisterMetricsCollector(targetPath, pod.Namespace, pod.Name, bucketName)
	}

	// prepare a server to listen for token requests
	vs, ok := s.volumeStateStore[targetPath]
	if !ok {
		s.volumeStateStore[targetPath] = &volumeState{}
		vs = s.volumeStateStore[targetPath]
	}

	if !vs.tokenServerStarted {
		errChan := make(chan error)
		go s.startTokenServer(ctx, targetPath, "/sockets", errChan)
		err := <-errChan
		if err != nil {
			klog.Errorf("Failed to start token server: %v", err)
			return nil, status.Error(codes.Internal, err.Error())
		}
		klog.Infof("Started token server")
		vs.tokenServerStarted = true
	}

	// Check if the sidecar container is still required,
	// if not, put an exit file to the emptyDir path to
	// notify the sidecar container to exit.
	if !isInitContainer {
		if err := putExitFile(pod, targetPath); err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
	}

	// Check if there is any error from the gcsfuse
	code, err := checkGcsFuseErr(isInitContainer, pod, targetPath)
	if code != codes.OK {
		if code == codes.Canceled {
			klog.V(4).Infof("NodePublishVolume on volume %q to target path %q is not needed because the gcsfuse has terminated.", bucketName, targetPath)

			return &csi.NodePublishVolumeResponse{}, nil
		}

		return nil, status.Error(code, err.Error())
	}

	// Check if there is any error from the sidecar container
	code, err = checkSidecarContainerErr(isInitContainer, pod)
	if code != codes.OK {
		return nil, status.Error(code, err.Error())
	}

	// TODO: Check if the socket listener timed out

	// Check if the target path is already mounted
	mounted, err := s.isDirMounted(targetPath)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to check if path %q is already mounted: %v", targetPath, err)
	}

	if mounted {
		klog.V(4).Infof("NodePublishVolume succeeded on volume %q to target path %q, mount already exists.", bucketName, targetPath)

		return &csi.NodePublishVolumeResponse{}, nil
	}

	klog.V(4).Infof("NodePublishVolume attempting mkdir for path %q", targetPath)
	if err := os.MkdirAll(targetPath, 0o750); err != nil {
		return nil, status.Errorf(codes.Internal, "mkdir failed for path %q: %v", targetPath, err)
	}

	// Start to mount
	if err = s.mounter.Mount(bucketName, targetPath, FuseMountType, fuseMountOptions); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to mount volume %q to target path %q: %v", bucketName, targetPath, err)
	}

	klog.V(4).Infof("NodePublishVolume succeeded on volume %q to target path %q", bucketName, targetPath)

	return &csi.NodePublishVolumeResponse{}, nil
}

func (s *nodeServer) NodeUnpublishVolume(_ context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	// Validate arguments
	targetPath := req.GetTargetPath()
	if len(targetPath) == 0 {
		return nil, status.Error(codes.InvalidArgument, "NodeUnpublishVolume target path must be provided")
	}

	// Acquire a lock on the target path instead of volumeID, since we do not want to serialize multiple node unpublish calls on the same volume.
	if acquired := s.volumeLocks.TryAcquire(targetPath); !acquired {
		return nil, status.Errorf(codes.Aborted, util.VolumeOperationAlreadyExistsFmt, targetPath)
	}
	defer s.volumeLocks.Release(targetPath)

	// Unregister metrics collecter.
	// It is idempotent to unregister the same collector.
	if s.driver.config.MetricsManager != nil {
		s.driver.config.MetricsManager.UnregisterMetricsCollector(targetPath)
	}

	delete(s.volumeStateStore, targetPath)

	// Check if the target path is already mounted
	if mounted, err := s.isDirMounted(targetPath); mounted || err != nil {
		if err != nil {
			klog.Errorf("failed to check if path %q is already mounted: %v", targetPath, err)
		}
		// Force unmount the target path
		// Try to do force unmount firstly because if the file descriptor was not closed,
		// mount.CleanupMountPoint() call will hang.
		forceUnmounter, ok := s.mounter.(mount.MounterForceUnmounter)
		if ok {
			if err = forceUnmounter.UnmountWithForce(targetPath, UmountTimeout); err != nil {
				return nil, status.Errorf(codes.Internal, "failed to force unmount target path %q: %v", targetPath, err)
			}
		} else {
			klog.Warningf("failed to cast the mounter to a forceUnmounter, proceed with the default mounter Unmount")
			if err = s.mounter.Unmount(targetPath); err != nil {
				return nil, status.Errorf(codes.Internal, "failed to unmount target path %q: %v", targetPath, err)
			}
		}
	}

	// Cleanup the mount point
	if err := mount.CleanupMountPoint(targetPath, s.mounter, false /* bind mount */); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to cleanup the mount point %q: %v", targetPath, err)
	}

	klog.V(4).Infof("NodeUnpublishVolume succeeded on target path %q", targetPath)

	return &csi.NodeUnpublishVolumeResponse{}, nil
}

// isDirMounted checks if the path is already a mount point.
func (s *nodeServer) isDirMounted(targetPath string) (bool, error) {
	mps, err := s.mounter.List()
	if err != nil {
		return false, err
	}
	for _, m := range mps {
		if m.Path == targetPath {
			return true, nil
		}
	}

	return false, nil
}

// prepareStorageService prepares the GCS Storage Service using the Kubernetes Service Account from VolumeContext.
func (s *nodeServer) prepareStorageService(ctx context.Context, vc map[string]string) (storage.Service, error) {
	ts := s.driver.config.TokenManager.GetTokenSourceFromK8sServiceAccount(vc[VolumeContextKeyPodNamespace], vc[VolumeContextKeyServiceAccountName], vc[VolumeContextKeyServiceAccountToken])
	storageService, err := s.storageServiceManager.SetupService(ctx, ts)
	if err != nil {
		return nil, fmt.Errorf("storage service manager failed to setup service: %w", err)
	}

	return storageService, nil
}
