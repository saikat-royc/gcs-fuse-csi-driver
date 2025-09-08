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
	"fmt"
	"os"
	"strconv"
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
	corev1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	mount "k8s.io/mount-utils"
)

const (
	UmountTimeout = time.Second * 5

	FuseMountType                               = "fuse"
	MiB                                   int64 = 1024 * 1024
	internalFileCacheBindSourcePathPrefix       = "internal-file-cache-bind-source-path="
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
	volumeStateStore      *util.VolumeStateStore
}

func newNodeServer(driver *GCSDriver, mounter mount.Interface) csi.NodeServer {
	return &nodeServer{
		driver:                driver,
		storageServiceManager: driver.config.StorageServiceManager,
		mounter:               mounter,
		volumeLocks:           util.NewVolumeLocks(),
		k8sClients:            driver.config.K8sClients,
		limiter:               *rate.NewLimiter(rate.Every(time.Second), 10),
		volumeStateStore:      util.NewVolumeStateStore(),
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

	publishContext := req.GetPublishContext()
	if publishContext != nil {
		// 1. Parse comma-separated Mount Options from StorageClass (if present)
		if encodedSCOptions, ok := publishContext[publishContextMountOptionsKey]; ok && encodedSCOptions != "" {
			klog.V(4).Infof("Found mount options '%s' in PublishContext under key %q for target path %q", encodedSCOptions, publishContextMountOptionsKey, targetPath)
			scMountOptions := strings.Split(encodedSCOptions, ",")
			filteredSCOptions := []string{}
			for _, opt := range scMountOptions {
				trimmedOpt := strings.TrimSpace(opt)
				if trimmedOpt != "" {
					filteredSCOptions = append(filteredSCOptions, trimmedOpt)
				}
			}
			if len(filteredSCOptions) > 0 {
				klog.V(4).Infof("Appending StorageClass mount options from PublishContext to fuseMountOptions for target path %q: %v", targetPath, filteredSCOptions)
				fuseMountOptions = append(fuseMountOptions, filteredSCOptions...)
			}
		} else {
			klog.V(5).Infof("No mount options found in PublishContext under key %q (or value is empty) for target path %q", publishContextMountOptionsKey, targetPath)
		}

		// 2. Parse Recommended Cache Sizes
		// Metadata Cache Size
		if metadataBytesStr, ok := publishContext[publishContextMetadataCacheBytesKey]; ok && metadataBytesStr != "" {
			metadataBytes, err := strconv.ParseInt(metadataBytesStr, 10, 64)
			if err != nil {
				klog.Warningf("Failed to parse %s value '%s' from PublishContext for target path %q: %v. Skipping metadata cache size option.",
					publishContextMetadataCacheBytesKey, metadataBytesStr, targetPath, err)
			} else if metadataBytes > 0 {
				metadataCacheMib := metadataBytes / MiB
				mountOpt := fmt.Sprintf("metadata-cache:stat-cache-max-size-mb:%d", metadataCacheMib)
				fuseMountOptions = append(fuseMountOptions, mountOpt)
				klog.V(4).Infof("Appending metadata cache option from PublishContext for target path %q: %s", targetPath, mountOpt)
			} else {
				klog.V(4).Infof("Metadata cache size %d bytes (<=0) specified in PublishContext for target path %q. Not adding size option.", metadataBytes, targetPath)
			}
		} else {
			klog.V(5).Infof("No %s key found in PublishContext for target path %q.", publishContextMetadataCacheBytesKey, targetPath)
		}

		// File Cache Size
		var fileCacheMedium string
		if fileBytesStr, ok := publishContext[publishContextFileCacheBytesKey]; ok && fileBytesStr != "" {
			fileBytes, err := strconv.ParseInt(fileBytesStr, 10, 64)
			if err != nil {
				klog.Warningf("Failed to parse %s value '%s' from PublishContext for target path %q: %v. Skipping file cache size option.",
					publishContextFileCacheBytesKey, fileBytesStr, targetPath, err)
			} else if fileBytes > 0 {
				// Only add file cache size if the medium is also specified and not 'none'/'empty'
				fileCacheMedium = publishContext[publishContextFileCacheMediumKey]
				if fileCacheMedium != "" {
					fileCacheMib := fileBytes / MiB
					mountOpt := fmt.Sprintf("file-cache:max-size-mb:%d", fileCacheMib)
					fuseMountOptions = append(fuseMountOptions, mountOpt)
					mountOpt = fmt.Sprintf("file-cache-medium=%s", fileCacheMedium)
					fuseMountOptions = append(fuseMountOptions, mountOpt)
					klog.V(4).Infof("Appending file cache option from PublishContext for target path %q: %s (Medium: %s)", targetPath, mountOpt, fileCacheMedium)
				} else {
					klog.V(4).Infof("File cache medium is '%s' in PublishContext for target path %q. Not adding file cache size option even though bytes (%d) > 0.", fileCacheMedium, targetPath, fileBytes)
					cleanedFuseMountOptions := []string{}
					for _, opt := range fuseMountOptions {
						if !strings.HasPrefix(opt, "file-cache:") {
							cleanedFuseMountOptions = append(cleanedFuseMountOptions, opt)
						} else {
							klog.V(5).Infof("Removing file cache option '%s' from fuseMountOptions for target path %q due to fileBytes <= 0.", opt, targetPath)
						}
					}
					fuseMountOptions = cleanedFuseMountOptions
				}
			} else {
				klog.V(4).Infof("File cache size %d bytes (<=0) specified in PublishContext for target path %q. Not adding size option.", fileBytes, targetPath)
			}
		} else {
			klog.V(5).Infof("No %s key found in PublishContext for target path %q.", publishContextFileCacheBytesKey, targetPath)
		}
	}

	if publishContext != nil {
		if cacheStagingPath, ok := publishContext[publishContextCachePVStagingPathKey]; ok && cacheStagingPath != "" {
			klog.V(4).Infof("NodePublishVolume: Cache PV staging path '%s' found in PublishContext for target path %q. Will attempt bind mount.", cacheStagingPath, targetPath)
			// Add the special internal option to signal a bind mount and pass the source path
			fuseMountOptions = append(fuseMountOptions, fmt.Sprintf("%s%s", internalFileCacheBindSourcePathPrefix, cacheStagingPath))
		} else {
			klog.V(5).Infof("NodePublishVolume: Key '%s' not found or empty in PublishContext for target path %q. Proceeding with direct GCS FUSE mount.", publishContextCachePVStagingPathKey, targetPath)
		}
	}
	klog.Infof("fuseMountOptions %v", fuseMountOptions)
	// Log the combined options before further modifications (like token server)

	// --- End Publish Context Mount Options Logic ---

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
		vs, ok := s.volumeStateStore.Load(targetPath)
		if !ok {
			s.volumeStateStore.Store(targetPath, &util.VolumeState{})
			vs, _ = s.volumeStateStore.Load(targetPath)
		}
		// volumeState is safe to access for remaining of function since volumeLock prevents
		// Node Publish/Unpublish Volume calls from running more than once at a time per volume.
		if !vs.BucketAccessCheckPassed {
			storageService, err := s.prepareStorageService(ctx, vc)
			if err != nil {
				return nil, status.Errorf(codes.Unauthenticated, "failed to prepare storage service: %v", err)
			}
			defer storageService.Close()

			if exist, err := storageService.CheckBucketExists(ctx, &storage.ServiceBucket{Name: bucketName}); !exist {
				return nil, status.Errorf(storage.ParseErrCode(err), "failed to get GCS bucket %q: %v", bucketName, err)
			}

			vs.BucketAccessCheckPassed = true
		}
	}

	// Check if the sidecar container was injected into the Pod
	pod, err := s.k8sClients.GetPod(vc[VolumeContextKeyPodNamespace], vc[VolumeContextKeyPodName])
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "failed to get pod: %v", err)
	}

	if s.shouldStartTokenServer(pod) && pod.Spec.HostNetwork {
		identityProvider := s.driver.config.TokenManager.GetIdentityProvider()
		fuseMountOptions = joinMountOptions(fuseMountOptions, []string{"token-server-identity-provider=" + identityProvider})
	}

	node, err := s.k8sClients.GetNode(s.driver.config.NodeID)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "failed to get node: %v", err)
	}

	val, ok := node.Labels[clientset.GkeMetaDataServerKey]
	// If Workload Identity is not enabled, the key should be missing; the check for "val == false" is just for extra caution
	isWorkloadIdentityDisabled := val != "true" || !ok
	if isWorkloadIdentityDisabled && !pod.Spec.HostNetwork {
		return nil, status.Errorf(codes.FailedPrecondition, "Workload Identity Federation is not enabled on node. Please make sure this is enabled on both cluster and node pool level (https://cloud.google.com/kubernetes-engine/docs/how-to/workload-identity)")
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

	// Register metrics collector.
	// It is idempotent to register the same collector in node republish calls.
	if s.driver.config.MetricsManager != nil && !disableMetricsCollection {
		klog.V(6).Infof("NodePublishVolume enabling metrics collector for target path %q", targetPath)
		s.driver.config.MetricsManager.RegisterMetricsCollector(targetPath, pod.Namespace, pod.Name, bucketName)
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

	s.volumeStateStore.Delete(targetPath)

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

func (s *nodeServer) shouldStartTokenServer(pod *corev1.Pod) bool {
	tokenVolumeInjected := false
	for _, vol := range pod.Spec.Volumes {
		if vol.Name == webhook.SidecarContainerSATokenVolumeName {
			klog.Infof("Service Account Token Injection feature is turned on from webhook.")

			tokenVolumeInjected = true

			break
		}
	}
	var sidecarVersionSupported bool

	for _, container := range pod.Spec.InitContainers {
		if container.Name == webhook.GcsFuseSidecarName {
			sidecarVersionSupported = isSidecarVersionSupportedForTokenServer(container.Image)

			break
		}
	}

	return tokenVolumeInjected && sidecarVersionSupported
}
