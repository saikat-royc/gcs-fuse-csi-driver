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
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	csi "github.com/container-storage-interface/spec/lib/go/csi"
	v1 "github.com/googlecloudplatform/gcs-fuse-csi-driver/pkg/apis/datalayer.gke.io/v1"
	"github.com/googlecloudplatform/gcs-fuse-csi-driver/pkg/cloud_provider/storage"
	"github.com/googlecloudplatform/gcs-fuse-csi-driver/pkg/util"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/client-go/informers" // Added for informers

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	corelisters "k8s.io/client-go/listers/core/v1" // Added for NodeLister
	storagelisters "k8s.io/client-go/listers/storage/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/tools/reference"
	"k8s.io/klog/v2"
)

const (
	MinimumVolumeSizeInBytes int64 = 1 * util.Mb
	controllerResync               = 30 * time.Minute

	volumeContextOrigPVName       = "origPVName"
	volumeContextStorageClassName = "storageClassName"

	annotationNumObjects     = "gke-gcsfuse/num-objects"
	annotationTotalSizeBytes = "gke-gcsfuse/total-size-bytes"

	controllerNamespaceEnvVar = "CONTROLLER_POD_NAMESPACE"
	DriverName                = "gcsfusecsi"

	// GKEAppliedNodeLabelsAnnotationKey is the annotation key that stores a comma-separated list of node labels.
	GKEAppliedNodeLabelsAnnotationKey = "node.gke.io/last-applied-node-labels"
	// EphemeralStorageLocalSSDLabelKey is the specific label key we are looking for within the applied labels.
	EphemeralStorageLocalSSDLabelKey = "cloud.google.com/gke-ephemeral-storage-local-ssd"
	// ExpectedEphemeralStorageLocalSSDLabelValue is the expected value for the label if local SSD is present.
	ExpectedEphemeralStorageLocalSSDLabelValue = "true"

	// Standard medium names
	mediumRAM   = "ram"
	mediumLSSD  = "lssd"
	mediumPDSSD = "pd-ssd" // Example name for persistent disk fallback

	// Node Types (adjust strings if different values are used)
	nodeTypeTPU            = "tpu"
	nodeTypeGPU            = "gpu"
	nodeTypeGeneralPurpose = "general_purpose"

	// Default priority key if nodeType specific one not found
	defaultPriorityKey     = "default"
	metadataBytesPerObject = 1500

	nvidiaGpuResourceName           = corev1.ResourceName("nvidia.com/gpu")
	googleTpuResourceName           = corev1.ResourceName("google.com/tpu")
	pdMaxCapacityBytes        int64 = 64 * 1024 * 1024 * 1024 * 1024
	pd90PercentThresholdBytes int64 = (pdMaxCapacityBytes / 10) * 9

	// storage class param keys
	fusefilecacheMediumPriorityKey               = "fusefilecacheMediumPriority"
	fuseNodeMemoryAllocatableFactorKey           = "fuseNodeMemoryAllocatableFactor"
	fuseNodeEphemeralStorageAllocatableFactorKey = "fuseNodeEphemeralStorageAllocatableFactor"
	publishContextMountOptionsKey                = "mountOptions"
	publishContextMetadataCacheBytesKey          = "metadata_cache_bytes"
	publishContextFileCacheBytesKey              = "file_cache_bytes"
	publishContextFileCacheMediumKey             = "file_cache_medium"

	pdCacheMediumTypeSSD          = "pd-ssd"  // Example identifier for PD-SSD medium
	cacheHelperPodImage           = "busybox" // Simple image for the helper pod
	cachePVCPollInterval          = 5 * time.Second
	cachePVCPollTimeout           = 2 * time.Minute
	cachePodPollInterval          = 5 * time.Second
	cachePodPollTimeout           = 5 * time.Minute
	publishContextCachePVCNameKey = "csi.storage.k8s.io/gcsfuse-cache-pvc-name"
	publishContextCachePVNameKey  = "csi.storage.k8s.io/gcsfuse-cache-pv-name"
	publishContextCachePodNameKey = "csi.storage.k8s.io/gcsfuse-cache-pod-name" // Storing pod name might be more useful than UID for debugging
	publishContextCachePodUIDKey  = "csi.storage.k8s.io/gcsfuse-cache-pod-uid"

	cacheResourceLabelKeyManagedBy      = "app.kubernetes.io/managed-by"
	cacheResourceLabelValueDriver       = "gcsfuse-csi-driver" // Or your driver's name
	cacheResourceLabelKeyTargetPV       = "gcsfuse.csi.storage.gke.io/target-pv-name"
	cacheResourceLabelKeyPurpose        = "gcsfuse.csi.storage.gke.io/purpose"
	cacheResourceLabelValueFileCache    = "file-cache-device"
	publishContextCachePVStagingPathKey = "csi.storage.k8s.io/gcsfuse-cache-pv-staging-path"
)

// CreateVolume parameters.
const (
	// Keys for PV and PVC parameters as reported by external-provisioner.
	ParameterKeyPVCName      = "csi.storage.k8s.io/pvc/name"
	ParameterKeyPVCNamespace = "csi.storage.k8s.io/pvc/namespace"
	ParameterKeyPVName       = "csi.storage.k8s.io/pv/name"

	// User provided labels.
	ParameterKeyLabels = "labels"

	// Keys for tags to attach to the provisioned disk.
	tagKeyCreatedForClaimNamespace = "kubernetes_io_created-for_pvc_namespace"
	tagKeyCreatedForClaimName      = "kubernetes_io_created-for_pvc_name"
	tagKeyCreatedForVolumeName     = "kubernetes_io_created-for_pv_name"
	tagKeyCreatedBy                = "storage_gke_io_created-by"
	nodePublishPathFmt             = "/var/lib/kubelet/pods/%s/volumes/kubernetes.io~csi/%s/mount"
	// nodeStagingPathFmt             = "/var/lib/kubelet/pods/%s/volumes/kubernetes.io~csi/%s/mount"
)

type GCSFuseRecommendations struct {
	MetadataCacheBytes int64
	FileCacheBytes     int64
	FilecacheMedium    string
	// Signal
	SignalNumObjects                              int64
	SignalTotalDataSizeBytes                      int64
	SignalNodeType                                string
	SignalNodeAllocatableBytesRam                 int64
	SignalNodeAllocatableBytesEpehemralStorage    int64
	SignalMaxFuseMemoryAllocatableBytes           int64
	SignalMaxFuseEphemeralStorageAllocatableBytes int64
}

type PVDetails struct {
	NumObjects     int64
	TotalSizeBytes int64
}

type NodeAllocatables struct {
	MemoryBytes           int64
	EphemeralStorageBytes int64
}

// controllerServer handles volume provisioning.
type controllerServer struct {
	csi.UnimplementedControllerServer
	driver                *GCSDriver
	storageServiceManager storage.ServiceManager
	volumeLocks           *util.VolumeLocks

	// informers
	nodeLister                 corelisters.NodeLister
	nodeInformerSynced         cache.InformerSynced
	pvLister                   corelisters.PersistentVolumeLister
	pvInformerSynced           cache.InformerSynced
	storageClassLister         storagelisters.StorageClassLister
	storageClassInformerSynced cache.InformerSynced

	recorder            record.EventRecorder
	controllerNamespace string
	k8sClient           kubernetes.Interface
}

func getKubeClientInCluster() (kubernetes.Interface, error) {
	// rest.InClusterConfig() creates a config object based on the Pod's environment variables
	// and mounted service account token.
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to get in-cluster config: %w", err)
	}

	// kubernetes.NewForConfig creates a new clientset for the given config.
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create clientset from config: %w", err)
	}

	fmt.Println("Successfully initialized in-cluster Kubernetes clientset")
	return clientset, nil
}

func newControllerServer(driver *GCSDriver, storageServiceManager storage.ServiceManager) (csi.ControllerServer, error) {
	// Create a new shared informer factor
	// Consider sharing this factory if other controllers/informers are needed.
	var ds v1.GCPDataSource
	_ = ds
	k8sClient, err := getKubeClientInCluster()
	if err != nil {
		return nil, fmt.Errorf("failed to init k8s client")
	}

	controllerNs := os.Getenv(controllerNamespaceEnvVar)
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(klog.Infof)
	// Record events to Kubernetes API server
	// Pass the namespace where the controller has permission to create events.
	eventBroadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: k8sClient.CoreV1().Events(controllerNs)})
	// Create the recorder instance
	// The component name should be unique and identify the source of the event.
	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{Component: DriverName + "-controller"})
	klog.Infof("Event recorder initialized for component %s, recording events in namespace %q", DriverName+"-controller", controllerNs)

	informerFactory := informers.NewSharedInformerFactory(k8sClient, controllerResync)
	nodeInformer := informerFactory.Core().V1().Nodes()
	pvInformer := informerFactory.Core().V1().PersistentVolumes()
	scInformer := informerFactory.Storage().V1().StorageClasses()
	klog.Info("PersistentVolume informer obtained from factory.")
	// Start the informer factory. This is crucial.
	// The stopCh should be closed when the driver terminates.
	ctx := context.Background()
	go informerFactory.Start(ctx.Done())

	// Wait for the initial sync. It's better to wait here or during driver startup
	// than checking in every RPC call.
	if !cache.WaitForCacheSync(ctx.Done(), nodeInformer.Informer().HasSynced, pvInformer.Informer().HasSynced, scInformer.Informer().HasSynced) {
		return nil, fmt.Errorf("timed out waiting for node informer caches to sync")
	}
	klog.Info("Controller informer cache synced successfully.")

	return &controllerServer{
		driver:                     driver,
		storageServiceManager:      storageServiceManager,
		volumeLocks:                util.NewVolumeLocks(),
		nodeLister:                 nodeInformer.Lister(),
		nodeInformerSynced:         nodeInformer.Informer().HasSynced,
		pvLister:                   pvInformer.Lister(),
		pvInformerSynced:           pvInformer.Informer().HasSynced,
		storageClassLister:         scInformer.Lister(),
		storageClassInformerSynced: scInformer.Informer().HasSynced,
		recorder:                   recorder,
		controllerNamespace:        controllerNs,
		k8sClient:                  k8sClient,
	}, nil
}

func (s *controllerServer) ControllerGetCapabilities(_ context.Context, _ *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	return &csi.ControllerGetCapabilitiesResponse{
		Capabilities: s.driver.cscap,
	}, nil
}

func (s *controllerServer) ValidateVolumeCapabilities(ctx context.Context, req *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	// Validate arguments
	volumeID := req.GetVolumeId()
	if req.GetVolumeContext()[VolumeContextKeyEphemeral] != util.TrueStr {
		volumeID = parseVolumeID(volumeID)
	}
	if len(volumeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "ValidateVolumeCapabilities volumeID must be provided")
	}
	caps := req.GetVolumeCapabilities()
	if len(caps) == 0 {
		return nil, status.Error(codes.InvalidArgument, "ValidateVolumeCapabilities volume capabilities must be provided")
	}

	storageService, err := s.prepareStorageService(ctx, req.GetSecrets())
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "failed to prepare storage service: %v", err)
	}
	defer storageService.Close()

	// Check that the volume exists
	if exist, err := storageService.CheckBucketExists(ctx, &storage.ServiceBucket{Name: volumeID}); !exist {
		return nil, status.Errorf(storage.ParseErrCode(err), "volume %v doesn't exist: %v", volumeID, err)
	}

	// Validate that the volume matches the capabilities
	// Note that there is nothing in the bucket that we actually need to validate
	if err := s.driver.validateVolumeCapabilities(caps); err != nil {
		return &csi.ValidateVolumeCapabilitiesResponse{
			Message: err.Error(),
		}, status.Error(codes.InvalidArgument, err.Error())
	}

	return &csi.ValidateVolumeCapabilitiesResponse{
		Confirmed: &csi.ValidateVolumeCapabilitiesResponse_Confirmed{
			VolumeContext:      req.GetVolumeContext(),
			VolumeCapabilities: req.GetVolumeCapabilities(),
			Parameters:         req.GetParameters(),
		},
	}, nil
}

func (s *controllerServer) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	// Validate arguments
	name := req.GetName()
	if len(name) == 0 {
		return nil, status.Error(codes.InvalidArgument, "CreateVolume name must be provided")
	}
	volumeID := strings.ToLower(name)

	if err := s.driver.validateVolumeCapabilities(req.GetVolumeCapabilities()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	capBytes, err := getRequestCapacity(req.GetCapacityRange())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	secrets := req.GetSecrets()
	projectID, ok := secrets["projectID"]
	if !ok {
		return nil, status.Error(codes.InvalidArgument, "projectID must be provided in secret")
	}

	if acquired := s.volumeLocks.TryAcquire(volumeID); !acquired {
		return nil, status.Errorf(codes.Aborted, util.VolumeOperationAlreadyExistsFmt, volumeID)
	}
	defer s.volumeLocks.Release(volumeID)

	param := req.GetParameters()
	newBucket := &storage.ServiceBucket{
		Project:                        projectID,
		Name:                           volumeID,
		SizeBytes:                      capBytes,
		EnableUniformBucketLevelAccess: true,
	}

	storageService, err := s.prepareStorageService(ctx, secrets)
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "failed to prepare storage service: %v", err)
	}
	defer storageService.Close()

	// Check if the bucket already exists
	bucket, err := storageService.GetBucket(ctx, newBucket)
	if err != nil && !storage.IsNotExistErr(err) {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if bucket != nil {
		klog.V(4).Infof("Found existing bucket %+v, current bucket %+v\n", bucket, newBucket)
		// Bucket already exists, check if it meets the request
		if err = storage.CompareBuckets(newBucket, bucket); err != nil {
			return nil, status.Error(codes.AlreadyExists, err.Error())
		}
	} else {
		// Add labels
		labels, err := extractLabels(param, s.driver.config.Name)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		newBucket.Labels = labels

		// Create the bucket
		var createErr error
		bucket, createErr = storageService.CreateBucket(ctx, newBucket)
		if createErr != nil {
			return nil, status.Error(codes.Internal, createErr.Error())
		}
	}
	resp := &csi.CreateVolumeResponse{Volume: bucketToCSIVolume(bucket)}

	return resp, nil
}

func (s *controllerServer) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	// Validate arguments
	volumeID := req.GetVolumeId()
	if len(volumeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "DeleteVolume volumeID must be provided")
	}

	if acquired := s.volumeLocks.TryAcquire(volumeID); !acquired {
		return nil, status.Errorf(codes.Aborted, util.VolumeOperationAlreadyExistsFmt, volumeID)
	}
	defer s.volumeLocks.Release(volumeID)

	storageService, err := s.prepareStorageService(ctx, req.GetSecrets())
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "failed to prepare storage service: %v", err)
	}

	// Delete the volume
	err = storageService.DeleteBucket(ctx, &storage.ServiceBucket{Name: volumeID})
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &csi.DeleteVolumeResponse{}, nil
}

func (s *controllerServer) ControllerPublishVolume(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	klog.Infof("ControllerPublishVolume called")
	volumeID := req.GetVolumeId()
	if len(volumeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "ControllerPublishVolume Volume ID must be provided")
	}
	// TODO: remove this and read from SC
	publishContext := map[string]string{
		"skipCSIBucketAccessCheck": "true",
	}

	nodeID := req.NodeId
	volumeContext := req.GetVolumeContext()
	klog.Infof("ControllerPublishVolume: VolumeContext: %v", volumeContext)
	// --- Get PV Name from Volume Context ---
	pvName, ok := volumeContext[volumeContextOrigPVName]
	if !ok || pvName == "" {
		klog.Warningf("ControllerPublishVolume: Volume context for volume %q is missing the '%s' key.", volumeID, volumeContextOrigPVName)
	}

	if pvName == "" {
		klog.Warningf("PV name missing for volume %s, skipping smart recommendations", volumeID)
		return &csi.ControllerPublishVolumeResponse{}, nil
	}

	if !s.pvInformerSynced() {
		return nil, fmt.Errorf("PV informer cache is not synced") // Let caller wrap in gRPC status
	}
	pv, err := s.pvLister.Get(pvName)
	if err != nil {
		if apierrors.IsNotFound(err) {
			klog.Errorf("PersistentVolume %q not found in cache.", pvName)
			return nil, err // Return NotFound error for caller
		}
		klog.Errorf("Failed to get PersistentVolume %q from lister: %v", pvName, err)
		return nil, fmt.Errorf("failed to get PersistentVolume %q: %w", pvName, err) // Wrap internal error
	}
	klog.V(4).Infof("Successfully fetched PV %q from lister.", pvName)

	pvDetails, err := s.getPVDetailsFromAnnotations(pv, s.recorder)
	if err != nil {
		return nil, err
	}
	klog.Infof("Successfully parsed annotations for PV %q: %+v", pvName, pvDetails)

	scName, scNameProvided := volumeContext[volumeContextStorageClassName]
	var storageClass *storagev1.StorageClass
	var scParameters map[string]string

	if !scNameProvided || scName == "" {
		klog.Warningf("ControllerPublishVolume: Volume context for volume %q is missing the '%s' key. Cannot fetch StorageClass parameters, skipping smart recommednations for volumeID", volumeID, volumeContextStorageClassName)
		return &csi.ControllerPublishVolumeResponse{}, nil
	}

	klog.Infof("ControllerPublishVolume: Attempting to fetch StorageClass %q from volume context for volume %q", scName, volumeID)
	if s.storageClassLister == nil {
		klog.Errorf("ControllerPublishVolume: StorageClass Lister is not available for volume %q, cannot fetch StorageClass %q.", volumeID, scName)
		// This is an internal setup error
		return nil, status.Error(codes.Internal, "StorageClass lister not initialized")
	}
	if !s.storageClassInformerSynced() {
		klog.Errorf("ControllerPublishVolume: StorageClass informer cache not synced for volume %q.", volumeID)
		return nil, status.Error(codes.Unavailable, "StorageClass informer cache is not synced")
	}

	storageClass, err = s.storageClassLister.Get(scName)
	if err != nil {
		if apierrors.IsNotFound(err) {
			klog.Errorf("ControllerPublishVolume: StorageClass %q specified in volume context for volume %q not found.", scName, volumeID)
			return nil, status.Errorf(codes.NotFound, "StorageClass %q not found", scName)
		}
		klog.Errorf("ControllerPublishVolume: Failed to get StorageClass %q from lister for volume %q: %v", scName, volumeID, err)
		return nil, status.Errorf(codes.Internal, "Failed to get StorageClass %q: %v", scName, err)
	}

	// StorageClass found, get parameters
	if storageClass.Parameters == nil {
		klog.Infof("ControllerPublishVolume: StorageClass %q found but has nil Parameters map for volume %q, skipping smart recommendations", scName, volumeID)
		return &csi.ControllerPublishVolumeResponse{}, nil
	}

	if len(storageClass.MountOptions) > 0 {
		publishContext[publishContextMountOptionsKey] = strings.Join(storageClass.MountOptions, ",")
	}

	scParameters = storageClass.Parameters
	valStr, ok := scParameters[fusefilecacheMediumPriorityKey]
	if !ok {
		return nil, fmt.Errorf("missing fusefilecacheMediumPriority")
	}

	fileCacheMediumPriotity, err := parseFileCacheMediumPriority(valStr)
	if err != nil {
		return nil, err
	}

	fuseNodeEphemeralStorageAllocatableFactor, ok := scParameters[fuseNodeEphemeralStorageAllocatableFactorKey]
	if !ok {
		return nil, fmt.Errorf("missing fuseNodeEphemeralStorageAllocatableFactor")
	}

	fuseNodeMemoryAllocatableFactor, ok := scParameters[fuseNodeMemoryAllocatableFactorKey]
	if !ok {
		return nil, fmt.Errorf("missing fuseNodeMemoryAllocatableFactor")
	}

	fuseNodeEphemeralStorageAllocatableFactorVal, _ := strconv.ParseFloat(fuseNodeEphemeralStorageAllocatableFactor, 32)
	fuseNodeMemoryAllocatableFactorVal, _ := strconv.ParseFloat(fuseNodeMemoryAllocatableFactor, 32)
	klog.Infof("ControllerPublishVolume: Successfully fetched StorageClass %q with parameters: %v", scName, scParameters)
	if s.nodeLister == nil {
		klog.Errorf("ControllerPublishVolume: Node Lister is not available for volume %q, cannot fetch node %q.", volumeID, nodeID)
		return nil, status.Error(codes.Internal, "StorageClass lister not initialized")
	}

	// Check if the informer cache is synced (important!)
	// This check is less critical if WaitForCacheSync was successful on startup,
	// but provides an extra layer of safety.
	if !s.nodeInformerSynced() {
		return nil, status.Error(codes.Unavailable, "Node informer cache is not synced")
	}

	klog.V(4).Infof("ControllerPublishVolume: Attempting to fetch node %q from lister", nodeID)
	node, err := s.nodeLister.Get(nodeID)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, status.Errorf(codes.NotFound, "Node %q not found", nodeID)
		}
		return nil, status.Errorf(codes.Internal, "Failed to get node %q from lister: %v", nodeID, err)
	}

	// Node found, extract information
	nodeAnnotations := node.GetAnnotations()
	nodeAllocatable := node.Status.Allocatable

	// Log the extracted information (replace with actual logic as needed)
	klog.Infof("ControllerPublishVolume: Successfully fetched node %q", nodeID)
	klog.V(4).Infof("Node %q Annotations: %v", nodeID, nodeAnnotations)
	klog.V(4).Infof("Node %q Status.Allocatable: %v", nodeID, nodeAllocatable)

	// TODO: Add logic here that USES the nodeAnnotations or nodeAllocatable info
	// For example, you might pass some annotation value into the PublishContext
	// or validate something based on allocatable resources.
	nodeAllocatables := parseNodeAllocatableResources(nodeAllocatable)
	nodeType := "general_purpose"
	if isGpuNodeByResource(node) {
		nodeType = nodeTypeGPU
	}
	if isTpuNodeByResource(node) {
		nodeType = nodeTypeTPU
	}
	recommendation, err := s.recommendGCSFuseCacheConfigs(
		pv,
		pvDetails,
		node.Name,
		nodeAllocatables,
		nodeType,
		fileCacheMediumPriotity,
		hasLocalSSDEphemeralStorageAnnotation(nodeAnnotations),
		fuseNodeMemoryAllocatableFactorVal,
		fuseNodeEphemeralStorageAllocatableFactorVal)
	if err != nil {
		return nil, err
	}
	//////////////////
	publishContext[publishContextMetadataCacheBytesKey] = strconv.FormatInt(recommendation.MetadataCacheBytes, 10)
	publishContext[publishContextFileCacheBytesKey] = strconv.FormatInt(recommendation.FileCacheBytes, 10)
	publishContext[publishContextFileCacheMediumKey] = recommendation.FilecacheMedium
	//////////////////
	if recommendation != nil {
		publishContext[publishContextMetadataCacheBytesKey] = strconv.FormatInt(recommendation.MetadataCacheBytes, 10)
		// --- MODIFIED PD CACHE HANDLING for WaitForFirstConsumer ---
		if recommendation.FilecacheMedium == mediumPDSSD && recommendation.FileCacheBytes > 0 && pvName != "" {
			klog.Infof("ControllerPublishVolume: PD-SSD cache medium recommended for PV %q on node %q. Size: %d bytes. Ensuring helper resources.", pvName, nodeID, recommendation.FileCacheBytes)

			cachePVCName := "gcsfuse-cache-" + nodeID + "-" + volumeID
			if len(cachePVCName) > 63 {
				cachePVCName = cachePVCName[:63]
			}
			cachePVCName = strings.Trim(cachePVCName, "-")
			cachePodName := cachePVCName // Using same base name for simplicity

			labels := map[string]string{
				cacheResourceLabelKeyManagedBy: cacheResourceLabelValueDriver,
				cacheResourceLabelKeyTargetPV:  pvName,
				cacheResourceLabelKeyPurpose:   cacheResourceLabelValueFileCache,
			}

			podClient := s.k8sClient.CoreV1().Pods(s.controllerNamespace)
			pvcClient := s.k8sClient.CoreV1().PersistentVolumeClaims(s.controllerNamespace)

			var currentPod *corev1.Pod
			var cacheActualPVName string
			var cachePodUID string

			currentPod, err = podClient.Get(ctx, cachePodName, metav1.GetOptions{})
			if err != nil {
				if apierrors.IsNotFound(err) {
					klog.Infof("ControllerPublishVolume: Cache Pod %s/%s not found. Will create PVC and Pod.", s.controllerNamespace, cachePodName)

					// 1. Ensure PVC exists (create if not). No need to wait for binding here.
					_, pvcGetErr := pvcClient.Get(ctx, cachePVCName, metav1.GetOptions{})
					if pvcGetErr != nil {
						if apierrors.IsNotFound(pvcGetErr) {
							klog.Infof("ControllerPublishVolume: Cache PVC %s/%s not found, creating.", s.controllerNamespace, cachePVCName)
							pvcStorageQuantity, parseErr := resource.ParseQuantity(fmt.Sprintf("%d", recommendation.FileCacheBytes))
							if parseErr != nil {
								klog.Errorf("ControllerPublishVolume: Failed to parse FileCacheBytes %d into quantity: %v", recommendation.FileCacheBytes, parseErr)
								return nil, status.Errorf(codes.Internal, "Invalid FileCacheBytes value for PVC: %v", parseErr)
							}
							var pdCachePVCStorageClassName = "premium-rwo"
							cachePVCSpec := &corev1.PersistentVolumeClaim{
								ObjectMeta: metav1.ObjectMeta{Name: cachePVCName, Namespace: s.controllerNamespace, Labels: labels},
								Spec: corev1.PersistentVolumeClaimSpec{
									AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
									Resources: corev1.VolumeResourceRequirements{
										Requests: corev1.ResourceList{
											corev1.ResourceStorage: pvcStorageQuantity,
										},
									},
									StorageClassName: &pdCachePVCStorageClassName,
								},
							}
							if _, createErr := pvcClient.Create(ctx, cachePVCSpec, metav1.CreateOptions{}); createErr != nil {
								klog.Errorf("ControllerPublishVolume: Failed to create cache PVC %s/%s: %v", s.controllerNamespace, cachePVCName, createErr)
								return nil, status.Errorf(codes.Internal, "Failed to create cache PVC: %v", createErr)
							}
							klog.Infof("ControllerPublishVolume: Cache PVC %s/%s created. Binding will occur upon Pod schedule.", s.controllerNamespace, cachePVCName)
						} else {
							klog.Errorf("ControllerPublishVolume: Failed to get cache PVC %s/%s: %v", s.controllerNamespace, cachePVCName, pvcGetErr)
							return nil, status.Errorf(codes.Internal, "Failed to get cache PVC: %v", pvcGetErr)
						}
					} else {
						klog.Infof("ControllerPublishVolume: Cache PVC %s/%s already exists.", s.controllerNamespace, cachePVCName)
					}

					// 2. Create the Pod
					klog.Infof("ControllerPublishVolume: Creating Cache Pod %s/%s for Node %s.", s.controllerNamespace, cachePodName, nodeID)
					cachePodSpec := &corev1.Pod{
						ObjectMeta: metav1.ObjectMeta{Name: cachePodName, Namespace: s.controllerNamespace, Labels: labels},
						Spec: corev1.PodSpec{
							// ServiceAccountName: s.serviceAccountName, // Or a specific minimal SA for the helper pod
							Volumes:       []corev1.Volume{{Name: "cache-storage", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: cachePVCName}}}},
							Containers:    []corev1.Container{{Name: "cache-holder", Image: cacheHelperPodImage, Command: []string{"/bin/sh", "-c", "trap : TERM INT; sleep infinity & wait"}, VolumeMounts: []corev1.VolumeMount{{Name: "cache-storage", MountPath: "/cache"}}}},
							RestartPolicy: corev1.RestartPolicyAlways,
							Affinity: &corev1.Affinity{
								NodeAffinity: &corev1.NodeAffinity{
									RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
										NodeSelectorTerms: []corev1.NodeSelectorTerm{
											{
												MatchExpressions: []corev1.NodeSelectorRequirement{
													{
														Key:      "kubernetes.io/hostname",
														Operator: corev1.NodeSelectorOpIn,
														Values:   []string{nodeID},
													},
												},
											},
										},
									},
								},
							},
						},
					}
					createdPod, createErr := podClient.Create(ctx, cachePodSpec, metav1.CreateOptions{})
					if createErr != nil {
						klog.Errorf("ControllerPublishVolume: Failed to create cache Pod %s/%s: %v", s.controllerNamespace, cachePodName, createErr)
						// Attempt to clean up PVC if Pod creation fails immediately? Or rely on future ControllerUnpublish. For now, error out.
						return nil, status.Errorf(codes.Internal, "Failed to create cache Pod: %v", createErr)
					}
					currentPod = createdPod // Use the newly created pod for waiting
				} else { // Other error getting pod
					klog.Errorf("ControllerPublishVolume: Failed to get cache Pod %s/%s: %v", s.controllerNamespace, cachePodName, err)
					return nil, status.Errorf(codes.Internal, "Failed to get cache Pod: %v", err)
				}
			} else { // Pod already exists
				klog.Infof("ControllerPublishVolume: Cache Pod %s/%s already exists.", s.controllerNamespace, cachePodName)
				if currentPod.Spec.NodeName != nodeID {
					klog.Errorf("ControllerPublishVolume: Cache Pod %s/%s exists but is on node %s, expected %s. This is a critical mismatch.", s.controllerNamespace, cachePodName, currentPod.Spec.NodeName, nodeID)
					return nil, status.Errorf(codes.FailedPrecondition, "Cache Pod %s found on incorrect node %s, expected %s", cachePodName, currentPod.Spec.NodeName, nodeID)
				}
			}

			// 3. Wait for Pod to be ready (this implies PVC is now attempting to bind or already bound)
			klog.Infof("ControllerPublishVolume: Waiting for cache Pod %s/%s to be ready on node %s...", s.controllerNamespace, cachePodName, nodeID)
			err = wait.PollUntilContextTimeout(ctx, cachePodPollInterval, cachePodPollTimeout, true, func(conditionCtx context.Context) (bool, error) {
				// Use conditionCtx for the API call inside the poll condition
				pod, pollErr := podClient.Get(conditionCtx, cachePodName, metav1.GetOptions{})
				if pollErr != nil {
					if apierrors.IsNotFound(pollErr) { // Pod got deleted during wait?
						klog.Warningf("ControllerPublishVolume: Cache Pod %s/%s was deleted during readiness check.", s.controllerNamespace, cachePodName)
						return false, pollErr // Propagate error to stop polling immediately
					}
					// For other errors, you might want to continue polling or stop.
					// Returning nil here means "not done yet, no error, continue polling".
					klog.V(4).Infof("ControllerPublishVolume: Error getting cache Pod %s/%s during poll: %v. Retrying.", s.controllerNamespace, cachePodName, pollErr)
					return false, nil // Continue polling despite this transient error
				}
				// Update the outer scope 'currentPod' if you need its latest state after the poll succeeds.
				// If only used within this polling, a local var is fine.
				// For this example, assuming currentPod is from outer scope and needs update on success.
				// currentPod = pod // Be careful with concurrent access if s.currentPod or similar
				// Assign to a local var first, then to outer scope if poll succeeds.

				if pod.Status.Phase == corev1.PodRunning {
					for _, cond := range pod.Status.Conditions {
						if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
							klog.V(4).Infof("ControllerPublishVolume: Cache Pod %s/%s is Running and Ready.", s.controllerNamespace, cachePodName)
							// Update currentPod from outer scope ONLY when condition is met
							// to ensure it holds the 'Ready' state.
							// This assignment should ideally happen outside the poll loop after success,
							// using the 'pod' variable from the last successful Get.
							// For simplicity here, if currentPod is just to hold the last seen version:
							// currentPod = pod
							return true, nil // Pod is Running and Ready
						}
					}
				}
				klog.V(4).Infof("ControllerPublishVolume: Cache Pod %s/%s phase is %s, conditions: %v, waiting...", s.controllerNamespace, cachePodName, pod.Status.Phase, pod.Status.Conditions)
				return false, nil // Not ready yet
			})
			if err != nil {
				klog.Errorf("ControllerPublishVolume: Cache Pod %s/%s did not become ready: %v", s.controllerNamespace, cachePodName, err)
				return nil, status.Errorf(codes.DeadlineExceeded, "Cache Pod %s did not become ready in time: %v", cachePodName, err)
			}
			cachePodUID = string(currentPod.UID)
			klog.Infof("ControllerPublishVolume: Cache Pod %s/%s (UID: %s) is ready on node %s.", s.controllerNamespace, cachePodName, cachePodUID, nodeID)

			// 4. After Pod is ready, get the PVC to find the bound PV name
			finalPVC, pvcGetErr := pvcClient.Get(ctx, cachePVCName, metav1.GetOptions{})
			if pvcGetErr != nil {
				klog.Errorf("ControllerPublishVolume: Failed to get cache PVC %s/%s after Pod readiness: %v", s.controllerNamespace, cachePVCName, pvcGetErr)
				return nil, status.Errorf(codes.Internal, "Failed to re-fetch cache PVC %s: %v", cachePVCName, pvcGetErr)
			}
			if finalPVC.Status.Phase != corev1.ClaimBound || finalPVC.Spec.VolumeName == "" {
				klog.Errorf("ControllerPublishVolume: Cache Pod %s/%s is Ready, but its PVC %s is not Bound (%s) or has no VolumeName. This is unexpected.", s.controllerNamespace, cachePodName, cachePVCName, finalPVC.Status.Phase)
				return nil, status.Errorf(codes.Internal, "Cache PVC %s not bound or VolumeName missing after Pod ready", cachePVCName)
			}
			cacheActualPVName = finalPVC.Spec.VolumeName
			klog.Infof("ControllerPublishVolume: Cache PVC %s/%s bound to PV %s after Pod readiness.", s.controllerNamespace, cachePVCName, cacheActualPVName)

			// --- NEW LOGIC STARTS HERE ---
			// 5. Fetch the cache PV object using cacheActualPVName
			cachePV, pvGetErr := s.pvLister.Get(cacheActualPVName)
			if pvGetErr != nil {
				if apierrors.IsNotFound(pvGetErr) {
					klog.Errorf("ControllerPublishVolume: Cache PV %q (from PVC %s/%s) not found in lister.", cacheActualPVName, s.controllerNamespace, cachePVCName)
					return nil, status.Errorf(codes.NotFound, "Cache PV %q not found", cacheActualPVName)
				}
				klog.Errorf("ControllerPublishVolume: Failed to get Cache PV %q from lister: %v", cacheActualPVName, pvGetErr)
				return nil, status.Errorf(codes.Internal, "Failed to get Cache PV %q: %v", cacheActualPVName, pvGetErr)
			}
			klog.V(4).Infof("ControllerPublishVolume: Successfully fetched Cache PV %q from lister.", cacheActualPVName)

			// 6. Extract the VolumeHandle from the cache PV
			if cachePV.Spec.CSI == nil || cachePV.Spec.CSI.VolumeHandle == "" {
				klog.Errorf("ControllerPublishVolume: Cache PV %q has no CSI spec or an empty VolumeHandle.", cacheActualPVName)
				return nil, status.Errorf(codes.Internal, "Cache PV %q is invalid (missing CSI spec or VolumeHandle)", cacheActualPVName)
			}
			cacheVolumeHandle := cachePV.Spec.CSI.VolumeHandle
			klog.V(4).Infof("ControllerPublishVolume: Extracted VolumeHandle '%s' from Cache PV %q.", cacheVolumeHandle, cacheActualPVName)

			// 7. Calculate SHA256 sum of the VolumeHandle and hex-encode it
			hash := sha256.Sum256([]byte(cacheVolumeHandle))
			volumeHandleSHA256 := hex.EncodeToString(hash[:])
			klog.V(4).Infof("ControllerPublishVolume: SHA256 of Cache PV VolumeHandle '%s' is '%s'.", cacheVolumeHandle, volumeHandleSHA256)

			// 8. Frame the staging path for the cache PV
			// The CSI driver name "pd.csi.storage.gke.io" is hardcoded as per your requirement.
			// cachePVStagingPath := fmt.Sprintf("/var/lib/kubelet/plugins/kubernetes.io/csi/pd.csi.storage.gke.io/%s/globalmount", volumeHandleSHA256)

			cachePVStagingPath := fmt.Sprintf(nodePublishPathFmt, cachePodUID, cacheActualPVName)
			klog.Infof("ControllerPublishVolume: Constructed node publish path for Cache PV %q (VolumeHandle: %s) is: %s",
				cacheActualPVName, cacheVolumeHandle, cachePVStagingPath)
			// Add the constructed staging path to publishContext
			publishContext[publishContextCachePVStagingPathKey] = cachePVStagingPath

			// Update publishContext for PD cache
			// publishContext[publishContextFileCacheBytesKey] = "0" // Signal to NodePublish that cache is externally managed
			// publishContext[publishContextFileCacheMediumKey] = recommendation.FilecacheMedium
			publishContext[publishContextCachePVCNameKey] = cachePVCName
			publishContext[publishContextCachePVNameKey] = cacheActualPVName
			publishContext[publishContextCachePodNameKey] = cachePodName // Pass pod name
			publishContext[publishContextCachePodUIDKey] = cachePodUID   // Pass pod UID
			klog.Infof("ControllerPublishVolume: Updated publishContext with PD cache info: %+v", publishContext)
		}

		publishContext[publishContextFileCacheBytesKey] = strconv.FormatInt(recommendation.FileCacheBytes, 10)
		publishContext[publishContextFileCacheMediumKey] = recommendation.FilecacheMedium
		eventMessage := fmt.Sprintf(
			"GCSFuse Recommendation Details:\n"+
				"  Target Information:\n"+
				"    PV Name: %s\n"+
				"    Node Name: %s\n"+
				"    Node Type: %s\n"+
				"  Input Signals Used for Recommendation:\n"+
				"    Bucket - Total Objects: %d\n"+
				"    Bucket - Total Data Size (Bytes): %d\n"+
				"    Node - Allocatable RAM (Bytes): %d\n"+
				"    Node - Allocatable Ephemeral Storage (Bytes): %d\n"+
				"    Fuse - Max  Allocatable RAM (Bytes): %d\n"+
				"    Fuse - Allocatable Ephemeral Storage (Bytes): %d\n"+
				"  Recommended GCSFuse Configuration:\n"+
				"    Metadata Cache (Bytes): %d\n"+
				"    File Cache (Bytes): %d\n"+
				"    File Cache Medium: %s",
			pvName,
			node.Name,
			nodeType,
			recommendation.SignalNumObjects,
			recommendation.SignalTotalDataSizeBytes,
			recommendation.SignalNodeAllocatableBytesRam,
			recommendation.SignalNodeAllocatableBytesEpehemralStorage,
			recommendation.SignalMaxFuseMemoryAllocatableBytes,
			recommendation.SignalMaxFuseEphemeralStorageAllocatableBytes,
			recommendation.MetadataCacheBytes,
			recommendation.FileCacheBytes,
			recommendation.FilecacheMedium,
		)
		s.createEvent(pv, corev1.EventTypeNormal, "GCSFuseConfigRecommender", eventMessage)
		// --- END MODIFIED PD CACHE HANDLING ---
	} else if err != nil { // Error from recommendGCSFuseCacheConfigs
		publishContext[publishContextFileCacheBytesKey] = "0"
		publishContext[publishContextFileCacheMediumKey] = "none"
		klog.Warningf("ControllerPublishVolume: Proceeding without file cache recommendations for PV %q due to error: %v", pvName, err)
	} else { // recommendation object itself is nil
		publishContext[publishContextFileCacheBytesKey] = "0"
		publishContext[publishContextFileCacheMediumKey] = "none"
		klog.Warningf("ControllerPublishVolume: Recommendation object is nil for PV %q. Proceeding without file cache recommendations.", pvName)
	}

	klog.Info("Controller Publish context %v", publishContext)
	return &csi.ControllerPublishVolumeResponse{
		PublishContext: publishContext,
	}, nil
}

func (s *controllerServer) getPVDetailsFromAnnotations(
	pv *corev1.PersistentVolume,
	recorder record.EventRecorder,
) (*PVDetails, error) {
	// Extract annotations
	pvAnnotations := pv.GetAnnotations()
	if pvAnnotations == nil {
		klog.Warningf("PV %q has no annotations.", pv.Name)
		missingAnnotations := []string{annotationNumObjects, annotationTotalSizeBytes}
		msg := fmt.Sprintf("Required GCS Fuse annotations missing: %s", strings.Join(missingAnnotations, ", "))
		s.createEvent(pv, corev1.EventTypeWarning, "MissingGCSFuseAnnotations", msg)
		return nil, nil // Annotations missing, but PV found. Event emitted. No details.
	}

	// Get annotation values as strings
	numObjectsStr, numObjectsFound := pvAnnotations[annotationNumObjects]
	totalSizeStr, totalSizeFound := pvAnnotations[annotationTotalSizeBytes]

	// Check for missing annotations
	var missingAnnotations []string
	if !numObjectsFound {
		missingAnnotations = append(missingAnnotations, annotationNumObjects)
	}
	if !totalSizeFound {
		missingAnnotations = append(missingAnnotations, annotationTotalSizeBytes)
	}

	if len(missingAnnotations) > 0 {
		klog.Warningf("PV %q is missing required annotations: %s", pv.Name, strings.Join(missingAnnotations, ", "))
		msg := fmt.Sprintf("Annotations missing for finegrained tuning of gcsfuse client: %s", strings.Join(missingAnnotations, ", "))
		s.createEvent(pv, corev1.EventTypeWarning, "MissingGCSFuseAnnotations", msg)
		return nil, nil // Annotations missing, but PV found. Event emitted. No details.
	}

	// Parse annotations into int64
	var parseErrors []string
	var numObjects int64
	var totalSizeBytes int64

	numObjects, err := strconv.ParseInt(numObjectsStr, 10, 64)
	if err != nil {
		parseErrors = append(parseErrors, fmt.Sprintf("failed to parse %s value %q: %v", annotationNumObjects, numObjectsStr, err))
	}

	totalSizeBytes, err = strconv.ParseInt(totalSizeStr, 10, 64)
	if err != nil {
		parseErrors = append(parseErrors, fmt.Sprintf("failed to parse %s value %q: %v", annotationTotalSizeBytes, totalSizeStr, err))
	}

	if len(parseErrors) > 0 {
		errorMsg := strings.Join(parseErrors, "; ")
		klog.Errorf("PV %q has invalid annotation format: %s", pv.Name, errorMsg)
		msg := fmt.Sprintf("Invalid format for GCS Fuse annotations: %s", errorMsg)
		s.createEvent(pv, corev1.EventTypeWarning, "InvalidAnnotationFormat", msg)
		// Return an error indicating invalid data found in the PV
		return nil, fmt.Errorf("invalid annotation format on PV %q: %s", pv.Name, errorMsg)
	}

	// Success! Create and return the details.
	details := &PVDetails{
		NumObjects:     numObjects,
		TotalSizeBytes: totalSizeBytes,
	}

	return details, nil
}

// func (gceCS *controllerServer) ControllerUnpublishVolume(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
// 	klog.Infof("ControllerUnpublishVolume called")
// 	return &csi.ControllerUnpublishVolumeResponse{}, nil
// }

// Assuming these constants were defined and used in ControllerPublishVolume
const (
	cacheResourceDeletePollInterval = 5 * time.Second
	cacheResourceDeleteTimeout      = 2 * time.Minute // Timeout for deleting a single resource (Pod or PVC)
)

func (s *controllerServer) ControllerUnpublishVolume(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	volumeID := req.GetVolumeId()
	nodeID := req.GetNodeId() // Though not strictly needed for deleting namespaced resources by name
	klog.Infof("ControllerUnpublishVolume called for VolumeID: %s, NodeID: %s", volumeID, nodeID)

	if len(volumeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "ControllerUnpublishVolume Volume ID must be provided")
	}

	cachePVCName := "gcsfuse-cache-" + nodeID + "-" + volumeID
	if len(cachePVCName) > 63 {
		cachePVCName = cachePVCName[:63]
	}
	cachePVCName = strings.Trim(cachePVCName, "-")
	cachePodName := cachePVCName

	if cachePodName == "" && cachePVCName == "" {
		klog.Infof("ControllerUnpublishVolume: Helper Pod and PVC names not found in PublishContext for VolumeID %s. No PD-cache resources to clean up.", volumeID)
		return &csi.ControllerUnpublishVolumeResponse{}, nil
	}

	var RWMOncePodDeletePropagationPolicy = metav1.DeletePropagationForeground // Ensures dependents (like PVC mounts) are cleaned up first
	// Or use metav1.DeletePropagationBackground for faster API response, but less guarantee on order.
	// Foreground can sometimes lead to long waits if finalizers are stuck on the Pod.
	// For simple busybox pods, background is usually fine, but foreground is safer for resource release.

	// --- Delete Helper Pod ---
	if cachePodName != "" {
		klog.Infof("ControllerUnpublishVolume: Attempting to delete helper Pod %s/%s for VolumeID %s.", s.controllerNamespace, cachePodName, volumeID)
		podClient := s.k8sClient.CoreV1().Pods(s.controllerNamespace)
		err := podClient.Delete(ctx, cachePodName, metav1.DeleteOptions{
			PropagationPolicy: &RWMOncePodDeletePropagationPolicy, // Or nil for default
		})
		if err != nil && !apierrors.IsNotFound(err) {
			klog.Warningf("ControllerUnpublishVolume: Failed to initiate delete for helper Pod %s/%s: %v. Proceeding with PVC cleanup attempt.", s.controllerNamespace, cachePodName, err)
			// Don't return error yet, try to clean up PVC as well. Cleanup is best effort.
		} else if apierrors.IsNotFound(err) {
			klog.Infof("ControllerUnpublishVolume: Helper Pod %s/%s was already deleted or not found.", s.controllerNamespace, cachePodName)
		} else {
			klog.Infof("ControllerUnpublishVolume: Delete initiated for helper Pod %s/%s. Waiting for termination...", s.controllerNamespace, cachePodName)
			// Wait for the Pod to be actually deleted
			err = wait.PollUntilContextTimeout(ctx, cacheResourceDeletePollInterval, cacheResourceDeleteTimeout, true,
				func(conditionCtx context.Context) (bool, error) {
					_, getErr := podClient.Get(conditionCtx, cachePodName, metav1.GetOptions{})
					if apierrors.IsNotFound(getErr) {
						return true, nil // Successfully deleted
					}
					if getErr != nil {
						// Log non-critical errors, but continue polling unless it's a permanent issue
						klog.Warningf("Error getting cache Pod %s/%s during deletion poll: %v. Retrying.", s.controllerNamespace, cachePodName, getErr)
						return false, nil // Consider this transient and continue polling
					}
					klog.V(4).Infof("Cache Pod %s/%s still present, waiting for deletion...", s.controllerNamespace, cachePodName)
					return false, nil // Still exists
				})

			if err != nil {
				klog.Warningf("ControllerUnpublishVolume: Error or timeout waiting for helper Pod %s/%s to be deleted: %v. Proceeding with PVC cleanup attempt.", s.controllerNamespace, cachePodName, err)
			} else {
				klog.Infof("ControllerUnpublishVolume: Helper Pod %s/%s successfully deleted.", s.controllerNamespace, cachePodName)
			}
		}
	} else {
		klog.Infof("ControllerUnpublishVolume: No helper Pod name in PublishContext for VolumeID %s.", volumeID)
	}

	// --- Delete Helper PVC ---
	// This is done after attempting Pod deletion to allow the Pod to release the PVC.
	if cachePVCName != "" {
		klog.Infof("ControllerUnpublishVolume: Attempting to delete helper PVC %s/%s for VolumeID %s.", s.controllerNamespace, cachePVCName, volumeID)
		pvcClient := s.k8sClient.CoreV1().PersistentVolumeClaims(s.controllerNamespace)
		err := pvcClient.Delete(ctx, cachePVCName, metav1.DeleteOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			klog.Warningf("ControllerUnpublishVolume: Failed to delete helper PVC %s/%s: %v.", s.controllerNamespace, cachePVCName, err)
			// Log, but still return success as unpublish of main volume might be okay.
		} else if apierrors.IsNotFound(err) {
			klog.Infof("ControllerUnpublishVolume: Helper PVC %s/%s was already deleted or not found.", s.controllerNamespace, cachePVCName)
		} else {
			klog.Infof("ControllerUnpublishVolume: Helper PVC %s/%s successfully deleted (or deletion initiated).", s.controllerNamespace, cachePVCName)
			// Waiting for PVC actual deletion can be complex due to finalizers from storage provisioner for the PV.
			// For this controller-managed PVC, if the PV was also managed or reclaimPolicy=Delete, it should go away.
			// Not strictly waiting for PVC deletion here to keep Unpublish responsive.
		}
	} else {
		klog.Infof("ControllerUnpublishVolume: No helper PVC name in PublishContext for VolumeID %s.", volumeID)
	}

	klog.Infof("ControllerUnpublishVolume completed for VolumeID %s.", volumeID)
	return &csi.ControllerUnpublishVolumeResponse{}, nil
}

// prepareStorageService prepares the GCS Storage Service using CreateVolume/DeleteVolume sercets.
func (s *controllerServer) prepareStorageService(ctx context.Context, secrets map[string]string) (storage.Service, error) {
	serviceAccountName, ok := secrets["serviceAccountName"]
	if !ok {
		return nil, status.Error(codes.InvalidArgument, "serviceAccountName must be provided in secret")
	}
	serviceAccountNamespace, ok := secrets["serviceAccountNamespace"]
	if !ok {
		return nil, status.Error(codes.InvalidArgument, "serviceAccountNamespace must be provided in secret")
	}

	ts := s.driver.config.TokenManager.GetTokenSourceFromK8sServiceAccount(serviceAccountNamespace, serviceAccountName, "")
	storageService, err := s.storageServiceManager.SetupService(ctx, ts)
	if err != nil {
		return nil, fmt.Errorf("storage service manager failed to setup service: %w", err)
	}

	return storageService, nil
}

// bucketToCSIVolume generates a CSI volume spec from the Google Cloud Storage Bucket.
func bucketToCSIVolume(bucket *storage.ServiceBucket) *csi.Volume {
	resp := &csi.Volume{
		CapacityBytes: bucket.SizeBytes,
		VolumeId:      bucket.Name,
	}

	return resp
}

func getRequestCapacity(capRange *csi.CapacityRange) (int64, error) {
	var capBytes int64
	// Default case where nothing is set
	if capRange == nil {
		capBytes = MinimumVolumeSizeInBytes

		return capBytes, nil
	}

	rBytes := capRange.GetRequiredBytes()
	rSet := rBytes > 0
	lBytes := capRange.GetLimitBytes()
	lSet := lBytes > 0

	if lSet && rSet && lBytes < rBytes {
		return 0, fmt.Errorf("limit bytes %v is less than required bytes %v", lBytes, rBytes)
	}
	if lSet && lBytes < MinimumVolumeSizeInBytes {
		return 0, fmt.Errorf("limit bytes %v is less than minimum volume size: %v", lBytes, MinimumVolumeSizeInBytes)
	}

	// If Required set just set capacity to that which is Required
	if rSet {
		capBytes = rBytes
	}

	// Limit is more than Required, but larger than Minimum. So we just set capcity to Minimum
	// Too small, default
	if capBytes < MinimumVolumeSizeInBytes {
		capBytes = MinimumVolumeSizeInBytes
	}

	return capBytes, nil
}

func extractLabels(parameters map[string]string, driverName string) (map[string]string, error) {
	labels := make(map[string]string)
	scLabels := make(map[string]string)
	for k, v := range parameters {
		switch strings.ToLower(k) {
		case ParameterKeyPVCName:
			labels[tagKeyCreatedForClaimName] = v
		case ParameterKeyPVCNamespace:
			labels[tagKeyCreatedForClaimNamespace] = v
		case ParameterKeyPVName:
			labels[tagKeyCreatedForVolumeName] = v
		case ParameterKeyLabels:
			var err error
			scLabels, err = util.ConvertLabelsStringToMap(v)
			if err != nil {
				return nil, fmt.Errorf("parameters contain invalid labels parameter: %w", err)
			}
		}
	}

	labels[tagKeyCreatedBy] = strings.ReplaceAll(driverName, ".", "_")
	labels, err := mergeLabels(scLabels, labels)
	if err != nil {
		return nil, err
	}

	// TODO: validate labels: https://cloud.google.com/storage/docs/tags-and-labels#bucket-labels
	for k, v := range labels {
		labels[k] = strings.ReplaceAll(v, ".", "_")
	}

	return mergeLabels(scLabels, labels)
}

func mergeLabels(scLabels map[string]string, metedataLabels map[string]string) (map[string]string, error) {
	result := make(map[string]string)
	for k, v := range metedataLabels {
		result[k] = v
	}

	for k, v := range scLabels {
		if _, ok := result[k]; ok {
			return nil, fmt.Errorf("storage Class labels cannot contain metadata label key %s", k)
		}

		result[k] = v
	}

	return result, nil
}

func (s *controllerServer) createEvent(involvedObject runtime.Object, eventType, reason, message string) {
	// Get the name for logging, ensure involvedObject implements metav1.Object
	involvedObjectName := "unknown"
	if metaObj, ok := involvedObject.(metav1.Object); ok {
		involvedObjectName = metaObj.GetName()
	} else {
		klog.Warningf("Involved object (type: %T) does not implement metav1.Object, cannot get name for logging.", involvedObject)
		// Optionally, still proceed without the name in logs
	}

	// --- Correct way to get ObjectReference ---
	objectRef, err := reference.GetReference(scheme.Scheme, involvedObject)
	if err != nil {
		// Log the error, but maybe don't fail completely? Or fallback?
		// The previous fallback logic was okay, let's keep it but log this error.
		klog.Errorf("Failed to get object reference using reference.GetReference for %q (%T): %v. Falling back to manual construction.", involvedObjectName, involvedObject, err)

		// Fallback to manual construction (Ensure involvedObject implements metav1.Object)
		metaObj, ok := involvedObject.(metav1.Object)
		if !ok {
			klog.Errorf("Cannot manually construct ObjectReference: Involved object %q (%T) does not implement metav1.Object.", involvedObjectName, involvedObject)
			return // Cannot proceed without a valid reference
		}
		gvk := involvedObject.GetObjectKind().GroupVersionKind()
		objectRef = &corev1.ObjectReference{
			Kind:       gvk.Kind,
			APIVersion: gvk.GroupVersion().String(),
			Name:       metaObj.GetName(),
			UID:        metaObj.GetUID(),
			// Namespace:  metaObj.GetNamespace(), // Will be empty for cluster-scoped objects like PV
			Namespace: "default",
			// ResourceVersion is usually not needed/set here
		}
	}
	// --- End ObjectReference ---

	// Ensure objectRef is not nil before dereferencing
	if objectRef == nil {
		klog.Errorf("Failed to obtain object reference for event.")
		return
	}

	event := &corev1.Event{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: DriverName + "-",
			Namespace:    "default",
		},
		// Use the obtained objectRef
		InvolvedObject:      *objectRef,
		Reason:              reason,
		Message:             message,
		Type:                eventType,
		Source:              corev1.EventSource{Component: DriverName},
		FirstTimestamp:      metav1.Now(),
		LastTimestamp:       metav1.Now(),
		Count:               1,
		ReportingController: DriverName,
	}

	// Use context passed to the outer function (e.g., getPVDetailsFromAnnotations)
	_, err = s.k8sClient.CoreV1().Events("default").Create(context.TODO(), event, metav1.CreateOptions{})
	if err != nil {
		// Log error referencing the object name obtained earlier
		klog.Errorf("Failed to create event for %s %q (Reason: %s): %v", objectRef.Kind, involvedObjectName, reason, err)
	} else {
		klog.V(4).Infof("Successfully created event for %s %q (Reason: %s)", objectRef.Kind, involvedObjectName, reason)
	}
}

func parseNodeAllocatableResources(nodeAllocatable corev1.ResourceList) *NodeAllocatables {
	// var allocatableMemoryBytes int64 = 0           // Default to 0 if not found/parsable
	// var allocatableEphemeralStorageBytes int64 = 0 // Default to 0 if not found/parsable
	var nodeAllocatables NodeAllocatables
	// --- Parse Memory ---
	if memQuantity, ok := nodeAllocatable[corev1.ResourceMemory]; ok {
		// Found the memory resource quantity
		memBytes, parsedOK := memQuantity.AsInt64()
		if parsedOK {
			// allocatableMemoryBytes = memBytes
			nodeAllocatables.MemoryBytes = memBytes
			klog.V(4).Infof("Successfully parsed allocatable memory: %d bytes (%s)", memBytes, memQuantity.String())
		} else {
			// This should ideally not happen for memory quantities in Allocatable
			klog.Warningf("Could not parse node allocatable memory quantity %q as int64 bytes", memQuantity.String())
		}
	} else {
		// Memory resource was not found in the map
		klog.Warningf("Node Status Allocatable map does not contain resource key %q", corev1.ResourceMemory)
	}

	// --- Parse Ephemeral Storage ---
	if storageQuantity, ok := nodeAllocatable[corev1.ResourceEphemeralStorage]; ok {
		// Found the ephemeral storage resource quantity
		storageBytes, parsedOK := storageQuantity.AsInt64()
		if parsedOK {
			// allocatableEphemeralStorageBytes = storageBytes
			nodeAllocatables.EphemeralStorageBytes = storageBytes
			klog.V(4).Infof("Successfully parsed allocatable ephemeral storage: %d bytes (%s)", storageBytes, storageQuantity.String())
		} else {
			// This should ideally not happen for storage quantities in Allocatable
			klog.Warningf("Could not parse node allocatable ephemeral storage quantity %q as int64 bytes", storageQuantity.String())
		}
	} else {
		// Ephemeral storage resource was not found in the map
		klog.Warningf("Node Status Allocatable map does not contain resource key %q", corev1.ResourceEphemeralStorage)
	}

	return &nodeAllocatables
}

func parseFileCacheMediumPriority(input string) (map[string][]string, error) {
	result := make(map[string][]string)
	if strings.TrimSpace(input) == "" {
		return result, nil
	}

	pairs := strings.Split(input, ",")
	for i, pair := range pairs {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}

		parts := strings.SplitN(pair, ":", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("malformed pair found at index %d: %q", i, pair)
		}

		key := strings.TrimSpace(parts[0])
		valueString := strings.TrimSpace(parts[1])
		if key == "" {
			return nil, fmt.Errorf("found pair with empty key at index %d: %q", i, pair)
		}

		var values []string
		if valueString != "" {
			rawValues := strings.Split(valueString, "|")
			for _, val := range rawValues {
				trimmedVal := strings.TrimSpace(val)
				if trimmedVal != "" {
					values = append(values, trimmedVal)
				}
			}
		}

		result[key] = values
	}

	return result, nil
}

func hasLocalSSDEphemeralStorageAnnotation(annotations map[string]string) bool {
	// Step 1: Get the value of the 'node.gke.io/last-applied-node-labels' annotation.
	appliedLabelsStr, ok := annotations[GKEAppliedNodeLabelsAnnotationKey]
	if !ok || appliedLabelsStr == "" {
		// The annotation key itself is missing or empty, so we can't determine.
		return false
	}

	// Step 2: Split the comma-separated string of labels into individual label strings.
	labelPairs := strings.Split(appliedLabelsStr, ",")

	// Step 3: Iterate through each label string (e.g., "key=value").
	for _, labelPairStr := range labelPairs {
		// Step 4: Split the label string into key and value.
		// Use SplitN to handle cases where a value might unexpectedly contain an '='.
		kv := strings.SplitN(labelPairStr, "=", 2)
		if len(kv) != 2 {
			// Malformed label pair, skip it.
			continue
		}

		labelKey := strings.TrimSpace(kv[0])
		labelValue := strings.TrimSpace(kv[1])

		// Step 5: Check if this is the label we're looking for and if its value is "true".
		if labelKey == EphemeralStorageLocalSSDLabelKey && labelValue == ExpectedEphemeralStorageLocalSSDLabelValue {
			return true
		}
	}

	// If we've gone through all labels and haven't found the specific key-value pair,
	// then the node does not have the indicator.
	return false
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

// Helper function for maximum of two int64
func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// Helper to check if a medium string represents a Persistent Disk type
func isPDMedium(medium string) bool {
	normalized := strings.ToLower(medium)
	// Checks if it starts with "pd-" which covers pd-ssd, pd-standard, pd-balanced etc.
	return strings.HasPrefix(normalized, "pd-")
}

// isGpuNodeByResource checks if the node has allocatable nvidia.com/gpu resources.
func isGpuNodeByResource(node *corev1.Node) bool {
	if node == nil || node.Status.Allocatable == nil {
		return false
	}
	gpuQuantity, exists := node.Status.Allocatable[nvidiaGpuResourceName]
	return exists && gpuQuantity.CmpInt64(0) > 0
}

// isTpuNodeByResource checks if the node has allocatable google.com/tpu resources.
func isTpuNodeByResource(node *corev1.Node) bool {
	if node == nil || node.Status.Allocatable == nil {
		return false
	}
	tpuQuantity, exists := node.Status.Allocatable[googleTpuResourceName]
	// Check if the key exists and the quantity is greater than 0
	return exists && tpuQuantity.CmpInt64(0) > 0
}

// recommendGCSFuseCacheConfigs calculates recommended cache sizes and medium.
// Adds specific logic for PD medium: caps at 64TiB and disables if requirement > 64TiB.
func (s *controllerServer) recommendGCSFuseCacheConfigs(
	pv *corev1.PersistentVolume,
	pvDetails *PVDetails,
	nodeName string,
	nodeAllocatables *NodeAllocatables,
	nodeType string, // e.g., "gpu", "tpu", "general_purpose"
	fileCacheMediumPriority map[string][]string, // Assumed type map[string][]string
	hasLocalSSDEphemeralStorageAnnotation bool,
	fuseNodeMemoryAllocatableFactor float64, // Assumed type float64
	fuseNodeEphemeralStorageAllocatableFactor float64, // Assumed type float64
) (*GCSFuseRecommendations, error) {
	// --- Input Validation ---
	if pvDetails == nil {
		return nil, fmt.Errorf("pvDetails cannot be nil")
	}
	if nodeAllocatables == nil {
		return nil, fmt.Errorf("nodeAllocatables cannot be nil")
	}
	// Allow factors to be 0, but clamp negative values to 0 for calculation
	if fuseNodeMemoryAllocatableFactor < 0 {
		return nil, fmt.Errorf("fuseNodeMemoryAllocatableFactor is < 0")
	}
	if fuseNodeEphemeralStorageAllocatableFactor < 0 {
		return nil, fmt.Errorf("fuseNodeEphemeralStorageAllocatableFactor is < 0")
	}

	recommendations := &GCSFuseRecommendations{} // Use pointer

	// --- 1. Calculate Initial Requirements ---
	metadataCacheRequired := pvDetails.NumObjects * metadataBytesPerObject
	// Ensure file cache requirement isn't negative (though pvDetails should ideally be validated earlier)
	fileCacheRequired := pvDetails.TotalSizeBytes
	recommendations.SignalNumObjects = pvDetails.NumObjects
	recommendations.SignalTotalDataSizeBytes = pvDetails.TotalSizeBytes
	klog.V(4).Infof("initial Requirements: MetadataCache=%d bytes, FileCache=%d bytes", metadataCacheRequired, fileCacheRequired)

	// --- 2. Calculate Max Available Resources & Cap Metadata Cache ---
	// Use allocatable resources directly, ensure non-negative
	safeNodeMemory := nodeAllocatables.MemoryBytes
	safeNodeEphemeralStorage := nodeAllocatables.EphemeralStorageBytes
	recommendations.SignalNodeAllocatableBytesRam = safeNodeMemory
	recommendations.SignalNodeAllocatableBytesEpehemralStorage = safeNodeEphemeralStorage
	// Calculate budgets based on factors
	maxFuseMemory := int64(float64(safeNodeMemory) * fuseNodeMemoryAllocatableFactor)
	maxFuseEphemeralStorage := int64(float64(safeNodeEphemeralStorage) * fuseNodeEphemeralStorageAllocatableFactor)
	recommendations.SignalMaxFuseEphemeralStorageAllocatableBytes = maxFuseEphemeralStorage
	recommendations.SignalMaxFuseMemoryAllocatableBytes = maxFuseMemory

	klog.V(4).Infof("max Fuse Budgets: Memory=%d bytes, EphemeralStorage=%d bytes (Node Allocatable Mem: %d, Eph: %d; Factors Mem: %.2f, Eph: %.2f)",
		maxFuseMemory, maxFuseEphemeralStorage, safeNodeMemory, safeNodeEphemeralStorage, fuseNodeMemoryAllocatableFactor, fuseNodeEphemeralStorageAllocatableFactor)

	recommendations.MetadataCacheBytes = minInt64(metadataCacheRequired, maxFuseMemory)
	if recommendations.MetadataCacheBytes < metadataCacheRequired && metadataCacheRequired > 0 {
		msg := fmt.Sprintf("For target node %s, required metadata cache %d bytes capped to available fuse memory budget %d bytes. This can impact perf due to increased GCS metadata API calls", nodeName, metadataCacheRequired, recommendations.MetadataCacheBytes)
		s.createEvent(pv, corev1.EventTypeWarning, "MissingGCSSubOptimalResource", msg)
	}

	// Calculate RAM remaining *after* allocating metadata cache
	availableRamForFileCache := maxInt64(0, maxFuseMemory-recommendations.MetadataCacheBytes)
	klog.V(4).Infof("available RAM for File Cache (after metadata): %d bytes", availableRamForFileCache)

	recommendations.SignalNodeType = nodeType
	// --- 3. Determine Priority List & Perform Node Type Specific Checks ---
	priorityList, found := fileCacheMediumPriority[nodeType]
	if !found {
		return nil, fmt.Errorf("no file cache medium priority list found for nodeType %q", nodeType)
	}

	klog.V(4).Infof("Using file cache medium priority list for nodeType %q: %v", nodeType, priorityList)

	// TPU Check: Error if LSSD is in the priority list for TPU
	if nodeType == nodeTypeTPU {
		for _, mediumInList := range priorityList {
			if mediumInList == mediumLSSD {
				return nil, fmt.Errorf("LSSD medium is not supported/recommended for file cache on TPU node type (%q)", nodeType)
			}
		}
		klog.V(4).Infof("TPU node type check passed: LSSD not found in priority list.")
	}

	// --- 4. Walk Through File Cache Medium Priority List ---
	foundSuitableMedium := false
	// Skip medium evaluation entirely if no file cache is required
	if fileCacheRequired <= 0 {
		klog.V(2).Infof("File cache not required (%d bytes). Skipping medium evaluation.", fileCacheRequired)
		recommendations.FileCacheBytes = 0
		recommendations.FilecacheMedium = ""
		foundSuitableMedium = true // Mark as suitable since none is needed
	} else {
		// Only loop if file cache is actually needed
		for i, medium := range priorityList {
			isLastMedium := (i == len(priorityList)-1)
			klog.V(4).Infof("Evaluating medium %q (Priority %d/%d)", medium, i+1, len(priorityList))

			availableForMedium := int64(0)
			mediumAllowed := true

			// Determine availability and limits for the current medium
			switch {
			case medium == mediumRAM:
				availableForMedium = availableRamForFileCache
				if availableForMedium <= 0 {
					klog.V(4).Infof("Medium %q skipped: No RAM available/budgeted for file cache.", medium)
					mediumAllowed = false
				}

			case medium == mediumLSSD:
				if maxFuseEphemeralStorage <= 0 {
					klog.V(4).Infof("Medium %q skipped: No ephemeral storage budget available (maxFuseEphemeralStorage=%d).", medium, maxFuseEphemeralStorage)
					mediumAllowed = false
				} else if !hasLocalSSDEphemeralStorageAnnotation {
					// Warning/Skip logic for LSSD annotation missing (as before)
					if nodeType == nodeTypeGPU || nodeType == nodeTypeGeneralPurpose {
						msg := fmt.Sprintf("For target node %q (type %q) does not have local SSD annotation %q set to true; skipping LSSD medium as a candidate", nodeName, nodeType, EphemeralStorageLocalSSDLabelKey)
						s.createEvent(pv, corev1.EventTypeWarning, "MissingGCSSubOptimalResource", msg)
					} else {
						klog.V(4).Infof("Medium %q skipped on node type %q: Node annotation %q is not 'true'.", medium, nodeType, EphemeralStorageLocalSSDLabelKey)
					}
					mediumAllowed = false
				} else {
					// LSSD Allowed: Use the ephemeral storage budget
					availableForMedium = maxFuseEphemeralStorage
				}

			case isPDMedium(medium): // Handles pd-ssd, pd-balanced etc.
				// *** PD Specific Logic ***
				// 1. Check if requirement exceeds absolute PD limit (64TiB)
				if fileCacheRequired > pd90PercentThresholdBytes {
					klog.Warningf("File cache requirement %d bytes exceeds the maximum allowed %d bytes for PD medium %q. Skipping this medium.", fileCacheRequired, pdMaxCapacityBytes, medium)
					mediumAllowed = false // Cannot satisfy requirement with this medium type AT ALL.
				} else {
					// 2. Requirement is <= 0.9 * 64TiB. Available space is MIN(ephemeral_budget, 64TiB)
					effectivePDCapacity := minInt64(fileCacheRequired, pd90PercentThresholdBytes)
					availableForMedium = effectivePDCapacity // Available space for this PD medium attempt
					mediumAllowed = true
					klog.V(4).Infof("Medium %q: Effective available capacity considering ephemeral budget (%d) and PD limit (%d) is %d bytes.",
						medium, maxFuseEphemeralStorage, pdMaxCapacityBytes, availableForMedium)
				}

			default: // Handles any other unknown types assumed to use ephemeral budget
				return nil, fmt.Errorf("unkown storage medium")
			} // End switch determining availability

			// --- Process medium based on allowance and capacity ---
			if !mediumAllowed {
				if isLastMedium && !foundSuitableMedium {
					klog.Warningf("Last resort medium %q is not allowed/available.", medium)
					// Break loop; post-loop check will disable file cache
				}
				// Otherwise (not allowed, not last), just continue to next medium
				continue
			}

			// --- Medium is allowed, check if requirement fits ---
			klog.V(4).Infof("Medium %q is allowed. Checking if requirement %d bytes fits in available %d bytes.", medium, fileCacheRequired, availableForMedium)
			if fileCacheRequired <= availableForMedium {
				recommendations.FileCacheBytes = fileCacheRequired
				recommendations.FilecacheMedium = medium
				klog.V(2).Infof("Selected medium %q: File cache requirement %d bytes fits within available %d bytes.", medium, fileCacheRequired, availableForMedium)
				foundSuitableMedium = true
				break // Found a suitable medium that fits requirement
			} else {
				// Requirement doesn't fit in this medium's available space
				if isLastMedium {
					// *** Last resort medium, but requirement doesn't fit ***
					// Special handling for PD: If we got here for PD, fileCacheRequired > availableForMedium,
					// BUT fileCacheRequired <= pdMaxCapacityBytes (checked earlier).
					// We cap to the available space for this medium.
					recommendations.FileCacheBytes = availableForMedium // Cap to what's available
					recommendations.FilecacheMedium = medium
					klog.Warningf("File cache required %d bytes, capping to %d bytes available in last resort medium %q", fileCacheRequired, recommendations.FileCacheBytes, medium)
					// TODO: Consider creating a Kubernetes event here
					foundSuitableMedium = true
					break // Used last resort (capped)
				} else {
					// Not the last medium, and doesn't fit
					klog.V(4).Infof("File cache requirement %d bytes does not fit in medium %q (available %d bytes), trying next priority.", fileCacheRequired, medium, availableForMedium)
					// Continue to the next medium in the loop
				}
			}
		} // --- End of priority list loop ---
	} // --- End of if fileCacheRequired > 0 ---

	// --- 5. Post-Loop Checks & Warnings ---
	if !foundSuitableMedium { // This only happens now if fileCacheRequired > 0 and no medium worked
		klog.Warningf("No suitable file cache medium found or requirement exceeded limits for all options based on priority list %v and available resources for node type %q. Disabling file cache.", priorityList, nodeType)
		recommendations.FileCacheBytes = 0
		recommendations.FilecacheMedium = ""
	}

	klog.V(2).Infof("Final Recommendations: MetadataCacheBytes=%d, FileCacheBytes=%d, FilecacheMedium=%q",
		recommendations.MetadataCacheBytes, recommendations.FileCacheBytes, recommendations.FilecacheMedium)

	return recommendations, nil
}
