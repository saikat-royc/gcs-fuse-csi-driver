package pvccontroller // Or your chosen package name

import (
	"context"
	"fmt"
	"strings"
	"time"

	// GCPDataSource API
	gcpdatasourcesv1 "github.com/googlecloudplatform/gcs-fuse-csi-driver/pkg/apis/datalayer.gke.io/v1"
	// Client for GCPDataSource (needed for informer factory if using typed informers)
	// If the official library doesn't provide a clientset/informer factory easily,
	// we might need to fetch directly or use dynamic client + unstructured.
	// Let's assume for now we can get it via the generic factory or fetch directly.

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"

	// Informers and Listers
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme" // Standard scheme
	listerscorev1 "k8s.io/client-go/listers/core/v1"

	// dynamic "k8s.io/client-go/dynamic" // Needed if using dynamic client for GCPDataSource
	// dynamicinformer "k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/workqueue"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/dynamic"
	storagev1listers "k8s.io/client-go/listers/storage/v1"
	"k8s.io/klog/v2"
)

const (
	ControllerName = "gcpdatasource-pvc-controller"

	// GCPDataSource details
	gcpDataSourceAPIGroup = "datalayer.gke.io"
	gcpDataSourceVersion  = "v1"
	gcpDataSourceKind     = "GCPDataSource"
	gcpDataSourceResource = "gcpdatasources" // Plural resource name

	// GCS FUSE CSI Driver details
	csiDriverNameGCSFuse = "gcsfuse.csi.storage.gke.io" // Verify this is correct

	// PV Volume Attributes
	attrSkipCSIBucketAccessCheck = "skipCSIBucketAccessCheck"
	attrPrefetchOnMount          = "gcsfuseMetadataPrefetchOnMount"
	csiDriverConfigParameterKey  = "csiDriverConfig"
	attrOrigPVName               = "origPVName"
	attrStorageClassName         = "storageClassName"
)

// PVCControllerConfig holds configuration for the controller
type PVCControllerConfig struct {
	KubeConfigPath string
	KubeAPIQPS     float64
	KubeAPIBurst   int
	ResyncPeriod   time.Duration
	RateLimiter    workqueue.RateLimiter // Use default if nil
	NumWorkers     int                   // Number of worker goroutines
}

// PVCController manages watching PVCs and creating PVs
type PVCController struct {
	kubeClient    kubernetes.Interface
	dynamicClient dynamic.Interface // Use if fetching GCPDataSource dynamically

	pvcLister listerscorev1.PersistentVolumeClaimLister
	pvcSynced cache.InformerSynced
	scLister  storagev1listers.StorageClassLister
	scSynced  cache.InformerSynced

	// Option 1: Use a typed lister if the GCPDataSource library provides one easily with client-go
	// gcpDataSourceLister gcpdatasourcesv1listers.GCPDataSourceLister // Requires generated lister
	// gcpDataSourceSynced cache.InformerSynced

	// Option 2: Use a generic lister (less type-safe)
	// gcpDataSourceLister cache.GenericLister
	// gcpDataSourceSynced cache.InformerSynced

	// Option 3: Fetch GCPDataSource directly using kubeClient (simplest if typed lister isn't easy)
	// (No dedicated lister/synced needed in this case)

	queue   workqueue.RateLimitingInterface
	factory informers.SharedInformerFactory
	// dynamicFactory dynamicinformer.DynamicSharedInformerFactory // If using dynamic client
}

func buildConfig(kubeconfigPath string, qps float64, burst int) (*rest.Config, error) {
	var cfg *rest.Config
	var err error

	if kubeconfigPath != "" {
		cfg, err = clientcmd.BuildConfigFromFlags("", kubeconfigPath)
		if err != nil {
			return nil, fmt.Errorf("error building kubeconfig from path %s: %w", kubeconfigPath, err)
		}
		klog.Infof("Using Kubeconfig: %s", kubeconfigPath)
	} else {
		cfg, err = rest.InClusterConfig()
		if err != nil {
			return nil, fmt.Errorf("error building in-cluster kubeconfig: %w", err)
		}
		klog.Info("Using In-Cluster Kubeconfig")
	}

	// Apply QPS/Burst settings
	cfg.QPS = float32(qps)
	cfg.Burst = burst
	klog.Infof("KubeClient QPS: %f, Burst: %d", cfg.QPS, cfg.Burst)
	return cfg, nil
}

// NewPVCController creates a new controller instance
func NewPVCController(config *PVCControllerConfig) (*PVCController, error) {
	kubeconfig, err := buildConfig(config.KubeConfigPath, config.KubeAPIQPS, config.KubeAPIBurst)
	if err != nil {
		return nil, fmt.Errorf("failed to build kubeconfig: %w", err)
	}

	kubeClient, err := kubernetes.NewForConfig(kubeconfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create kubernetes client: %w", err)
	}

	dynamicClient, err := dynamic.NewForConfig(kubeconfig) // Initialize if using dynamic client
	if err != nil {
		return nil, fmt.Errorf("failed to create dynamic client: %w", err)
	}

	// Add GCPDataSource scheme (important for clients to recognize the type)
	// This might be necessary even if fetching directly, depending on client behavior.
	utilruntime.Must(gcpdatasourcesv1.AddToScheme(scheme.Scheme))

	// Use default rate limiter if none provided
	rateLimiter := config.RateLimiter
	if rateLimiter == nil {
		rateLimiter = workqueue.DefaultControllerRateLimiter()
		klog.Info("Using default workqueue rate limiter")
	}

	// Use ResyncPeriod for factory, default if zero
	resync := config.ResyncPeriod
	if resync == 0 {
		resync = 10 * time.Minute // Default resync period
		klog.Infof("Using default informer resync period: %v", resync)
	}

	factory := informers.NewSharedInformerFactory(kubeClient, resync)
	pvcInformer := factory.Core().V1().PersistentVolumeClaims()
	scInformer := factory.Storage().V1().StorageClasses()

	// --- GCPDataSource Informer Setup ---
	// Option 1 & 2: Requires setting up informer/lister for GCPDataSource
	// This can be complex without a dedicated typed informer factory for the CRD.
	// gcpDataSourceGVR := schema.GroupVersionResource{
	// 	Group:    gcpDataSourceAPIGroup,
	// 	Version:  gcpDataSourceVersion,
	// 	Resource: gcpDataSourceResource,
	// }
	// dynamicFactory := dynamicinformer.NewDynamicSharedInformerFactory(dynamicClient, resync)
	// gcpDsInformer := dynamicFactory.ForResource(gcpDataSourceGVR).Informer()
	// gcpDsLister := dynamicFactory.ForResource(gcpDataSourceGVR).Lister()

	// Option 3: We will fetch GCPDataSource directly in syncPVC, so no dedicated informer here.
	controller := &PVCController{
		kubeClient: kubeClient,
		// dynamicClient: dynamicClient, // If using dynamic client
		factory:       factory,
		dynamicClient: dynamicClient,
		pvcLister:     pvcInformer.Lister(),
		pvcSynced:     pvcInformer.Informer().HasSynced,
		// gcpDataSourceLister: gcpDsLister, // If using informer
		// gcpDataSourceSynced: gcpDsInformer.HasSynced, // If using informer
		queue:    workqueue.NewNamedRateLimitingQueue(rateLimiter, "PVCControllerQueue"),
		scLister: scInformer.Lister(),             // Initialize StorageClass Lister
		scSynced: scInformer.Informer().HasSynced, //
	}

	klog.Info("Setting up event handlers for PersistentVolumeClaims")
	pvcInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    controller.addPVC,
		DeleteFunc: controller.deletePVC, // Optional: Handle deletes if necessary (e.g., cleanup)
	})

	return controller, nil
}

// Run starts the controller worker loops
func (c *PVCController) Run(stopCh <-chan struct{}, numWorkers int) {
	defer utilruntime.HandleCrash()
	defer c.queue.ShutDown()

	klog.Infof("Starting %s", ControllerName)
	defer klog.Infof("Shutting down %s", ControllerName)

	// Start informers
	c.factory.Start(stopCh)
	// c.dynamicFactory.Start(stopCh) // If using dynamic factory

	// Wait for caches to sync
	klog.Info("Waiting for informer caches to sync")
	// Add gcpDataSourceSynced here if using an informer for it
	if !cache.WaitForCacheSync(stopCh, c.pvcSynced, c.scSynced) {
		klog.Fatal("Failed to wait for PVC cache to sync")
	}
	klog.Info("Informer caches synced")

	// Start workers
	klog.Infof("Starting %d workers", numWorkers)
	for i := 0; i < numWorkers; i++ {
		go wait.Until(c.runWorker, time.Second, stopCh)
	}
	klog.Infof("%s started successfully", ControllerName)

	<-stopCh // Block until stop signal is received
}

// runWorker continuously processes items from the workqueue.
func (c *PVCController) runWorker() {
	for c.processNextWorkItem() {
	}
	klog.Info("Worker shutting down")
}

// processNextWorkItem retrieves and processes an item from the workqueue.
func (c *PVCController) processNextWorkItem() bool {
	key, quit := c.queue.Get()
	if quit {
		klog.Info("Workqueue is shutting down, stopping worker.")
		return false // Stop processing
	}
	defer c.queue.Done(key) // Mark item as done processing

	klog.V(4).Infof("Processing item from queue: %v", key)
	err := c.syncPVC(key.(string)) // Assuming key is string "namespace/name"
	if err == nil {
		klog.V(4).Infof("Successfully processed item: %v", key)
		c.queue.Forget(key) // Remove item from queue on success
	} else if apierrors.IsNotFound(err) {
		// Don't requeue if the source PVC or GCPDataSource is not found after initial checks
		klog.V(3).Infof("Not requeuing item %v because a required resource was not found: %v", key, err)
		c.queue.Forget(key)
	} else {
		// An error occurred, requeue the item with rate limiting
		klog.Errorf("Error processing item %v: %v. Requeuing.", key, err)
		c.queue.AddRateLimited(key) // Requeue with backoff
	}

	return true // Continue processing
}

// syncPVC is the core logic for processing a PVC event.
func (c *PVCController) syncPVC(key string) error {
	startTime := time.Now()
	klog.V(2).Infof("Started syncing PVC %q (%v)", key, startTime)
	defer func() {
		klog.V(2).Infof("Finished syncing PVC %q (%v)", key, time.Since(startTime))
	}()

	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("invalid resource key: %s", key))
		return nil // Don't requeue invalid keys
	}

	// 1. Get the PVC from the lister
	pvc, err := c.pvcLister.PersistentVolumeClaims(namespace).Get(name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			klog.V(3).Infof("PVC %q has been deleted (not found in lister), nothing to do.", key)
			return nil // Don't requeue
		}
		return fmt.Errorf("failed to get PVC %q from lister: %w", key, err)
	}

	// Handle terminating PVCs (e.g., if using finalizers later)
	if pvc.DeletionTimestamp != nil {
		klog.V(4).Infof("PVC %q is terminating.", key)
		return nil
	}

	// 2. Check if the PVC references a GCPDataSource
	if !isGCPDataSourceRef(pvc.Spec.DataSourceRef) {
		klog.V(4).Infof("PVC %q does not reference a GCPDataSource, skipping.", key)
		return nil
	}
	dataSourceName := pvc.Spec.DataSourceRef.Name

	// 3. Check if the PVC is already Bound or has a VolumeName
	if pvc.Status.Phase == corev1.ClaimBound || pvc.Spec.VolumeName != "" {
		klog.V(3).Infof("PVC %q is already bound or has a volume assigned, skipping PV creation.", key)
		return nil
	}

	// 4. Check if the PVC is in Pending phase
	if pvc.Status.Phase != corev1.ClaimPending {
		klog.V(3).Infof("PVC %q is not in Pending phase (%s), skipping PV creation.", key, pvc.Status.Phase)
		return nil
	}

	// === 5. Check the Storage Class ===
	storageClassName := pvc.Spec.StorageClassName
	if storageClassName == nil || *storageClassName == "" {
		klog.Warningf("PVC %q is Pending but has no storageClassName defined. Cannot proceed.", key)
		// Record event?
		return nil // Don't requeue, PVC needs fixing.
	}

	sc, err := c.scLister.Get(*storageClassName)
	if err != nil {
		if apierrors.IsNotFound(err) {
			klog.Warningf("StorageClass %q specified by PVC %q not found.", *storageClassName, key)
			// Return specific error to avoid requeue by processNextWorkItem
			return fmt.Errorf("storageClass %q not found: %w", *storageClassName, err)
		}
		// Other error fetching StorageClass
		return fmt.Errorf("failed to get StorageClass %q for PVC %q: %w", *storageClassName, key, err)
	}

	// Check if the provisioner matches the GCS Fuse CSI driver
	if sc.Provisioner != csiDriverNameGCSFuse {
		klog.V(3).Infof("PVC %q uses StorageClass %q, but its provisioner (%q) is not %q. Skipping.",
			key, *storageClassName, sc.Provisioner, csiDriverNameGCSFuse)
		return nil // Not an error, just not relevant for this controller
	}
	klog.V(4).Infof("PVC %q uses relevant StorageClass %q with provisioner %q.", key, *storageClassName, sc.Provisioner)
	// === End Storage Class Check ===

	pvNameForCreateCheck := generatePVName(pvc)
	pvVolumeAttributes := map[string]string{
		attrSkipCSIBucketAccessCheck: "false", // Default to "false"
		attrPrefetchOnMount:          "false", // Default to "false"
		attrStorageClassName:         *storageClassName,
		attrOrigPVName:               pvNameForCreateCheck,
	}

	// Check for the csiDriverConfig parameter in StorageClass.Parameters
	if sc.Parameters != nil { // Ensure Parameters map exists
		if csiConfigStr, ok := sc.Parameters[csiDriverConfigParameterKey]; ok && csiConfigStr != "" {
			klog.V(4).Infof("PVC %q: Found %q in StorageClass %q: %s", key, csiDriverConfigParameterKey, *storageClassName, csiConfigStr)
			// Parse the comma-separated key=value pairs
			pairs := strings.Split(csiConfigStr, ",")
			for _, pair := range pairs {
				kv := strings.SplitN(strings.TrimSpace(pair), "=", 2) // Split only on the first "=" and trim spaces
				if len(kv) == 2 {
					paramKey := strings.TrimSpace(kv[0])
					paramValue := strings.TrimSpace(kv[1])

					// Check if the parsed key is one of the ones we care about
					// and override the default if found.
					if paramKey == attrSkipCSIBucketAccessCheck {
						pvVolumeAttributes[attrSkipCSIBucketAccessCheck] = paramValue
					} else if paramKey == attrPrefetchOnMount {
						pvVolumeAttributes[attrPrefetchOnMount] = paramValue
					}
					// Add other parameters here if needed in the future
				} else if strings.TrimSpace(pair) != "" { // Avoid logging for empty strings due to trailing commas
					klog.Warningf("PVC %q: Malformed pair in %s for StorageClass %q: %s", key, csiDriverConfigParameterKey, *storageClassName, pair)
				}
			}
		} else {
			klog.V(4).Infof("PVC %q: No %q parameter found in StorageClass %q or it's empty. Using default volume attributes.", key, csiDriverConfigParameterKey, *storageClassName)
		}
	} else {
		klog.V(4).Infof("PVC %q: StorageClass %q has no parameters defined. Using default volume attributes.", key, *storageClassName)
	}
	klog.Infof("PVC %q: Effective PV VolumeAttributes to be used: %v", key, pvVolumeAttributes)
	// === End Storage Class Parameter Parsing ===

	// 6. Check if PV already exists (basic check by generated name)
	_, err = c.kubeClient.CoreV1().PersistentVolumes().Get(context.TODO(), pvNameForCreateCheck, metav1.GetOptions{})
	if err == nil {
		klog.V(3).Infof("PV %q (expected name) for PVC %q already exists.", pvNameForCreateCheck, key)
		return nil // Don't try to create again
	}
	if !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to check for existing PV %q: %w", pvNameForCreateCheck, err)
	}
	// PV does not exist, proceed.
	klog.V(4).Infof("Fetching GCPDataSource %s/%s using dynamic client", namespace, dataSourceName)
	gcpDataSourceGVR := schema.GroupVersionResource{
		Group:    gcpDataSourceAPIGroup,
		Version:  gcpDataSourceVersion,
		Resource: gcpDataSourceResource, // Use the plural resource name
	}

	unstructuredGcpDs, err := c.dynamicClient.Resource(gcpDataSourceGVR).Namespace(namespace).Get(context.TODO(), dataSourceName, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			klog.Warningf("Referenced GCPDataSource %s/%s not found for PVC %q via dynamic client.", namespace, dataSourceName, key)
			// Wrap error to preserve NotFound status for processNextWorkItem logic
			return fmt.Errorf("referenced GCPDataSource %s/%s not found: %w", namespace, dataSourceName, err)
		}
		return fmt.Errorf("failed to get referenced GCPDataSource %s/%s for PVC %q using dynamic client: %w", namespace, dataSourceName, key, err)
	}
	klog.V(4).Infof("Successfully fetched unstructured GCPDataSource %s/%s", namespace, dataSourceName)

	// Convert unstructured object to typed GCPDataSource struct
	var gcpDataSource gcpdatasourcesv1.GCPDataSource // Use the imported type alias
	err = runtime.DefaultUnstructuredConverter.FromUnstructured(unstructuredGcpDs.UnstructuredContent(), &gcpDataSource)
	if err != nil {
		return fmt.Errorf("failed to convert unstructured GCPDataSource %s/%s to typed struct for PVC %q: %w", namespace, dataSourceName, key, err)
	}
	// --- End Dynamic Fetch and Conversion ---

	// 8. Validate GCPDataSource spec
	if gcpDataSource.Spec.CloudStorage.URI == "" {
		klog.Warningf("Referenced GCPDataSource %s/%s for PVC %q is missing spec.cloudStorage.uri.", namespace, dataSourceName, key)
		return nil
	}
	gcsURI := strings.TrimSuffix(gcpDataSource.Spec.CloudStorage.URI, "/")
	volhandle := strings.TrimPrefix(gcsURI, "gs://")
	// 9. Construct the PersistentVolume
	pvNameToCreate := generatePVName(pvc) // Use generated name for creation
	klog.Infof("Attempting to create PersistentVolume %q for PVC %q (StorageClass: %s, GCS URI: %s)",
		pvNameToCreate, key, *storageClassName, gcsURI)

	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: pvNameToCreate,
			Labels: map[string]string{
				"created-by": ControllerName,
			},
		},
		Spec: corev1.PersistentVolumeSpec{
			Capacity:                      corev1.ResourceList{corev1.ResourceStorage: pvc.Spec.Resources.Requests[corev1.ResourceStorage]},
			VolumeMode:                    &[]corev1.PersistentVolumeMode{corev1.PersistentVolumeFilesystem}[0],
			AccessModes:                   pvc.Spec.AccessModes,
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain,
			StorageClassName:              *storageClassName, // Set storage class name on PV
			ClaimRef: &corev1.ObjectReference{
				Namespace: pvc.Namespace,
				Name:      pvc.Name,
				UID:       pvc.UID,
			},
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{
					Driver:           csiDriverNameGCSFuse,
					VolumeHandle:     volhandle,
					VolumeAttributes: pvVolumeAttributes,
				},
			},
		},
	}

	// 10. Create the PersistentVolume
	_, err = c.kubeClient.CoreV1().PersistentVolumes().Create(context.TODO(), pv, metav1.CreateOptions{})
	if err != nil {
		if apierrors.IsAlreadyExists(err) {
			klog.Infof("PersistentVolume %q already exists, likely created concurrently.", pvNameToCreate)
			return nil
		}
		return fmt.Errorf("failed to create PersistentVolume %q for PVC %q: %w", pvNameToCreate, key, err)
	}

	klog.Infof("Successfully created PersistentVolume %q for PVC %q", pvNameToCreate, key)
	return nil // Success
}

// --- Event Handlers ---

func (c *PVCController) addPVC(obj interface{}) {
	pvc, ok := obj.(*corev1.PersistentVolumeClaim)
	if !ok {
		klog.Errorf("AddFunc: Expected PVC but got %T", obj)
		return
	}
	// Enqueue only if relevant and pending/unbound
	// The sync loop will perform the detailed SC check
	if isGCPDataSourceRef(pvc.Spec.DataSourceRef) && pvc.Status.Phase == corev1.ClaimPending && pvc.Spec.VolumeName == "" {
		klog.V(4).Infof("PVC ADDED %s/%s (Relevant and Pending)", pvc.Namespace, pvc.Name)
		c.enqueuePVC(pvc)
	} else {
		klog.V(5).Infof("PVC ADDED %s/%s (Irrelevant or Not Pending/Bound)", pvc.Namespace, pvc.Name)
	}
}

func (c *PVCController) updatePVC(oldObj, newObj interface{}) {
	oldPVC, ok := oldObj.(*corev1.PersistentVolumeClaim)
	if !ok {
		klog.Errorf("UpdateFunc: Expected old PVC but got %T", oldObj)
		return
	}
	newPVC, ok := newObj.(*corev1.PersistentVolumeClaim)
	if !ok {
		klog.Errorf("UpdateFunc: Expected new PVC but got %T", newObj)
		return
	}

	// Enqueue only if the *new* PVC is relevant and pending/unbound.
	// Check resource version to avoid no-op updates.
	// The sync loop will perform the detailed SC check
	if newPVC.ResourceVersion != oldPVC.ResourceVersion &&
		isGCPDataSourceRef(newPVC.Spec.DataSourceRef) &&
		newPVC.Status.Phase == corev1.ClaimPending && // Process only if pending
		newPVC.Spec.VolumeName == "" {
		klog.V(4).Infof("PVC UPDATED %s/%s (Relevant and Pending)", newPVC.Namespace, newPVC.Name)
		c.enqueuePVC(newPVC)
	} else {
		klog.V(5).Infof("PVC UPDATED %s/%s (Irrelevant or Not Pending/Already Assigned)", newPVC.Namespace, newPVC.Name)
	}
}

// deletePVC handles cleanup when a relevant PVC is deleted.
func (c *PVCController) deletePVC(obj interface{}) {
	pvc, ok := obj.(*corev1.PersistentVolumeClaim)
	if !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			klog.Errorf("DeleteFunc: Expected PVC or Tombstone but got %T", obj)
			return
		}
		pvc, ok = tombstone.Obj.(*corev1.PersistentVolumeClaim)
		if !ok {
			klog.Errorf("DeleteFunc: Expected PVC in Tombstone but got %T", tombstone.Obj)
			return
		}
		klog.V(4).Infof("PVC TOMBSTONE %s/%s received", pvc.Namespace, pvc.Name)
	} else {
		klog.V(4).Infof("PVC DELETED %s/%s received", pvc.Namespace, pvc.Name)
	}

	// 1. Check if the deleted PVC was relevant to this controller
	if !isGCPDataSourceRef(pvc.Spec.DataSourceRef) {
		klog.V(5).Infof("Deleted PVC %s/%s did not reference GCPDataSource, skipping PV cleanup.", pvc.Namespace, pvc.Name)
		return
	}

	// *** No need to check StorageClass here for deletion ***
	// We decide whether to delete based on whether the PVC was relevant (GCPDataSourceRef)
	// and what PV it was bound to (spec.volumeName). The original StorageClass
	// doesn't affect the decision to clean up the bound PV.

	// 2. Get the PV name from the PVC's spec (indicates the bound volume)
	pvName := pvc.Spec.VolumeName
	if pvName == "" {
		klog.Infof("Relevant PVC %s/%s deleted, but spec.volumeName is empty. No specific PV to delete.", pvc.Namespace, pvc.Name)
		return
	}

	klog.Infof("Relevant PVC %s/%s deleted. Attempting to delete corresponding bound PV %q (from spec.volumeName)", pvc.Namespace, pvc.Name, pvName)

	// 3. Attempt to delete the PV identified by spec.volumeName
	err := c.kubeClient.CoreV1().PersistentVolumes().Delete(context.TODO(), pvName, metav1.DeleteOptions{})
	if err != nil {
		// If the PV is already gone, that's okay.
		if apierrors.IsNotFound(err) {
			klog.Infof("PV %q (from spec.volumeName) for deleted PVC %s/%s not found, likely already deleted.", pvName, pvc.Namespace, pvc.Name)
			return // Successfully cleaned up (or was already gone)
		}
		// Log other errors but don't block or retry indefinitely here.
		utilruntime.HandleError(fmt.Errorf("failed to delete PV %q (from spec.volumeName) for deleted PVC %s/%s: %w", pvName, pvc.Namespace, pvc.Name, err))
		return
	}

	klog.Infof("Successfully deleted PV %q (from spec.volumeName) for deleted PVC %s/%s", pvName, pvc.Namespace, pvc.Name)
}

// enqueuePVC adds a PVC's key to the workqueue.
func (c *PVCController) enqueuePVC(pvc *corev1.PersistentVolumeClaim) {
	key, err := cache.MetaNamespaceKeyFunc(pvc)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for object %#v: %w", pvc, err))
		return
	}
	klog.V(4).Infof("Enqueuing PVC %q for processing", key)
	c.queue.Add(key)
}

// --- Helper Functions ---

// isGCPDataSourceRef checks if the DataSourceRef points to a GCPDataSource
func isGCPDataSourceRef(ref *corev1.TypedObjectReference) bool {
	return ref != nil &&
		ref.APIGroup != nil &&
		*ref.APIGroup == gcpDataSourceAPIGroup &&
		ref.Kind == gcpDataSourceKind &&
		ref.Name != ""
}

// generatePVName creates a deterministic name for the PV based on the PVC UID.
func generatePVName(pvc *corev1.PersistentVolumeClaim) string {
	return fmt.Sprintf("pv-%s", pvc.UID)
}
