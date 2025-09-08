package scanner

import (
	"context"
	"encoding/json"
	"fmt"

	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"sync"
	"time"

	"io"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	corev1listers "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
)

const (
	Project                  = "saikatroyc-stateful-joonix"
	BucketInfoVolumeBasePath = "/bucket-info-vol"
	ScanBucketPyScriptPath   = "/dataflux-client-python/scan-bucket.py"

	RelevantCSIDriverName = "gcsfuse.csi.storage.gke.io"
	PeriodicScanInterval  = 12 * time.Hour
	AnnotationPrefix      = "gke-gcsfuse"
	AnnotationNumObjects  = AnnotationPrefix + "/num-objects"
	AnnotationTotalSize   = AnnotationPrefix + "/total-size-bytes"
	AnnotationLastScan    = AnnotationPrefix + "/last-scan-timestamp"
	AnnotationHNSEnabled  = AnnotationPrefix + "/hns-enabled"

	ScriptTimeout = 10 * time.Minute
)

// Updated BucketInfo to include TotalSize
type BucketInfo struct {
	NumObjects       int64 `json:"num_objects"`
	MedianObjectSize int64 `json:"median_object_size_bytes"` // Note: Median size isn't used for annotations per reqs
	TotalSizeBytes   int64 `json:"total_size_bytes"`         // Added field
	IsHNSEnabled     bool  `json:"hns_enabled"`
}

type ScannerConfig struct {
	KubeAPIQPS     float64
	KubeAPIBurst   int
	ResyncPeriod   time.Duration
	KubeConfigPath string
	RateLimiter    workqueue.RateLimiter // Use default if nil
}

type Scanner struct {
	kubeClient kubernetes.Interface
	pvLister   corev1listers.PersistentVolumeLister
	pvSynced   cache.InformerSynced
	factory    informers.SharedInformerFactory
	queue      workqueue.RateLimitingInterface

	// Track relevant PV names for periodic scanning
	trackedPVs map[string]struct{}
	pvMutex    sync.RWMutex // Protects access to trackedPVs
}

func buildConfig(kubeconfigPath string) (*rest.Config, error) {
	if kubeconfigPath != "" {
		cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfigPath)
		if err != nil {
			return nil, fmt.Errorf("error building kubeconfig from path %s: %w", kubeconfigPath, err)
		}
		klog.Infof("Using Kubeconfig: %s", kubeconfigPath)
		return cfg, nil
	}
	cfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("error building in-cluster kubeconfig: %w", err)
	}
	klog.Info("Using In-Cluster Kubeconfig")
	return cfg, nil
}

func NewScanner(config *ScannerConfig) (*Scanner, error) {
	kubeconfig, err := buildConfig(config.KubeConfigPath)
	if err != nil {
		return nil, fmt.Errorf("failed to build kubeconfig: %w", err)
	}

	// Apply QPS/Burst settings
	kubeconfig.QPS = float32(config.KubeAPIQPS) // Note conversion
	kubeconfig.Burst = config.KubeAPIBurst
	klog.Infof("KubeClient QPS: %f, Burst: %d", kubeconfig.QPS, kubeconfig.Burst)

	kubeClient, err := kubernetes.NewForConfig(kubeconfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create kubernetes client: %w", err)
	}

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
	pvInformer := factory.Core().V1().PersistentVolumes()

	scanner := &Scanner{
		kubeClient: kubeClient,
		factory:    factory,
		pvLister:   pvInformer.Lister(),
		pvSynced:   pvInformer.Informer().HasSynced,
		queue:      workqueue.NewNamedRateLimitingQueue(rateLimiter, "PVScannerQueue"),
		trackedPVs: make(map[string]struct{}),
	}

	klog.Info("Setting up event handlers for PersistentVolumes")
	pvInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    scanner.addPV,
		DeleteFunc: scanner.deletePV,
	})

	// Start the factory in the Run method, not here

	return scanner, nil
}

func (s *Scanner) Run() {
	stopCh := make(chan struct{})
	defer close(stopCh)
	run := func(context.Context, chan struct{}) {
		// run...
		s.factory.Start(stopCh)
		if !cache.WaitForCacheSync(stopCh, s.pvSynced) {
			klog.Fatal("Failed to wait for informers cache to sync")
		}
		klog.Info("Cache sycned successfully")
		go wait.Until(s.runWorker, time.Second, stopCh)
	}
	go run(context.TODO(), stopCh)

	// Start the periodic enqueuer
	klog.Infof("Starting periodic scan enqueuer (Interval: %v)", PeriodicScanInterval)
	go wait.Until(s.enqueueTrackedPVs, PeriodicScanInterval, stopCh)

	klog.Info("Scanner controller started successfully. Waiting for stop signal...")
	// ...until SIGINT
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)
	<-sigChan
}

// runWorker continuously processes items from the workqueue.
func (s *Scanner) runWorker() {
	for s.processNextWorkItem() {
	}
	klog.Info("Worker shutting down")
}

// processNextWorkItem retrieves and processes an item from the workqueue.
func (s *Scanner) processNextWorkItem() bool {
	key, quit := s.queue.Get()
	if quit {
		klog.Info("Workqueue is shutting down, stopping worker.")
		return false // Stop processing
	}
	defer s.queue.Done(key) // Mark item as done processing

	klog.V(4).Infof("Processing item from queue: %v", key)
	err := s.syncPV(key.(string))
	if err == nil {
		klog.V(4).Infof("Successfully processed item: %v", key)
		s.queue.Forget(key) // Remove item from queue on success
	} else {
		// An error occurred, requeue the item with rate limiting
		klog.Errorf("Error processing item %v: %v. Requeuing.", key, err)
		s.queue.AddRateLimited(key) // Requeue with backoff
	}

	return true // Continue processing
}

// syncPV is the core logic for processing a PV based on its key.
func (s *Scanner) syncPV(key string) error {
	startTime := time.Now()
	klog.V(2).Infof("Started syncing PV %q (%v)", key, startTime)
	defer func() {
		klog.V(2).Infof("Finished syncing PV %q (%v)", key, time.Since(startTime))
	}()

	// PVs are cluster-scoped, key is just the name
	pvName := key
	pv, err := s.pvLister.Get(pvName)
	if err != nil {
		if apierrors.IsNotFound(err) {
			klog.V(3).Infof("PV %q has been deleted (not found in lister), nothing to do.", key)
			// Should have been removed from trackedPVs by deletePV handler
			return nil // Don't requeue
		}
		return fmt.Errorf("failed to get PV %q from lister: %w", key, err)
	}

	// Check if this PV is still relevant *now*
	isRelevant, bucketName := s.checkPVRelevance(pv)
	if !isRelevant {
		klog.V(4).Infof("PV %q is no longer relevant, skipping sync.", key)
		// Ensure it's removed from tracking if it somehow got enqueued while irrelevant
		s.pvMutex.Lock()
		delete(s.trackedPVs, pv.Name)
		s.pvMutex.Unlock()
		return nil // Don't requeue
	}
	klog.V(4).Infof("PV %q is relevant, bucket: %s", key, bucketName)

	// ----- Run the external script -----
	klog.V(3).Infof("Running scan script for bucket %s (PV: %s)", bucketName, pv.Name)
	// ctx, cancel := context.WithTimeout(context.Background(), ScriptTimeout)
	// defer cancel()

	////////////
	fileName := BucketInfoVolumeBasePath + "/" + bucketName + ".json"
	klog.Infof("Remove (if any) filename %s", fileName)
	os.Remove(fileName)
	err = runCommand(Project, bucketName, fileName)
	if err != nil {
		return err
	}
	bucketInfo, err := readBucketInfoFromFile(fileName)
	if err != nil {
		return err
	}
	klog.V(3).Infof("Scan script successful for bucket %s (PV: %s). Objects: %d, Size(bytes): %d", bucketName, pv.Name, bucketInfo.NumObjects, bucketInfo.TotalSizeBytes)

	// ----- Update PV Annotations -----
	klog.V(3).Infof("Updating annotations for PV %s", pv.Name)
	err = s.updatePVAnnotations(context.Background(), pv, bucketInfo) // Use fresh context
	if err != nil {
		return fmt.Errorf("failed to update annotations for PV %s: %w", pv.Name, err)
	}

	klog.Infof("Successfully synced PV %s (Bucket: %s, Objects: %d, Size: %d)", pv.Name, bucketName, bucketInfo.NumObjects, bucketInfo.TotalSizeBytes)
	return nil // Success
}

// --- Event Handlers ---

func (s *Scanner) addPV(obj interface{}) {
	pv, ok := obj.(*v1.PersistentVolume)
	if !ok {
		klog.Errorf("AddFunc: Expected PersistentVolume but got %T", obj)
		return
	}

	isRelevant, _ := s.checkPVRelevance(pv)
	klog.V(4).Infof("PV ADDED: %s (Relevant: %t)", pv.Name, isRelevant)

	if isRelevant {
		s.pvMutex.Lock()
		s.trackedPVs[pv.Name] = struct{}{} // Add to tracked list
		s.pvMutex.Unlock()
		s.enqueuePV(pv) // Enqueue for initial processing
	}
}

func (s *Scanner) deletePV(obj interface{}) {
	pv, ok := obj.(*v1.PersistentVolume)
	if !ok {
		// Handle case where object is deleted tombstone
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			klog.Errorf("DeleteFunc: Expected PV or Tombstone but got %T", obj)
			return
		}
		pv, ok = tombstone.Obj.(*v1.PersistentVolume)
		if !ok {
			klog.Errorf("DeleteFunc: Expected PV in Tombstone but got %T", tombstone.Obj)
			return
		}
		klog.V(4).Infof("PV TOMBSTONE: %s", pv.Name)
	} else {
		klog.V(4).Infof("PV DELETED: %s", pv.Name)
	}

	// Always try to remove from tracking on deletion
	s.pvMutex.Lock()
	if _, exists := s.trackedPVs[pv.Name]; exists {
		klog.V(3).Infof("PV %s deleted, removing from tracking.", pv.Name)
		delete(s.trackedPVs, pv.Name)
	}
	s.pvMutex.Unlock()

	// No need to enqueue the key on delete
}

// enqueuePV adds a PV's key to the workqueue.
func (s *Scanner) enqueuePV(pv *v1.PersistentVolume) {
	key, err := cache.MetaNamespaceKeyFunc(pv) // PVs are cluster-scoped, key is just name
	if err != nil {
		runtime.HandleError(fmt.Errorf("couldn't get key for object %#v: %w", pv, err))
		return
	}
	klog.V(4).Infof("Enqueuing PV %q for processing", key)
	s.queue.Add(key)
}

// --- Periodic Scanner ---

// enqueueTrackedPVs periodically adds all currently tracked PVs to the queue.
func (s *Scanner) enqueueTrackedPVs() {
	klog.V(2).Info("Periodic scan triggered: Enqueuing all tracked PVs.")
	s.pvMutex.RLock() // Use read lock
	count := 0
	for pvName := range s.trackedPVs {
		klog.V(4).Infof("Periodic scan: Enqueuing tracked PV %q", pvName)
		s.queue.Add(pvName) // Add PV name (key) directly
		count++
	}
	s.pvMutex.RUnlock()
	klog.V(2).Infof("Periodic scan: Enqueued %d tracked PVs.", count)
}

// --- Helper Functions ---

// checkPVRelevance checks if a PV is relevant for scanning and returns the bucket name if so.
func (s *Scanner) checkPVRelevance(pv *v1.PersistentVolume) (bool, string) {
	if pv == nil {
		return false, ""
	}
	if pv.Spec.CSI == nil || pv.Spec.CSI.Driver != RelevantCSIDriverName || pv.Spec.CSI.VolumeHandle == "" {
		return false, ""
	}

	bucketName := pv.Spec.CSI.VolumeHandle
	if bucketName == "" {
		klog.Warningf("PV %s has relevant CSI driver but failed to extract bucket from handle: %s", pv.Name, pv.Spec.CSI.VolumeHandle)
		return false, ""
	}

	return true, bucketName
}

// // runScanScript executes the python script and parses its JSON output.
// func (s *Scanner) runScanScript(ctx context.Context, bucketName string) (*BucketInfo, error) {
// 	cmd := exec.CommandContext(ctx, "python3", ScanBucketPyScriptPath, bucketName) // Assuming python3

// 	var stdout, stderr bytes.Buffer
// 	cmd.Stdout = &stdout
// 	cmd.Stderr = &stderr

// 	klog.V(4).Infof("Executing command: %s", cmd.String())
// 	err := cmd.Run() // This waits for the command to finish

// 	if err != nil {
// 		// Include stderr in the error message for better debugging
// 		stderrStr := strings.TrimSpace(stderr.String())
// 		if ctx.Err() == context.DeadlineExceeded {
// 			return nil, fmt.Errorf("script execution timed out (stderr: %s): %w", stderrStr, context.DeadlineExceeded)
// 		}
// 		return nil, fmt.Errorf("script execution failed with error: %w (stderr: %s)", err, stderrStr)
// 	}

// 	// Check stderr even on success, as some scripts might print warnings there
// 	stderrStr := strings.TrimSpace(stderr.String())
// 	if stderrStr != "" {
// 		klog.Warningf("Script for bucket %s produced stderr output: %s", bucketName, stderrStr)
// 	}

// 	// Parse stdout as JSON
// 	stdoutStr := strings.TrimSpace(stdout.String())
// 	klog.V(5).Infof("Script stdout for bucket %s: %s", bucketName, stdoutStr)

// 	var info BucketInfo
// 	if err := json.Unmarshal([]byte(stdoutStr), &info); err != nil {
// 		return nil, fmt.Errorf("failed to parse script JSON output: %w (output: %s)", err, stdoutStr)
// 	}

// 	// Basic validation (optional)
// 	if info.NumObjects < 0 || info.TotalSizeBytes < 0 {
// 		return nil, fmt.Errorf("script returned invalid data (negative values): %+v", info)
// 	}

// 	return &info, nil
// }

// updatePVAnnotations patches the PV's metadata with the latest scan results.
func (s *Scanner) updatePVAnnotations(ctx context.Context, pv *v1.PersistentVolume, info *BucketInfo) error {
	// Check context first
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("context cancelled before annotation update: %w", err)
	}

	numObjectsStr := strconv.FormatInt(info.NumObjects, 10)
	totalSizeStr := strconv.FormatInt(info.TotalSizeBytes, 10)
	timestampStr := time.Now().UTC().Format(time.RFC3339)
	hnsStatusStr := strconv.FormatBool(info.IsHNSEnabled)

	// Check if annotations already exist and match to avoid unnecessary updates
	currentAnnotations := pv.GetAnnotations()
	if currentAnnotations[AnnotationNumObjects] == numObjectsStr &&
		currentAnnotations[AnnotationTotalSize] == totalSizeStr {
		// Only update timestamp if other values haven't changed significantly?
		// For simplicity, let's always update timestamp if we ran the scan.
		// But maybe skip the whole patch if *nothing* changed including a recent timestamp?
		// Let's just always patch for now, k8s handles no-op patches efficiently.
		klog.V(4).Infof("Annotations for PV %s already up-to-date, updating timestamp only.", pv.Name)
	}

	patchData := map[string]interface{}{
		"metadata": map[string]interface{}{
			"annotations": map[string]string{
				AnnotationNumObjects: numObjectsStr,
				AnnotationTotalSize:  totalSizeStr,
				AnnotationLastScan:   timestampStr,
				AnnotationHNSEnabled: hnsStatusStr,
			},
		},
	}

	patchBytes, err := json.Marshal(patchData)
	if err != nil {
		return fmt.Errorf("failed to marshal annotation patch data for PV %s: %w", pv.Name, err)
	}

	klog.V(4).Infof("Patching PV %s with annotations: %s", pv.Name, string(patchBytes))
	_, err = s.kubeClient.CoreV1().PersistentVolumes().Patch(ctx, pv.Name, types.MergePatchType, patchBytes, metav1.PatchOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			klog.Warningf("Failed to patch PV %s because it was not found (likely deleted concurrently).", pv.Name)
			return nil // Treat as non-fatal in this context
		}
		return fmt.Errorf("failed to patch PV %s annotations: %w", pv.Name, err)
	}
	klog.V(3).Infof("Successfully patched annotations for PV %s", pv.Name)
	return nil
}

func runCommand(project, bucket, filename string) error {
	klog.Infof("DF Lister initiated for project %s, bucket %s", project, bucket)
	versionCmd := exec.Command("python", ScanBucketPyScriptPath,
		"--project", project,
		"--bucket", bucket,
		"--workers", "20",
		"--outputfile", filename)
	op, err := versionCmd.CombinedOutput()
	if err != nil {
		klog.Errorf("Error: %v", err)
		return err
	}
	klog.Infof("python run output : %v", string(op))
	return nil
}

func readBucketInfoFromFile(filename string) (*BucketInfo, error) {
	klog.Infof("Opening file %s to read ", filename)
	jsonFile, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer jsonFile.Close()
	bytes, err := io.ReadAll(jsonFile)
	if err != nil {
		return nil, err
	}
	klog.Infof("Starting unmarshall")
	var data BucketInfo
	err = json.Unmarshal(bytes, &data)
	if err != nil {
		klog.Errorf("unmarshall failed %v", err)
		return nil, err
	}
	klog.Infof("num objects: %d, median %d, total size: %d, hns enabled %v", data.NumObjects, data.MedianObjectSize, data.TotalSizeBytes, data.IsHNSEnabled)
	return &data, nil
}
