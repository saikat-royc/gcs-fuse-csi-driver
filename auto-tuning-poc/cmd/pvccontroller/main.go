package main

import (
	"flag"
	"os"
	"os/signal"
	"syscall"
	"time"

	// Import your controller package
	pvccontroller "github.com/googlecloudplatform/gcs-fuse-csi-driver/pkg/pvccontroller" // ADJUST THIS IMPORT PATH

	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
)

var (
	kubeconfig   string
	workers      int
	resyncPeriod time.Duration
	qps          float64
	burst        int
)

func init() {
	// Standard flags
	flag.StringVar(&kubeconfig, "kubeconfig", "", "Path to a kubeconfig. Only required if out-of-cluster.")
	flag.IntVar(&workers, "workers", 2, "Number of worker threads processing PVC events.")
	flag.DurationVar(&resyncPeriod, "resync-period", 10*time.Minute, "Informer resync period.")
	flag.Float64Var(&qps, "kube-api-qps", 20.0, "Maximum QPS to the API server.")
	flag.IntVar(&burst, "kube-api-burst", 30, "Maximum burst for throttle.")

	// Init klog - IMPORTANT: Call this before flag.Parse()
	klog.InitFlags(nil)
}

func main() {
	// Parse flags after defining them and initializing klog
	flag.Parse()

	// Always log logs to stderr by default (?)
	// klog.SetOutput(os.Stderr) // Might not be needed depending on klog version/defaults

	klog.Info("Starting GCPDataSource PVC Controller")
	klog.Infof("Worker threads: %d", workers)
	klog.Infof("Resync period: %v", resyncPeriod)
	klog.Infof("API Server QPS: %f, Burst: %d", qps, burst)

	// --- Setup signal handler ---
	stopCh := make(chan struct{})
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		klog.Infof("Received signal %v, shutting down...", sig)
		close(stopCh) // Close stopCh to signal termination
	}()

	// --- Create Controller Config ---
	// Using default rate limiter here, but could be made configurable
	config := &pvccontroller.PVCControllerConfig{
		KubeConfigPath: kubeconfig,
		KubeAPIQPS:     qps,
		KubeAPIBurst:   burst,
		ResyncPeriod:   resyncPeriod,
		RateLimiter:    workqueue.DefaultControllerRateLimiter(), // Default rate limiter
		NumWorkers:     workers,
	}

	// --- Create and Run Controller ---
	pvcController, err := pvccontroller.NewPVCController(config)
	if err != nil {
		klog.Fatalf("Error creating controller: %v", err)
	}

	// Run the controller until stopCh is closed
	pvcController.Run(stopCh, workers)

	klog.Info("GCPDataSource PVC Controller shut down gracefully")
}
