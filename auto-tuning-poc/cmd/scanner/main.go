package main

import (
	"flag"
	"time"

	"github.com/go-logr/logr"
	"github.com/googlecloudplatform/gcs-fuse-csi-driver/pkg/scanner"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

var (
	kubeconfigPath     = flag.String("kubeconfig-path", "", "kubeconfig path")
	kubeAPIQPS         = flag.Float64("kube-api-qps", 5, "QPS to use while communicating with the kubernetes apiserver. Defaults to 5.0.")
	kubeAPIBurst       = flag.Int("kube-api-burst", 10, "Burst to use while communicating with the kubernetes apiserver. Defaults to 10.")
	resyncPeriod       = flag.Duration("resync-period", 15*time.Minute, "Resync interval of the controller")
	retryIntervalStart = flag.Duration("retry-interval-start", time.Second, "Initial retry interval of failed create volume or deletion. It doubles with each failure, up to retry-interval-max.")
	retryIntervalMax   = flag.Duration("retry-interval-max", 5*time.Minute, "Maximum retry interval of failed create volume or deletion.")
	// This is set at compile time
	version = "unknown"
)

func main() {
	klog.InitFlags(nil)
	flag.Parse()
	log.SetLogger(logr.New(log.NullLogSink{}))
	klog.Infof("Running Bucket scanner %v", version)
	sc, err := scanner.NewScanner(&scanner.ScannerConfig{
		KubeAPIQPS:     *kubeAPIQPS,
		KubeAPIBurst:   *kubeAPIBurst,
		ResyncPeriod:   *resyncPeriod,
		KubeConfigPath: *kubeconfigPath,
		RateLimiter:    workqueue.NewItemExponentialFailureRateLimiter(*retryIntervalStart, *retryIntervalMax),
	})
	if err != nil {
		klog.Fatalf("failed to start scanner: %v", err)
	}
	sc.Run()
}
