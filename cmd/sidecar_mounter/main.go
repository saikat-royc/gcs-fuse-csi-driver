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

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	credentials "cloud.google.com/go/iam/credentials/apiv1"
	"github.com/googlecloudplatform/gcs-fuse-csi-driver/pkg/cloud_provider/storage"
	sidecarmounter "github.com/googlecloudplatform/gcs-fuse-csi-driver/pkg/sidecar_mounter"
	"github.com/googlecloudplatform/gcs-fuse-csi-driver/pkg/webhook"
	"golang.org/x/oauth2"
	"google.golang.org/api/option"
	"google.golang.org/api/sts/v1"
	"k8s.io/klog/v2"
)

var (
	gcsfusePath    = flag.String("gcsfuse-path", "/gcsfuse", "gcsfuse path")
	volumeBasePath = flag.String("volume-base-path", webhook.SidecarContainerTmpVolumeMountPath+"/.volumes", "volume base path")
	_              = flag.Int("grace-period", 0, "grace period for gcsfuse termination. This flag has been deprecated, has no effect and will be removed in the future.")
	// This is set at compile time.
	version = "unknown"
	// tokenUrlSocketPath = "/gcsfuse-tmp/token.sock"
	tokenUrlSocketPath = "/gcsfuse-tmp/.volumes/gcs-fuse-csi-ephemeral/token.sock"
)

const (
	bucketName = "test-wi-host-network-1"
	objectName = "object-foo"
)

func main() {
	klog.InitFlags(nil)
	flag.Parse()

	klog.Infof("Running Google Cloud Storage FUSE CSI driver sidecar mounter version %v", version)
	socketPathPattern := *volumeBasePath + "/*/socket"
	socketPaths, err := filepath.Glob(socketPathPattern)
	if err != nil {
		klog.Fatalf("failed to look up socket paths: %v", err)
	}

	var stsToken *oauth2.Token
	mounter := sidecarmounter.New(*gcsfusePath)
	ctx, cancel := context.WithCancel(context.Background())
	k8stoken, err := getK8sToken("/gcsfuse-sa-token/token")
	if err != nil {
		klog.Errorf("failed to get k8s token from path %v", err)
	} else {
		klog.Infof("k8s token %s", k8stoken)
		stsToken, err = fetchIdentityBindingToken(ctx, k8stoken)
		if err != nil {
			klog.Errorf("failed to get sts token from path %v", err)
		} else {
			klog.Infof("sts token %s", stsToken.AccessToken)
		}
	}

	// smoke test bucket access with the token
	tm := sidecarmounter.NewTokenManager()
	ts := tm.GetTokenSource(stsToken)
	sm, err := storage.NewGCSServiceManager()
	if err != nil {
		klog.Fatalf("Failed to set up storage service manager: %v", err)
	}
	storageService, err := sm.SetupService(ctx, ts)
	if err != nil {
		klog.Fatalf("Failed to set up storage service: %v", err)
	}

	if exist, err := storageService.CheckBucketExists(ctx, &storage.ServiceBucket{Name: bucketName}); !exist {
		klog.Errorf("failed to get GCS bucket %q: %v", bucketName, err)
	}
	klog.Infof("bucket check success for %s", bucketName)

	rand.Seed(time.Now().UnixNano())
	randomNumber := rand.Intn(1000) + 1
	content := fmt.Sprintf("content: %d", randomNumber)
	if err = storageService.UploadObject(ctx, bucketName, objectName, content); err != nil {
		klog.Errorf("failed to write object %s bucket %s, err: %v", objectName, bucketName, err)
	}
	klog.Infof("object %s with content %q uploaded to bucket %s success", objectName, content, bucketName)

	go startTokenServer(ctx)

	for _, sp := range socketPaths {
		// sleep 1.5 seconds before launch the next gcsfuse to avoid
		// 1. different gcsfuse logs mixed together.
		// 2. memory usage peak.
		time.Sleep(1500 * time.Millisecond)
		mc := sidecarmounter.NewMountConfig(sp)
		if mc != nil {
			if err := mounter.Mount(ctx, mc); err != nil {
				mc.ErrWriter.WriteMsg(fmt.Sprintf("failed to mount bucket %q for volume %q: %v\n", mc.BucketName, mc.VolumeName, err))
			}
		}
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGTERM)
	klog.Info("waiting for SIGTERM signal...")

	// Function that monitors the exit file used in regular sidecar containers.
	monitorExitFile := func() {
		ticker := time.NewTicker(5 * time.Second)
		for {
			<-ticker.C
			// If exit file is detected, send a syscall.SIGTERM signal to the signal channel.
			if _, err := os.Stat(*volumeBasePath + "/exit"); err == nil {
				klog.Info("all the other containers terminated in the Pod, exiting the sidecar container.")

				// After the Kubernetes native sidecar container feature is adopted,
				// we should propagate the SIGTERM signal outside of this goroutine.
				cancel()

				if err := os.Remove(*volumeBasePath + "/exit"); err != nil {
					klog.Error("failed to remove the exit file from emptyDir.")
				}

				c <- syscall.SIGTERM

				return
			}
		}
	}

	envVar := os.Getenv("NATIVE_SIDECAR")
	isNativeSidecar, err := strconv.ParseBool(envVar)
	if envVar != "" && err != nil {
		klog.Warningf(`env variable "%s" could not be converted to boolean`, envVar)
	}
	// When the pod contains a regular container, we monitor for the exit file.
	if !isNativeSidecar {
		go monitorExitFile()
	}

	<-c // blocking the process
	klog.Info("received SIGTERM signal, waiting for all the gcsfuse processes exit...")

	if isNativeSidecar {
		cancel()
	}

	mounter.WaitGroup.Wait()

	klog.Info("exiting sidecar mounter...")
}

func getK8sToken(tokenPath string) (string, error) {
	token, err := ioutil.ReadFile(tokenPath)
	if err != nil {
		return "", fmt.Errorf("error reading token file: %w", err)
	}
	return strings.TrimSpace(string(token)), nil
}

func fetchIdentityBindingToken(ctx context.Context, k8sSAToken string) (*oauth2.Token, error) {
	stsService, err := sts.NewService(ctx, option.WithHTTPClient(&http.Client{}))
	if err != nil {
		return nil, fmt.Errorf("new STS service error: %w", err)
	}

	audience := fmt.Sprintf(
		"identitynamespace:%s:%s",
		"saikatroyc-stateful-joonix.svc.id.goog", // identity pool
		"https://container.googleapis.com/v1/projects/saikatroyc-stateful-joonix/locations/us-central1-c/clusters/cluster-token-poc-1", // identity provider
	)
	klog.Infof("audience: %s", audience)
	stsRequest := &sts.GoogleIdentityStsV1ExchangeTokenRequest{
		Audience:           audience,
		GrantType:          "urn:ietf:params:oauth:grant-type:token-exchange",
		Scope:              credentials.DefaultAuthScopes()[0],
		RequestedTokenType: "urn:ietf:params:oauth:token-type:access_token",
		SubjectTokenType:   "urn:ietf:params:oauth:token-type:jwt",
		SubjectToken:       k8sSAToken,
	}

	stsResponse, err := stsService.V1.Token(stsRequest).Do()
	if err != nil {
		return nil, fmt.Errorf("IdentityBindingToken exchange error with audience %q: %v", audience, err)
	}

	return &oauth2.Token{
		AccessToken: stsResponse.AccessToken,
		TokenType:   stsResponse.TokenType,
		Expiry:      time.Now().Add(time.Second * time.Duration(stsResponse.ExpiresIn)),
	}, nil
}

func fetchTokenFromCSI() (*oauth2.Token, error) {
	unixURL := "http://unix/"
	volumeName := "gcs-fuse-csi-ephemeral"
	// Creating a new HTTP client that is configured to make HTTP requests over a unix domain socket.
	tokenSocketPath := filepath.Join(*volumeBasePath, volumeName, "token.sock")
	klog.Infof("dial socket path %s", tokenSocketPath)
	client := &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			DialContext: func(_ context.Context, _, _ string) (net.Conn, error) {
				return net.Dial("unix", tokenSocketPath)
			},
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, unixURL, nil)
	if err != nil {
		return nil, err
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected HTTP status: %v", resp.Status)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	token := &oauth2.Token{}
	err = json.Unmarshal(body, token)
	if err != nil {
		return nil, fmt.Errorf("proxyTokenSource cannot decode body: %w", err)
	}
	klog.Infof("unmarshall success, token %s", token.AccessToken)
	return token, nil
}

func startTokenServer(ctx context.Context) {
	// Create a unix domain socket and listen for incoming connections.
	socket, err := net.Listen("unix", tokenUrlSocketPath)
	if err != nil {
		klog.Errorf("failed to create socket %q: %v", tokenUrlSocketPath, err)
		// result <- err
		return
	}

	klog.Infof("created a listener using the socket path %s", tokenUrlSocketPath)
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		_, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		klog.Infof("got request on socket path %s", tokenUrlSocketPath)
		// queryParams := r.URL.Query()
		// for key, values := range queryParams {
		// 	klog.Infof("Query parameter %q: %v", key, values)
		// }
		ctx, cancel := context.WithCancel(context.Background())
		k8stoken, err := getK8sToken("/gcsfuse-sa-token/token")
		var stsToken *oauth2.Token
		if err != nil {
			klog.Errorf("failed to get k8s token from path %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		klog.Infof("k8s token %s", k8stoken)
		stsToken, err = fetchIdentityBindingToken(ctx, k8stoken)
		if err != nil {
			klog.Errorf("failed to get sts token from path %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		klog.Infof("sts token %s", stsToken.AccessToken)

		// Marshal the oauth2.Token object to JSON
		jsonToken, err := json.Marshal(stsToken)
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

	// result <- nil
	if err := server.Serve(socket); err != nil {
		klog.Errorf("failed to start the token server for %q: %v", tokenUrlSocketPath, err)
	}
}
