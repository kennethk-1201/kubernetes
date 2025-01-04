/*
Copyright 2016 The Kubernetes Authors.

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

package images

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/flowcontrol"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/cluster/ports"
	"k8s.io/kubernetes/pkg/kubelet/events"
	"net/http"
	"os"
	"sync"
)

const SourceCheckpointsDir = "/var/lib/kubelet/source-checkpoints"
const HttpsKeyFile = "/var/lib/kubelet/pki/kubelet-client-current.pem"
const HttpsCertFile = "/var/lib/kubelet/pki/kubelet-client-current.pem"
const CACertFile = "/etc/kubernetes/pki/ca.crt"

// checkpointManager provides the functionalities for checkpoint pulling and restoring.
type checkpointManager struct {
	recorder       record.EventRecorder
	kubeClient     clientset.Interface
	backOff        *flowcontrol.Backoff
	prevPullErrMsg sync.Map
}

var _ CheckpointManager = &checkpointManager{}

// NewCheckpointManager instantiates a new CheckpointManager object.
func NewCheckpointManager(recorder record.EventRecorder, kubeClient clientset.Interface, checkpointBackOff *flowcontrol.Backoff) CheckpointManager {
	return &checkpointManager{
		recorder:   recorder,
		kubeClient: kubeClient,
		backOff:    checkpointBackOff,
	}
}

// records an event using ref, event msg.  log to glog using prefix, msg, logFn
func (m *checkpointManager) logIt(objRef *v1.ObjectReference, eventtype, event, prefix, msg string, logFn func(args ...interface{})) {
	if objRef != nil {
		m.recorder.Event(objRef, eventtype, event, msg)
	} else {
		logFn(fmt.Sprint(prefix, " ", msg))
	}
}

// sendKubeletRequest prepares the necessary request information to communicate with another kubelet's API.
// TODO: Add proper TLS config management
func (m *checkpointManager) sendKubeletRequest(method string, endpoint string, body io.Reader) (*http.Response, error) {
	var tlsConfig *tls.Config
	// add client private key and cert
	cert, err := tls.LoadX509KeyPair(HttpsCertFile, HttpsKeyFile)
	if err != nil {
		return nil, fmt.Errorf("error loading certificate/key pair: %v", err)
	}
	// add the CA used to verify the source node's certificate
	if _, err := os.Stat(CACertFile); err == nil {
		caCert, err := os.ReadFile(CACertFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA certificate: %v", err)
		}

		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)

		tlsConfig = &tls.Config{
			Certificates: []tls.Certificate{cert},
			RootCAs:      caCertPool,
		}
	} else if errors.Is(err, os.ErrNotExist) {
		tlsConfig = &tls.Config{
			Certificates:       []tls.Certificate{cert},
			InsecureSkipVerify: true,
		}
	} else {
		return nil, fmt.Errorf("unable to determine if CA cert exists: %v", err)
	}

	transport := &http.Transport{TLSClientConfig: tlsConfig}
	client := &http.Client{Transport: transport}
	req, err := http.NewRequest(method, endpoint, body)
	if err != nil {
		return nil, fmt.Errorf("error creating request: %v", err)
	}
	return client.Do(req)
}

// retrieveSourcePodInfo retrieves the source pod's and source container's information using the new pod's annotations.
func (m *checkpointManager) retrieveCheckpointInfo(ctx context.Context, newPod *v1.Pod, container *v1.Container) (*v1.Pod, *v1.Container, string, error) {
	if m.kubeClient == nil {
		return nil, nil, "kube client does not exist, unable to retrieve source pod info", ErrCheckpointPull
	}

	sourcePodName, nameFound := newPod.GetAnnotations()["kubernetes.io/source-pod"]
	if !nameFound {
		// this error should not occur
		return nil, nil, "source pod annotation not specified", ErrInvalidSourcePod
	}

	sourceNamespace, namespaceFound := newPod.GetAnnotations()["kubernetes.io/source-namespace"]
	if !namespaceFound {
		sourceNamespace = newPod.Namespace
	}

	sourcePod, err := m.kubeClient.CoreV1().Pods(sourceNamespace).Get(ctx, sourcePodName, metav1.GetOptions{})
	if err != nil {
		return nil, nil, "unable to find source Pod", ErrInvalidSourcePod
	}

	containers := sourcePod.Spec.Containers
	for i := 0; i < len(containers); i++ {
		if containers[i].Name == container.Name {
			return sourcePod, container, "", nil
		}
	}

	return nil, nil, fmt.Sprintf("source container %s does not exist in list %+v", container.Name, containers), ErrInvalidSourcePod
}

// createCheckpoint creates the checkpoint in the source node.
func (m *checkpointManager) createCheckpoint(checkpointEndpoint string) (string, error) {
	checkpointResp, err := m.sendKubeletRequest(http.MethodPost, checkpointEndpoint, nil)
	if err != nil {
		return "unable to reach source node to checkpoint image", err
	}
	defer checkpointResp.Body.Close()
	if checkpointResp.StatusCode != http.StatusOK {
		body, readErr := io.ReadAll(checkpointResp.Body)
		if readErr != nil {
			return "source node failed to checkpoint image", readErr
		}
		return fmt.Sprintf("source node returned error: %b", body), readErr
	}

	return "", nil
}

// retrieveCheckpoint retrieves the checkpoint from the source node.
func (m *checkpointManager) retrieveCheckpoint(checkpointEndpoint string) (*io.ReadCloser, string, error) {
	getCheckpointResp, err := m.sendKubeletRequest(http.MethodGet, checkpointEndpoint, nil)
	if err != nil {
		return nil, "unable to reach source node to retrieve checkpoint", err
	}
	if getCheckpointResp.StatusCode != http.StatusOK {
		return nil, "source node failed to return checkpoint", errors.New("source node status code not ok")
	}
	return &getCheckpointResp.Body, "", nil
}

// saveCheckpoint saves the raw checkpoint data to a given path on disk.
func (m *checkpointManager) saveCheckpoint(checkpointData *io.ReadCloser, checkpointPath string) (string, error) {
	outFile, err := os.Create(checkpointPath)
	if err != nil {
		return "failed to store checkpoint", err
	}

	defer outFile.Close()
	_, err = io.Copy(outFile, *checkpointData)
	if err != nil {
		return "failed to store checkpoint", err
	}

	return "", nil
}

func (m *checkpointManager) getCheckpointDir(pod *v1.Pod) string {
	return fmt.Sprintf("%s/%s-%s", SourceCheckpointsDir, pod.Namespace, pod.Name)
}

// checkpointPullPrecheck checks for checkpoint presence in the local disk and returns (imageRef, exists, err).
func (m *checkpointManager) checkpointPullPrecheck(ctx context.Context, sourcePod *v1.Pod, newPod *v1.Pod, sourceContainer *v1.Container) (string, bool, error) {
	checkpointDir := m.getCheckpointDir(newPod)
	imageRef := fmt.Sprintf("%s/checkpoint-%s-%s-%s.tar", checkpointDir, sourcePod.Namespace, sourcePod.Name, sourceContainer.Name)

	if _, err := os.Stat(imageRef); errors.Is(err, os.ErrNotExist) {
		return imageRef, false, nil
	} else if err != nil {
		return "", false, err
	}

	return imageRef, true, nil
}

// pullCheckpoint pulls the checkpoint from the source node's kubelet API:
// 1. Create the checkpoint in the source kubelet.
// 2. Query the checkpoint file from the source kubelet.
// 3. Save the checkpoint file to disk.

func (m *checkpointManager) pullCheckpoint(ctx context.Context, sourcePod *v1.Pod, container *v1.Container, imageRef string) (msg string, err error) {
	// Create checkpoint in the source node.
	checkpointEndpoint := fmt.Sprintf("https://%s:%d/checkpoint/%s/%s/%s", sourcePod.Status.HostIP, ports.KubeletPort, sourcePod.Namespace, sourcePod.Name, container.Name)
	if msg, err = m.createCheckpoint(checkpointEndpoint); err != nil {
		return fmt.Sprintf("Failed to create checkpoint %q: %v", container.Name, err), ErrCheckpointPull
	}

	// Retrieve checkpoint from the source node.
	checkpointData, msg, err := m.retrieveCheckpoint(checkpointEndpoint)
	if err != nil {
		return fmt.Sprintf("Failed to retrieve checkpoint %q: %v", container.Name, err), ErrCheckpointPull
	}

	// Save checkpoint to disk.
	defer (*checkpointData).Close()
	if msg, err = m.saveCheckpoint(checkpointData, imageRef); err != nil {
		return fmt.Sprintf("Failed to save checkpoint %q: %v", container.Name, err), ErrCheckpointPull
	}

	return "", nil
}

// EnsureCheckpointExists pulls the container checkpoint for the specified pod and container, and returns
// (imageRef, error message, error). The imageRef here is the path of the checkpoint and NOT the image URI.
func (m *checkpointManager) EnsureCheckpointExists(ctx context.Context, objRef *v1.ObjectReference, newPod *v1.Pod, container *v1.Container) (string, string, error) {
	logPrefix := fmt.Sprintf("%s/%s/%s", newPod.Namespace, newPod.Name, container.Image)

	// Get the source pod information.
	sourcePod, sourceContainer, msg, err := m.retrieveCheckpointInfo(ctx, newPod, container)
	if err != nil {
		msg = fmt.Sprintf("Failed to get source Pod information for container %s: %s", container.Name, err)
		m.logIt(objRef, v1.EventTypeWarning, events.FailedToInspectCheckpoint, logPrefix, msg, klog.Warning)
		return "", msg, err
	}

	// Check if checkpoint already exists. If it exists, then return the imageRef.
	imageRef, exists, err := m.checkpointPullPrecheck(ctx, sourcePod, newPod, sourceContainer)
	if err != nil {
		msg = fmt.Sprintf("Failed to check if checkpoint already exists %q: %v", container.Name, err)
		m.logIt(objRef, v1.EventTypeWarning, events.FailedToPullCheckpoint, logPrefix, msg, klog.Warning)
		return "", "invalid path", ErrCheckpointPull
	}
	if exists {
		msg = fmt.Sprintf("Container checkpoint %q already present on machine", imageRef)
		m.logIt(objRef, v1.EventTypeNormal, events.PulledCheckpoint, logPrefix, msg, klog.Info)
		return imageRef, "checkpoint already exists", nil
	}

	// If the checkpoint does not exist, pull the checkpoint from the source node
	backOffKey := fmt.Sprintf("%s_%s", newPod.UID, imageRef)
	if m.backOff.IsInBackOffSinceUpdate(backOffKey, m.backOff.Clock.Now()) {
		msg = fmt.Sprintf("Back-off pulling checkpoint %q", imageRef)
		m.logIt(objRef, v1.EventTypeNormal, events.BackOffPullCheckpoint, logPrefix, msg, klog.Info)

		// Wrap the error from the actual pull if available.
		// This information is populated to the pods
		// .status.containerStatuses[*].state.waiting.message.
		prevPullErrMsg, ok := m.prevPullErrMsg.Load(backOffKey)
		if ok {
			msg = fmt.Sprintf("%s: %s", msg, prevPullErrMsg)
		}

		return "", msg, ErrCheckpointPullBackOff
	}

	// Ensure that the map cannot grow indefinitely.
	m.prevPullErrMsg.Delete(backOffKey)
	m.logIt(objRef, v1.EventTypeNormal, events.PullingCheckpoint, logPrefix, fmt.Sprintf("Pulling checkpoint %q", container.Name), klog.Info)

	if msg, err = m.pullCheckpoint(ctx, sourcePod, container, imageRef); err != nil {
		m.logIt(objRef, v1.EventTypeWarning, events.FailedToPullCheckpoint, logPrefix, msg, klog.Warning)
		m.backOff.Next(backOffKey, m.backOff.Clock.Now())

		// Store the actual pull error for providing that information during
		// the image pull back-off.
		m.prevPullErrMsg.Store(backOffKey, fmt.Sprintf("%s: %s", err, msg))
		return "", msg, err
	}

	m.logIt(objRef, v1.EventTypeNormal, events.PulledCheckpoint, logPrefix, msg, klog.Info)
	m.backOff.GC()
	return imageRef, "successfully retrieved checkpoint", nil
}

// CreatePodCheckpointStore creates the directory containing all of a pod's source checkpoints.
func (m *checkpointManager) CreatePodCheckpointStore(pod *v1.Pod) error {
	checkpointDir := m.getCheckpointDir(pod)

	// remove old checkpoints to prevent inconsistencies
	if err := os.RemoveAll(checkpointDir); err != nil {
		klog.V(4).InfoS("Unable to find checkpoint directory for pod", "pod", klog.KObj(pod))
	}

	// create new checkpoint directory for pod
	if err := os.MkdirAll(checkpointDir, os.ModePerm); err != nil {
		klog.ErrorS(err, "Failed to create checkpoint directory", "pod", klog.KObj(pod))
		return err
	}

	return nil
}
