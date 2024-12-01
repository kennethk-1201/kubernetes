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
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/cluster/ports"
	"net/http"
	"os"
)

const SourceCheckpointsDir = "/var/lib/kubelet/source-checkpoints"
const HttpsCertFile = "/var/run/kubernetes/client-admin.crt"
const HttpsKeyFile = "/var/run/kubernetes/client-admin.key"

// imageManager provides the functionalities for image pulling.
type checkpointManager struct {
	recorder   record.EventRecorder
	kubeClient clientset.Interface

	// TODO:
	// - add secure connection for kubelet request
	// - implement backoff logic
	// - implement event recorder logic
	// - eventually shift core logic down to crio
}

var _ CheckpointManager = &checkpointManager{}

// NewCheckpointManager instantiates a new CheckpointManager object.
func NewCheckpointManager(recorder record.EventRecorder, kubeClient clientset.Interface) CheckpointManager {
	return &checkpointManager{
		recorder:   recorder,
		kubeClient: kubeClient,
	}
}

// sendKubeletRequest prepares the necessary request information to communicate with another kubelet's API.
func sendKubeletRequest(method string, endpoint string, body io.Reader) (*http.Response, error) {
	cert, err := tls.LoadX509KeyPair(HttpsCertFile, HttpsKeyFile)
	if err != nil {
		klog.Errorf("Error loading certificate/key pair: %v\n", err)
		return nil, err
	}
	tlsConfig := &tls.Config{
		Certificates:       []tls.Certificate{cert},
		InsecureSkipVerify: true, // Equivalent to -k in curl; disables certificate validation
	}
	transport := &http.Transport{TLSClientConfig: tlsConfig}
	client := &http.Client{Transport: transport}
	req, err := http.NewRequest(method, endpoint, body)
	if err != nil {
		klog.Errorf("Error creating request: %v\n", err)
		return nil, err
	}
	return client.Do(req)
}

// retrieveSourcePodInfo retrieves the source pod's information using the new pod's annotations.
func (m *checkpointManager) retrieveSourcePodInfo(newPod *v1.Pod, container *v1.Container) (string, string, string, string, string, error) {
	if m.kubeClient == nil {
		klog.Errorf("[retrieveSourcePodInfo] kube client does not exist")
		return "", "", "", "", "kube client does not exist, unable to retrieve source pod info", ErrImageRestore
	}

	sourcePodName, nameFound := newPod.GetAnnotations()["kubernetes.io/source-pod"]
	if !nameFound {
		klog.Errorf("[retrieveSourcePodInfo] source pod annnotation not found")
		return "", "", "", "", "source pod annotation not specified", ErrInvalidSourcePodSpec
	}

	sourceNamespace, namespaceFound := newPod.GetAnnotations()["kubernetes.io/source-namespace"]
	if !namespaceFound {
		sourceNamespace = newPod.Namespace
	}

	sourcePod, err := m.kubeClient.CoreV1().Pods(sourceNamespace).Get(context.Background(), sourcePodName, metav1.GetOptions{})
	if err != nil {
		klog.Errorf("[retrieveSourcePodInfo] unable to find pod with name=%s, namespace=%s", sourcePodName, sourceNamespace)
		return "", "", "", "", "unable to find source Pod", ErrInvalidSourcePodSpec
	}

	containers := sourcePod.Spec.Containers
	for i := 0; i < len(containers); i++ {
		if containers[i].Name == container.Name {
			return sourcePodName, sourceNamespace, container.Name, sourcePod.Status.HostIP, "", nil
		}
	}

	klog.Errorf("[retrieveSourcePodInfo] unable to find container %s in pod spec", container.Name)
	return "", "", "", "", fmt.Sprintf("source container %s does not exist in list %+v", container.Name, containers), ErrInvalidSourcePodSpec
}

// createCheckpoint creates the checkpoint in the source node.
func (m *checkpointManager) createCheckpoint(checkpointEndpoint string) (string, error) {
	checkpointResp, err := sendKubeletRequest(http.MethodPost, checkpointEndpoint, nil)
	if err != nil {
		klog.ErrorS(err, "[restoreContainer] Failed to call the checkpoint endpoint", "checkpointEndpoint", checkpointEndpoint)
		return "unable to reach source node to checkpoint image", ErrImageCheckpointBackOff
	}
	defer checkpointResp.Body.Close()
	if checkpointResp.StatusCode != http.StatusOK {
		body, readErr := ioutil.ReadAll(checkpointResp.Body)
		if readErr != nil {
			fmt.Printf("[restoreContainer] Error reading error response: %v\n", readErr)
		}
		klog.Errorf("[restoreContainer] Checkpoint response not ok: %s", body)
		return "source node failed to checkpoint image", ErrImageCheckpointBackOff
	}

	return "", nil
}

// retrieveCheckpoint retrieves the checkpoint from the source node.
func (m *checkpointManager) retrieveCheckpoint(checkpointEndpoint string) (*io.ReadCloser, string, error) {
	getCheckpointResp, err := sendKubeletRequest(http.MethodGet, checkpointEndpoint, nil)
	if err != nil {
		klog.ErrorS(err, "[restoreContainer] Failed to retrieve from checkpoint endpoint", "checkpointEndpoint", checkpointEndpoint)
		return nil, "unable to reach source node to retrieve checkpoint", ErrImageRetrieveCheckpointBackOff
	}
	if getCheckpointResp.StatusCode != http.StatusOK {
		klog.ErrorS(err, "[restoreContainer] Failed to call the retrieve checkpoint endpoint", "checkpointEndpoint", checkpointEndpoint)
		return nil, "source node failed to return checkpoint", ErrImageRetrieveCheckpointBackOff
	}
	return &getCheckpointResp.Body, "", nil
}

// saveCheckpoint saves the raw checkpoint data to a given path on disk.
func (m *checkpointManager) saveCheckpoint(checkpointData *io.ReadCloser, checkpointPath string) (string, error) {
	outFile, err := os.Create(checkpointPath)
	if err != nil {
		klog.ErrorS(err, "[restoreContainer] Failed to create tarfile", "path", checkpointPath)
		return "failed to store checkpoint", ErrImageRetrieveCheckpointBackOff
	}
	defer outFile.Close()

	_, err = io.Copy(outFile, *checkpointData)
	if err != nil {
		klog.ErrorS(err, "[restoreContainer] Failed to copy tarfile", "path", checkpointPath)
		return "failed to store checkpoint", ErrImageRetrieveCheckpointBackOff
	}

	return "", nil
}

func (m *checkpointManager) getCheckpointDir(pod *v1.Pod) string {
	return fmt.Sprintf("%s/%s-%s", SourceCheckpointsDir, pod.Namespace, pod.Name)
}

// EnsureCheckpointExists pulls the container checkpoint for the specified pod and container, and returns
// (imageRef, error message, error). The imageRef here is the path of the checkpoint and NOT the image URI.
func (m *checkpointManager) EnsureCheckpointExists(ctx context.Context, newPod *v1.Pod, container *v1.Container) (string, string, error) {
	sourcePodName, sourceNamespace, sourceContainer, sourceNodeIP, msg, err := m.retrieveSourcePodInfo(newPod, container)
	if err != nil {
		return "", msg, err
	}
	klog.InfoS("Retrieving checkpoint", "pod", sourcePodName, "namespace", sourceNamespace, "container", sourceContainer, "node", sourceNodeIP)

	checkpointDir := m.getCheckpointDir(newPod)
	checkpointPath := fmt.Sprintf("%s/checkpoint-%s-%s-%s.tar", checkpointDir, sourceNamespace, sourcePodName, sourceContainer)

	if _, err = os.Stat(checkpointPath); errors.Is(err, os.ErrNotExist) {
		// Step 1: Create checkpoint
		checkpointEndpoint := fmt.Sprintf("https://%s:%d/checkpoint/%s/%s/%s", sourceNodeIP, ports.KubeletPort, sourceNamespace, sourcePodName, sourceContainer)
		if msg, err = m.createCheckpoint(checkpointEndpoint); err != nil {
			return "", msg, err
		}
		// Step 2: Retrieve checkpoint
		checkpointData, msg, err := m.retrieveCheckpoint(checkpointEndpoint)
		if err != nil {
			return "", msg, err
		}
		// Step 3: Save checkpoint
		defer (*checkpointData).Close()
		if msg, err = m.saveCheckpoint(checkpointData, checkpointPath); err != nil {
			return "", msg, err
		}

		return checkpointPath, "successfully retrieved checkpoint", nil
	} else if err != nil {
		klog.ErrorS(err, "Failed to check if file exists", "path", checkpointPath)
		return "", "invalid path", err
	}

	return checkpointPath, "checkpoint already exists", nil
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
