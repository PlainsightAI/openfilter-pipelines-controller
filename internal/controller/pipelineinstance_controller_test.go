/*
Copyright 2025.

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

package controller

import (
	"context"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	pipelinesv1alpha1 "github.com/PlainsightAI/openfilter-pipelines-controller/api/v1alpha1"
	"github.com/PlainsightAI/openfilter-pipelines-controller/internal/queue"
)

// MockValkeyClient provides a mock implementation for testing
type MockValkeyClient struct {
	StreamLength       int64
	ConsumerGroupLag   int64
	PendingCount       int64
	DLQLength          int64
	Messages           []mockMessage
	AckedMessages      []string
	EnqueuedFiles      []mockEnqueuedFile
	DLQEntries         []mockDLQEntry
	AutoClaimMessages  []mockMessage
	AutoClaimCallCount int
	DeletedMessageIDs  []string
	// PendingByConsumer simulates XPENDING for a specific consumer
	PendingByConsumer map[string][]string
}

type mockMessage struct {
	ID     string
	Values map[string]string
}

type mockEnqueuedFile struct {
	Stream     string
	InstanceID string
	Filepath   string
	Attempts   int
}

type mockDLQEntry struct {
	DLQKey     string
	InstanceID string
	Filepath   string
	Attempts   int
	Reason     string
}

func (m *MockValkeyClient) CreateStreamAndGroup(ctx context.Context, streamKey, groupName string) error {
	return nil
}

func (m *MockValkeyClient) GetStreamLength(ctx context.Context, streamKey string) (int64, error) {
	return m.StreamLength, nil
}

func (m *MockValkeyClient) GetConsumerGroupLag(ctx context.Context, streamKey, groupName string) (int64, error) {
	return m.ConsumerGroupLag, nil
}

func (m *MockValkeyClient) GetPendingCount(ctx context.Context, streamKey, groupName string) (int64, error) {
	return m.PendingCount, nil
}

func (m *MockValkeyClient) AckMessage(ctx context.Context, streamKey, groupName, messageID string) error {
	m.AckedMessages = append(m.AckedMessages, messageID)
	return nil
}

func (m *MockValkeyClient) EnqueueFileWithAttempts(ctx context.Context, streamKey, instanceID, filepath string, attempts int) (string, error) {
	m.EnqueuedFiles = append(m.EnqueuedFiles, mockEnqueuedFile{
		Stream:     streamKey,
		InstanceID: instanceID,
		Filepath:   filepath,
		Attempts:   attempts,
	})
	m.StreamLength++
	return fmt.Sprintf("msg-%d", len(m.EnqueuedFiles)), nil
}

func (m *MockValkeyClient) AddToDLQ(ctx context.Context, dlqKey, instanceID, filepath string, attempts int, reason string) error {
	m.DLQEntries = append(m.DLQEntries, mockDLQEntry{
		DLQKey:     dlqKey,
		InstanceID: instanceID,
		Filepath:   filepath,
		Attempts:   attempts,
		Reason:     reason,
	})
	return nil
}

// GetPendingForConsumer returns up to 'count' pending message IDs for a consumer.
func (m *MockValkeyClient) GetPendingForConsumer(ctx context.Context, streamKey, groupName, consumer string, count int64) ([]string, error) {
	if m.PendingByConsumer == nil {
		return []string{}, nil
	}
	pending := m.PendingByConsumer[consumer]
	if len(pending) == 0 {
		return []string{}, nil
	}
	// Respect count if > 0; pop from the slice to simulate consumption
	if count > 0 && int64(len(pending)) > count {
		ids := append([]string(nil), pending[:count]...)
		m.PendingByConsumer[consumer] = append([]string(nil), pending[count:]...)
		return ids, nil
	}
	ids := append([]string(nil), pending...)
	m.PendingByConsumer[consumer] = nil
	return ids, nil
}

func (m *MockValkeyClient) AutoClaim(ctx context.Context, streamKey, groupName, consumerName string, minIdleTime int64, count int64) ([]queue.XMessage, error) {
	m.AutoClaimCallCount++
	messages := make([]queue.XMessage, len(m.AutoClaimMessages))
	for i, msg := range m.AutoClaimMessages {
		messages[i] = queue.XMessage{
			ID:     msg.ID,
			Values: msg.Values,
		}
	}
	return messages, nil
}

func (m *MockValkeyClient) ReadRange(ctx context.Context, streamKey, start, end string, count int64) ([]queue.XMessage, error) {
	result := make([]queue.XMessage, len(m.Messages))
	for i, msg := range m.Messages {
		result[i] = queue.XMessage{
			ID:     msg.ID,
			Values: msg.Values,
		}
	}
	// Simulate messages being consumed once read.
	m.Messages = nil
	return result, nil
}

func (m *MockValkeyClient) DeleteMessages(ctx context.Context, streamKey string, messageIDs ...string) error {
	m.DeletedMessageIDs = append(m.DeletedMessageIDs, messageIDs...)
	return nil
}

var _ = Describe("PipelineInstance Controller", func() {
	Context("When reconciling a PipelineInstance resource", func() {
		const (
			namespace = "default"
			timeout   = time.Second * 10
			interval  = time.Millisecond * 250
		)

		ctx := context.Background()

		var (
			pipelineInstanceName string
			pipelineName         string
			pipelineSourceName   string
			pipeline             *pipelinesv1alpha1.Pipeline
			pipelineSource       *pipelinesv1alpha1.PipelineSource
			pipelineInstance     *pipelinesv1alpha1.PipelineInstance
			mockValkey           *MockValkeyClient
			reconciler           *PipelineInstanceReconciler
			testCounter          int
		)

		setPodStatus := func(podName string, phase corev1.PodPhase, statuses []corev1.ContainerStatus) {
			Eventually(func() error {
				existing := &corev1.Pod{}
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: podName, Namespace: namespace}, existing); err != nil {
					return err
				}
				existing.Status.Phase = phase
				existing.Status.ContainerStatuses = statuses
				return k8sClient.Status().Update(ctx, existing)
			}, timeout, interval).Should(Succeed())
		}

		BeforeEach(func() {
			// Generate unique names for this test
			testCounter++
			pipelineName = fmt.Sprintf("test-pipeline-%d", testCounter)
			pipelineSourceName = fmt.Sprintf("test-source-%d", testCounter)
			pipelineInstanceName = fmt.Sprintf("test-instance-%d", testCounter)

			// Create Pipeline resource
			pipeline = &pipelinesv1alpha1.Pipeline{
				ObjectMeta: metav1.ObjectMeta{
					Name:      pipelineName,
					Namespace: namespace,
				},
				Spec: pipelinesv1alpha1.PipelineSpec{
					VideoInputPath: "/ws/custom-input.mp4",
					Filters: []pipelinesv1alpha1.Filter{
						{
							Name:  "test-filter",
							Image: "test-image:latest",
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, pipeline)).To(Succeed())

			// Create PipelineSource resource
			pipelineSource = &pipelinesv1alpha1.PipelineSource{
				ObjectMeta: metav1.ObjectMeta{
					Name:      pipelineSourceName,
					Namespace: namespace,
				},
				Spec: pipelinesv1alpha1.PipelineSourceSpec{
					Bucket: &pipelinesv1alpha1.BucketSource{
						Name:     "test-bucket",
						Endpoint: "http://minio:9000",
						Region:   "us-east-1",
					},
				},
			}
			Expect(k8sClient.Create(ctx, pipelineSource)).To(Succeed())

			// Initialize mock Valkey client with default values
			mockValkey = &MockValkeyClient{
				StreamLength:      100,
				ConsumerGroupLag:  50,
				PendingCount:      0,
				DLQLength:         0,
				AckedMessages:     []string{},
				EnqueuedFiles:     []mockEnqueuedFile{},
				DLQEntries:        []mockDLQEntry{},
				AutoClaimMessages: []mockMessage{},
			}

			// Create reconciler with mock
			reconciler = &PipelineInstanceReconciler{
				Client:       k8sClient,
				Scheme:       k8sClient.Scheme(),
				ValkeyClient: mockValkey,
				ValkeyAddr:   "valkey:6379",
				ClaimerImage: "claimer:latest",
			}
		})

		AfterEach(func() {
			// Cleanup Job if it exists
			if pipelineInstance != nil {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: pipelineInstanceName, Namespace: namespace}, pipelineInstance)
				if err == nil && pipelineInstance.Status.JobName != "" {
					job := &batchv1.Job{}
					err = k8sClient.Get(ctx, types.NamespacedName{
						Name:      pipelineInstance.Status.JobName,
						Namespace: namespace,
					}, job)
					if err == nil {
						_ = k8sClient.Delete(ctx, job)
					}
				}

				// Cleanup PipelineInstance
				err = k8sClient.Delete(ctx, pipelineInstance)
				if err != nil && !errors.IsNotFound(err) {
					Expect(err).NotTo(HaveOccurred())
				}
			}

			// Cleanup any remaining pods for this test
			podList := &corev1.PodList{}
			err := k8sClient.List(ctx, podList, client.InNamespace(namespace))
			if err == nil {
				for _, pod := range podList.Items {
					// Delete pods matching test-specific pod names
					if pod.Labels["filter.plainsight.ai/instance"] == "test123" {
						_ = k8sClient.Delete(ctx, &pod)
					}
				}
			}

			// Cleanup PipelineSource
			if pipelineSource != nil {
				_ = k8sClient.Delete(ctx, pipelineSource)
			}

			// Cleanup Pipeline
			err = k8sClient.Delete(ctx, pipeline)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should initialize TotalFiles from stream length on first reconcile", func() {
			pipelineInstance = &pipelinesv1alpha1.PipelineInstance{
				ObjectMeta: metav1.ObjectMeta{
					Name:      pipelineInstanceName,
					Namespace: namespace,
				},
				Spec: pipelinesv1alpha1.PipelineInstanceSpec{
					PipelineRef: pipelinesv1alpha1.PipelineReference{
						Name: pipelineName,
					},
					SourceRef: pipelinesv1alpha1.SourceReference{
						Name: pipelineSourceName,
					},
				},
			}
			Expect(k8sClient.Create(ctx, pipelineInstance)).To(Succeed())

			// First reconcile should initialize TotalFiles
			_, err := reconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{
					Name:      pipelineInstanceName,
					Namespace: namespace,
				},
			})
			Expect(err).NotTo(HaveOccurred())

			// Check that TotalFiles was set
			Eventually(func() bool {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: pipelineInstanceName, Namespace: namespace}, pipelineInstance)
				if err != nil {
					return false
				}
				return pipelineInstance.Status.Counts != nil && pipelineInstance.Status.Counts.TotalFiles == 100
			}, timeout, interval).Should(BeTrue())
		})

		It("should create a Job for the PipelineInstance", func() {
			pipelineInstance = &pipelinesv1alpha1.PipelineInstance{
				ObjectMeta: metav1.ObjectMeta{
					Name:      pipelineInstanceName,
					Namespace: namespace,
				},
				Spec: pipelinesv1alpha1.PipelineInstanceSpec{
					PipelineRef: pipelinesv1alpha1.PipelineReference{
						Name: pipelineName,
					},
					SourceRef: pipelinesv1alpha1.SourceReference{
						Name: pipelineSourceName,
					},
					Execution: &pipelinesv1alpha1.ExecutionConfig{
						Parallelism: ptr.To(int32(5)),
					},
				},
			}
			Expect(k8sClient.Create(ctx, pipelineInstance)).To(Succeed())

			// Reconcile twice: once to set TotalFiles, once to create Job
			_, err := reconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: pipelineInstanceName, Namespace: namespace},
			})
			Expect(err).NotTo(HaveOccurred())

			_, err = reconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: pipelineInstanceName, Namespace: namespace},
			})
			Expect(err).NotTo(HaveOccurred())

			// Verify Job was created
			Eventually(func() bool {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: pipelineInstanceName, Namespace: namespace}, pipelineInstance)
				if err != nil {
					return false
				}
				if pipelineInstance.Status.JobName == "" {
					return false
				}

				job := &batchv1.Job{}
				err = k8sClient.Get(ctx, types.NamespacedName{
					Name:      pipelineInstance.Status.JobName,
					Namespace: namespace,
				}, job)
				return err == nil
			}, timeout, interval).Should(BeTrue())

			// Verify Job spec
			job := &batchv1.Job{}
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name:      pipelineInstance.Status.JobName,
				Namespace: namespace,
			}, job)
			Expect(err).NotTo(HaveOccurred())
			Expect(*job.Spec.Parallelism).To(Equal(int32(5)))
			Expect(*job.Spec.Completions).To(Equal(int32(100)))
			Expect(job.Spec.Template.Spec.InitContainers).To(HaveLen(1))
			Expect(job.Spec.Template.Spec.InitContainers[0].Name).To(Equal("claimer"))
			claimerEnv := job.Spec.Template.Spec.InitContainers[0].Env
			Expect(claimerEnv).To(ContainElement(corev1.EnvVar{Name: "VIDEO_INPUT_PATH", Value: "/ws/custom-input.mp4"}))
			// Only user-defined filters should be present.
			Expect(job.Spec.Template.Spec.Containers).To(HaveLen(1))
			Expect(job.Spec.Template.Spec.Containers[0].Name).To(Equal("test-filter"))
		})

		It("should set Progressing condition during execution", func() {
			pipelineInstance = &pipelinesv1alpha1.PipelineInstance{
				ObjectMeta: metav1.ObjectMeta{
					Name:      pipelineInstanceName,
					Namespace: namespace,
				},
				Spec: pipelinesv1alpha1.PipelineInstanceSpec{
					PipelineRef: pipelinesv1alpha1.PipelineReference{
						Name: pipelineName,
					},
					SourceRef: pipelinesv1alpha1.SourceReference{
						Name: pipelineSourceName,
					},
				},
			}
			Expect(k8sClient.Create(ctx, pipelineInstance)).To(Succeed())

			// First reconcile initializes TotalFiles
			_, err := reconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: pipelineInstanceName, Namespace: namespace},
			})
			Expect(err).NotTo(HaveOccurred())

			// Wait for initialization
			Eventually(func() bool {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: pipelineInstanceName, Namespace: namespace}, pipelineInstance)
				return err == nil && pipelineInstance.Status.Counts != nil && pipelineInstance.Status.Counts.TotalFiles == 100
			}, timeout, interval).Should(BeTrue())

			// Second reconcile creates job and sets conditions
			_, err = reconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: pipelineInstanceName, Namespace: namespace},
			})
			Expect(err).NotTo(HaveOccurred())

			// Verify Progressing condition is set
			Eventually(func() bool {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: pipelineInstanceName, Namespace: namespace}, pipelineInstance)
				if err != nil {
					return false
				}
				progressingCond := meta.FindStatusCondition(pipelineInstance.Status.Conditions, ConditionTypeProgressing)
				return progressingCond != nil && progressingCond.Status == metav1.ConditionTrue
			}, timeout, interval).Should(BeTrue())
		})

		It("should handle Pipeline not found error", func() {
			pipelineInstance = &pipelinesv1alpha1.PipelineInstance{
				ObjectMeta: metav1.ObjectMeta{
					Name:      pipelineInstanceName,
					Namespace: namespace,
				},
				Spec: pipelinesv1alpha1.PipelineInstanceSpec{
					PipelineRef: pipelinesv1alpha1.PipelineReference{
						Name: "nonexistent-pipeline",
					},
					SourceRef: pipelinesv1alpha1.SourceReference{
						Name: pipelineSourceName,
					},
				},
			}
			Expect(k8sClient.Create(ctx, pipelineInstance)).To(Succeed())

			// Reconcile should handle error gracefully
			_, err := reconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: pipelineInstanceName, Namespace: namespace},
			})

			Expect(err).To(HaveOccurred())

			// Verify Degraded condition is set
			Eventually(func() bool {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: pipelineInstanceName, Namespace: namespace}, pipelineInstance)
				if err != nil {
					return false
				}
				degradedCond := meta.FindStatusCondition(pipelineInstance.Status.Conditions, ConditionTypeDegraded)
				return degradedCond != nil && degradedCond.Status == metav1.ConditionTrue
			}, timeout, interval).Should(BeTrue())
		})

		It("should handle PipelineSource not found error", func() {
			pipelineInstance = &pipelinesv1alpha1.PipelineInstance{
				ObjectMeta: metav1.ObjectMeta{
					Name:      pipelineInstanceName,
					Namespace: namespace,
				},
				Spec: pipelinesv1alpha1.PipelineInstanceSpec{
					PipelineRef: pipelinesv1alpha1.PipelineReference{
						Name: pipelineName,
					},
					SourceRef: pipelinesv1alpha1.SourceReference{
						Name: "nonexistent-source",
					},
				},
			}
			Expect(k8sClient.Create(ctx, pipelineInstance)).To(Succeed())

			// Reconcile should handle error gracefully
			_, err := reconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: pipelineInstanceName, Namespace: namespace},
			})

			Expect(err).To(HaveOccurred())

			// Verify Degraded condition is set
			Eventually(func() bool {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: pipelineInstanceName, Namespace: namespace}, pipelineInstance)
				if err != nil {
					return false
				}
				degradedCond := meta.FindStatusCondition(pipelineInstance.Status.Conditions, ConditionTypeDegraded)
				return degradedCond != nil && degradedCond.Status == metav1.ConditionTrue && degradedCond.Reason == "PipelineSourceNotFound"
			}, timeout, interval).Should(BeTrue())
		})

		It("should build streaming Deployment without a ServiceAccountName", func() {
			// Create a streaming Pipeline
			streamPipelineName := fmt.Sprintf("test-pipeline-stream-%d", testCounter)
			streamPipeline := &pipelinesv1alpha1.Pipeline{
				ObjectMeta: metav1.ObjectMeta{
					Name:      streamPipelineName,
					Namespace: namespace,
				},
				Spec: pipelinesv1alpha1.PipelineSpec{
					Mode: pipelinesv1alpha1.PipelineModeStream,
					Filters: []pipelinesv1alpha1.Filter{
						{Name: "video-in", Image: "busybox:latest"},
					},
				},
			}
			Expect(k8sClient.Create(ctx, streamPipeline)).To(Succeed())

			defer func() {
				_ = k8sClient.Delete(ctx, streamPipeline)
			}()

			// Create RTSP PipelineSource
			rtspSourceName := fmt.Sprintf("test-rtsp-source-%d", testCounter)
			rtspSource := &pipelinesv1alpha1.PipelineSource{
				ObjectMeta: metav1.ObjectMeta{
					Name:      rtspSourceName,
					Namespace: namespace,
				},
				Spec: pipelinesv1alpha1.PipelineSourceSpec{
					RTSP: &pipelinesv1alpha1.RTSPSource{
						Host: "rtsp-video-stream",
						Port: 8554,
						Path: "/live",
					},
				},
			}
			Expect(k8sClient.Create(ctx, rtspSource)).To(Succeed())

			defer func() {
				_ = k8sClient.Delete(ctx, rtspSource)
			}()

			// Create PipelineInstance
			streamInstanceName := fmt.Sprintf("test-instance-stream-%d", testCounter)
			streamInstance := &pipelinesv1alpha1.PipelineInstance{
				ObjectMeta: metav1.ObjectMeta{
					Name:      streamInstanceName,
					Namespace: namespace,
				},
				Spec: pipelinesv1alpha1.PipelineInstanceSpec{
					PipelineRef: pipelinesv1alpha1.PipelineReference{Name: streamPipelineName},
					SourceRef:   pipelinesv1alpha1.SourceReference{Name: rtspSourceName},
				},
			}
			Expect(k8sClient.Create(ctx, streamInstance)).To(Succeed())

			defer func() {
				_ = k8sClient.Delete(ctx, streamInstance)
			}()

			// Reconcile to create Deployment
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: streamInstanceName, Namespace: namespace}})
			Expect(err).NotTo(HaveOccurred())

			// Deployment name should be recorded in status
			Eventually(func() bool {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: streamInstanceName, Namespace: namespace}, streamInstance)
				return err == nil && streamInstance.Status.Streaming != nil && streamInstance.Status.Streaming.DeploymentName != ""
			}, timeout, interval).Should(BeTrue())

			// Deployment should not specify serviceAccountName
			dep := &appsv1.Deployment{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: streamInstance.Status.Streaming.DeploymentName, Namespace: namespace}, dep)).To(Succeed())
			Expect(dep.Spec.Template.Spec.ServiceAccountName).To(Equal(""))
		})

		It("should handle completed pods and ACK successful messages", func() {
			pipelineInstance = &pipelinesv1alpha1.PipelineInstance{
				ObjectMeta: metav1.ObjectMeta{
					Name:      pipelineInstanceName,
					Namespace: namespace,
				},
				Spec: pipelinesv1alpha1.PipelineInstanceSpec{
					PipelineRef: pipelinesv1alpha1.PipelineReference{
						Name: pipelineName,
					},
					SourceRef: pipelinesv1alpha1.SourceReference{
						Name: pipelineSourceName,
					},
				},
			}
			Expect(k8sClient.Create(ctx, pipelineInstance)).To(Succeed())

			// Get the instanceID from the created PipelineInstance
			Eventually(func() bool {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: pipelineInstanceName, Namespace: namespace}, pipelineInstance)
				return err == nil && pipelineInstance.UID != ""
			}, timeout, interval).Should(BeTrue())
			instanceID := pipelineInstance.GetInstanceID()

			// Create a successful pod with queue annotations
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      fmt.Sprintf("test-pod-success-%d", testCounter),
					Namespace: namespace,
					Labels: map[string]string{
						"filter.plainsight.ai/instance": instanceID,
					},
					Annotations: map[string]string{
						AnnotationMessageID: "msg-123",
						AnnotationFile:      "test-file.txt",
						AnnotationAttempts:  "0",
					},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{Name: "test", Image: "test:latest"},
					},
				},
				Status: corev1.PodStatus{
					Phase: corev1.PodSucceeded,
				},
			}
			Expect(k8sClient.Create(ctx, pod)).To(Succeed())

			// Wait for pod to be created
			Eventually(func() bool {
				podName := fmt.Sprintf("test-pod-success-%d", testCounter)
				err := k8sClient.Get(ctx, types.NamespacedName{Name: podName, Namespace: namespace}, pod)
				return err == nil
			}, timeout, interval).Should(BeTrue())

			setPodStatus(
				fmt.Sprintf("test-pod-success-%d", testCounter),
				corev1.PodSucceeded,
				[]corev1.ContainerStatus{
					{
						Name: "test",
						State: corev1.ContainerState{
							Terminated: &corev1.ContainerStateTerminated{
								ExitCode: 0,
							},
						},
					},
				},
			)

			// Seed queue: map this pod (consumer) to a pending message and provide its fields
			successPodName := fmt.Sprintf("test-pod-success-%d", testCounter)
			mockValkey.PendingByConsumer = map[string][]string{
				successPodName: {"msg-123"},
			}
			mockValkey.Messages = []mockMessage{{
				ID: "msg-123",
				Values: map[string]string{
					"file":     "test-file.txt",
					"attempts": "0",
				},
			}}

			// Reconcile multiple times to ensure pod is processed
			for i := 0; i < 3; i++ {
				_, err := reconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: pipelineInstanceName, Namespace: namespace},
				})
				Expect(err).NotTo(HaveOccurred())
			}

			// Verify message was ACKed
			Eventually(func() bool {
				for _, msgID := range mockValkey.AckedMessages {
					if msgID == "msg-123" {
						return true
					}
				}
				return false
			}, timeout, interval).Should(BeTrue())

			// Verify no legacy processed annotation was set
			Consistently(func() bool {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: successPodName, Namespace: namespace}, pod)
				if err != nil {
					return false
				}
				_, exists := pod.Annotations["filter.plainsight.ai/processed"]
				return !exists
			}, time.Second*2, interval).Should(BeTrue())
		})

		It("should detect completion when queued and running are zero", func() {
			mockValkey.ConsumerGroupLag = 0
			mockValkey.PendingCount = 0

			pipelineInstance = &pipelinesv1alpha1.PipelineInstance{
				ObjectMeta: metav1.ObjectMeta{
					Name:      pipelineInstanceName,
					Namespace: namespace,
				},
				Spec: pipelinesv1alpha1.PipelineInstanceSpec{
					PipelineRef: pipelinesv1alpha1.PipelineReference{
						Name: pipelineName,
					},
					SourceRef: pipelinesv1alpha1.SourceReference{
						Name: pipelineSourceName,
					},
				},
			}
			Expect(k8sClient.Create(ctx, pipelineInstance)).To(Succeed())

			// Reconcile to initialize and create job
			_, err := reconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: pipelineInstanceName, Namespace: namespace},
			})
			Expect(err).NotTo(HaveOccurred())

			_, err = reconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: pipelineInstanceName, Namespace: namespace},
			})
			Expect(err).NotTo(HaveOccurred())

			// Get the job and mark it as complete
			Eventually(func() error {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: pipelineInstanceName, Namespace: namespace}, pipelineInstance)
				if err != nil {
					return err
				}
				if pipelineInstance.Status.JobName == "" {
					return fmt.Errorf("job not created yet")
				}

				job := &batchv1.Job{}
				err = k8sClient.Get(ctx, types.NamespacedName{
					Name:      pipelineInstance.Status.JobName,
					Namespace: namespace,
				}, job)
				if err != nil {
					return err
				}

				// Mark job as complete with all required fields
				now := metav1.Now()
				job.Status.StartTime = &now
				job.Status.CompletionTime = &now
				job.Status.Succeeded = *job.Spec.Completions
				job.Status.Conditions = []batchv1.JobCondition{
					{
						Type:               batchv1.JobSuccessCriteriaMet,
						Status:             corev1.ConditionTrue,
						LastTransitionTime: now,
						Reason:             "JobSuccessCriteriaMet",
						Message:            "Job success criteria met",
					},
					{
						Type:               batchv1.JobComplete,
						Status:             corev1.ConditionTrue,
						LastTransitionTime: now,
						Reason:             "Completed",
						Message:            "Job completed successfully",
					},
				}
				return k8sClient.Status().Update(ctx, job)
			}, timeout, interval).Should(Succeed())

			// Reconcile again to detect completion
			_, err = reconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: pipelineInstanceName, Namespace: namespace},
			})
			Expect(err).NotTo(HaveOccurred())

			// Verify Succeeded condition is set
			Eventually(func() bool {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: pipelineInstanceName, Namespace: namespace}, pipelineInstance)
				if err != nil {
					return false
				}
				succeededCond := meta.FindStatusCondition(pipelineInstance.Status.Conditions, ConditionTypeSucceeded)
				return succeededCond != nil && succeededCond.Status == metav1.ConditionTrue
			}, timeout, interval).Should(BeTrue())

			// Verify CompletionTime is set
			Expect(pipelineInstance.Status.CompletionTime).NotTo(BeNil())
		})
	})
})
