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
	Stream   string
	RunID    string
	Filepath string
	Attempts int
}

type mockDLQEntry struct {
	DLQKey   string
	RunID    string
	Filepath string
	Attempts int
	Reason   string
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

func (m *MockValkeyClient) EnqueueFileWithAttempts(ctx context.Context, streamKey, runID, filepath string, attempts int) (string, error) {
	m.EnqueuedFiles = append(m.EnqueuedFiles, mockEnqueuedFile{
		Stream:   streamKey,
		RunID:    runID,
		Filepath: filepath,
		Attempts: attempts,
	})
	m.StreamLength++
	return fmt.Sprintf("msg-%d", len(m.EnqueuedFiles)), nil
}

func (m *MockValkeyClient) AddToDLQ(ctx context.Context, dlqKey, runID, filepath string, attempts int, reason string) error {
	m.DLQEntries = append(m.DLQEntries, mockDLQEntry{
		DLQKey:   dlqKey,
		RunID:    runID,
		Filepath: filepath,
		Attempts: attempts,
		Reason:   reason,
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

var _ = Describe("PipelineRun Controller", func() {
	Context("When reconciling a PipelineRun resource", func() {
		const (
			namespace = "default"
			timeout   = time.Second * 10
			interval  = time.Millisecond * 250
		)

		ctx := context.Background()

		var (
			pipelineRunName string
			pipelineName    string
			pipeline        *pipelinesv1alpha1.Pipeline
			pipelineRun     *pipelinesv1alpha1.PipelineRun
			mockValkey      *MockValkeyClient
			reconciler      *PipelineRunReconciler
			testCounter     int
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
			pipelineRunName = fmt.Sprintf("test-pipelinerun-%d", testCounter)

			// Create Pipeline resource
			pipeline = &pipelinesv1alpha1.Pipeline{
				ObjectMeta: metav1.ObjectMeta{
					Name:      pipelineName,
					Namespace: namespace,
				},
				Spec: pipelinesv1alpha1.PipelineSpec{
					VideoInputPath: "/ws/custom-input.mp4",
					Source: pipelinesv1alpha1.Source{
						Bucket: &pipelinesv1alpha1.BucketSource{
							Name:     "test-bucket",
							Endpoint: "http://minio:9000",
							Region:   "us-east-1",
						},
					},
					Filters: []pipelinesv1alpha1.Filter{
						{
							Name:  "test-filter",
							Image: "test-image:latest",
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, pipeline)).To(Succeed())

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
			reconciler = &PipelineRunReconciler{
				Client:       k8sClient,
				Scheme:       k8sClient.Scheme(),
				ValkeyClient: mockValkey,
				ValkeyAddr:   "valkey:6379",
				ClaimerImage: "claimer:latest",
			}
		})

		AfterEach(func() {
			// Cleanup Job if it exists
			if pipelineRun != nil {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: pipelineRunName, Namespace: namespace}, pipelineRun)
				if err == nil && pipelineRun.Status.JobName != "" {
					job := &batchv1.Job{}
					err = k8sClient.Get(ctx, types.NamespacedName{
						Name:      pipelineRun.Status.JobName,
						Namespace: namespace,
					}, job)
					if err == nil {
						_ = k8sClient.Delete(ctx, job)
					}
				}

				// Cleanup PipelineRun
				err = k8sClient.Delete(ctx, pipelineRun)
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
					if pod.Labels["filter.plainsight.ai/run"] == "test123" {
						_ = k8sClient.Delete(ctx, &pod)
					}
				}
			}

			// Cleanup Pipeline
			err = k8sClient.Delete(ctx, pipeline)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should initialize TotalFiles from stream length on first reconcile", func() {
			pipelineRun = &pipelinesv1alpha1.PipelineRun{
				ObjectMeta: metav1.ObjectMeta{
					Name:      pipelineRunName,
					Namespace: namespace,
				},
				Spec: pipelinesv1alpha1.PipelineRunSpec{
					PipelineRef: pipelinesv1alpha1.PipelineReference{
						Name: pipelineName,
					},
				},
			}
			Expect(k8sClient.Create(ctx, pipelineRun)).To(Succeed())

			// First reconcile should initialize TotalFiles
			_, err := reconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{
					Name:      pipelineRunName,
					Namespace: namespace,
				},
			})
			Expect(err).NotTo(HaveOccurred())

			// Check that TotalFiles was set
			Eventually(func() bool {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: pipelineRunName, Namespace: namespace}, pipelineRun)
				if err != nil {
					return false
				}
				return pipelineRun.Status.Counts != nil && pipelineRun.Status.Counts.TotalFiles == 100
			}, timeout, interval).Should(BeTrue())
		})

		It("should create a Job for the PipelineRun", func() {
			pipelineRun = &pipelinesv1alpha1.PipelineRun{
				ObjectMeta: metav1.ObjectMeta{
					Name:      pipelineRunName,
					Namespace: namespace,
				},
				Spec: pipelinesv1alpha1.PipelineRunSpec{
					PipelineRef: pipelinesv1alpha1.PipelineReference{
						Name: pipelineName,
					},
					Execution: &pipelinesv1alpha1.ExecutionConfig{
						Parallelism: ptr.To(int32(5)),
					},
				},
			}
			Expect(k8sClient.Create(ctx, pipelineRun)).To(Succeed())

			// Reconcile twice: once to set TotalFiles, once to create Job
			_, err := reconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: pipelineRunName, Namespace: namespace},
			})
			Expect(err).NotTo(HaveOccurred())

			_, err = reconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: pipelineRunName, Namespace: namespace},
			})
			Expect(err).NotTo(HaveOccurred())

			// Verify Job was created
			Eventually(func() bool {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: pipelineRunName, Namespace: namespace}, pipelineRun)
				if err != nil {
					return false
				}
				if pipelineRun.Status.JobName == "" {
					return false
				}

				job := &batchv1.Job{}
				err = k8sClient.Get(ctx, types.NamespacedName{
					Name:      pipelineRun.Status.JobName,
					Namespace: namespace,
				}, job)
				return err == nil
			}, timeout, interval).Should(BeTrue())

			// Verify Job spec
			job := &batchv1.Job{}
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name:      pipelineRun.Status.JobName,
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

		It("should update status with file counts from Valkey", func() {
			mockValkey.StreamLength = 100
			mockValkey.ConsumerGroupLag = 30
			mockValkey.PendingCount = 20
			mockValkey.DLQLength = 5

			pipelineRun = &pipelinesv1alpha1.PipelineRun{
				ObjectMeta: metav1.ObjectMeta{
					Name:      pipelineRunName,
					Namespace: namespace,
				},
				Spec: pipelinesv1alpha1.PipelineRunSpec{
					PipelineRef: pipelinesv1alpha1.PipelineReference{
						Name: pipelineName,
					},
				},
			}
			Expect(k8sClient.Create(ctx, pipelineRun)).To(Succeed())

			// First reconcile initializes TotalFiles
			_, err := reconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: pipelineRunName, Namespace: namespace},
			})
			Expect(err).NotTo(HaveOccurred())

			// Wait for TotalFiles to be set
			Eventually(func() bool {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: pipelineRunName, Namespace: namespace}, pipelineRun)
				return err == nil && pipelineRun.Status.Counts != nil && pipelineRun.Status.Counts.TotalFiles == 100
			}, timeout, interval).Should(BeTrue())

			// Reconcile again to create job and update status
			_, err = reconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: pipelineRunName, Namespace: namespace},
			})
			Expect(err).NotTo(HaveOccurred())

			// One more reconcile to update all counts
			_, err = reconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: pipelineRunName, Namespace: namespace},
			})
			Expect(err).NotTo(HaveOccurred())

			// Verify status was updated
			Eventually(func() bool {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: pipelineRunName, Namespace: namespace}, pipelineRun)
				if err != nil {
					return false
				}
				if pipelineRun.Status.Counts == nil {
					return false
				}
				// Succeeded = TotalFiles - Queued - Running - Failed
				// With DLQLength = 5, formula becomes: 100 - 30 - 20 - 5 = 45
				// But mock returns DLQLength via GetStreamLength for DLQ key, not automatically
				// The test is checking that counts are correctly retrieved from Valkey
				return pipelineRun.Status.Counts.Queued == 30 &&
					pipelineRun.Status.Counts.Running == 20 &&
					pipelineRun.Status.Counts.TotalFiles == 100
			}, timeout, interval).Should(BeTrue())
		})

		It("should handle completed pods and ACK successful messages", func() {
			pipelineRun = &pipelinesv1alpha1.PipelineRun{
				ObjectMeta: metav1.ObjectMeta{
					Name:      pipelineRunName,
					Namespace: namespace,
				},
				Spec: pipelinesv1alpha1.PipelineRunSpec{
					PipelineRef: pipelinesv1alpha1.PipelineReference{
						Name: pipelineName,
					},
				},
			}
			Expect(k8sClient.Create(ctx, pipelineRun)).To(Succeed())

			// Get the runID from the created PipelineRun
			Eventually(func() bool {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: pipelineRunName, Namespace: namespace}, pipelineRun)
				return err == nil && pipelineRun.UID != ""
			}, timeout, interval).Should(BeTrue())
			runID := pipelineRun.GetRunID()

			// Create a successful pod with queue annotations
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      fmt.Sprintf("test-pod-success-%d", testCounter),
					Namespace: namespace,
					Labels: map[string]string{
						"filter.plainsight.ai/run": runID,
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
					NamespacedName: types.NamespacedName{Name: pipelineRunName, Namespace: namespace},
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

		It("should re-enqueue failed pods with incremented attempts", func() {
			pipelineRun = &pipelinesv1alpha1.PipelineRun{
				ObjectMeta: metav1.ObjectMeta{
					Name:      pipelineRunName,
					Namespace: namespace,
				},
				Spec: pipelinesv1alpha1.PipelineRunSpec{
					PipelineRef: pipelinesv1alpha1.PipelineReference{
						Name: pipelineName,
					},
					Execution: &pipelinesv1alpha1.ExecutionConfig{
						MaxAttempts: ptr.To(int32(3)),
					},
				},
			}
			Expect(k8sClient.Create(ctx, pipelineRun)).To(Succeed())

			// Get the runID from the created PipelineRun
			Eventually(func() bool {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: pipelineRunName, Namespace: namespace}, pipelineRun)
				return err == nil && pipelineRun.UID != ""
			}, timeout, interval).Should(BeTrue())
			runID := pipelineRun.GetRunID()

			// Create a failed pod
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      fmt.Sprintf("test-pod-failed-%d", testCounter),
					Namespace: namespace,
					Labels: map[string]string{
						"filter.plainsight.ai/run": runID,
					},
					Annotations: map[string]string{
						AnnotationMessageID: "msg-456",
						AnnotationFile:      "failed-file.txt",
						AnnotationAttempts:  "1",
					},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{Name: "test", Image: "test:latest"},
					},
				},
				Status: corev1.PodStatus{
					Phase: corev1.PodFailed,
					ContainerStatuses: []corev1.ContainerStatus{
						{
							Name: "test",
							State: corev1.ContainerState{
								Terminated: &corev1.ContainerStateTerminated{
									ExitCode: 1,
								},
							},
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, pod)).To(Succeed())

			// Seed queue mapping and message for this consumer
			mockValkey.PendingByConsumer = map[string][]string{
				pod.Name: {"msg-456"},
			}
			mockValkey.Messages = []mockMessage{{
				ID: "msg-456",
				Values: map[string]string{
					"file":     "failed-file.txt",
					"attempts": "1",
				},
			}}

			// Wait for the pod to exist before updating status
			Eventually(func() bool {
				podName := fmt.Sprintf("test-pod-failed-%d", testCounter)
				return k8sClient.Get(ctx, types.NamespacedName{Name: podName, Namespace: namespace}, pod) == nil
			}, timeout, interval).Should(BeTrue())

			setPodStatus(
				fmt.Sprintf("test-pod-failed-%d", testCounter),
				corev1.PodFailed,
				[]corev1.ContainerStatus{
					{
						Name: "test",
						State: corev1.ContainerState{
							Terminated: &corev1.ContainerStateTerminated{
								ExitCode: 1,
							},
						},
					},
				},
			)

			// Reconcile multiple times
			for i := 0; i < 3; i++ {
				_, err := reconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: pipelineRunName, Namespace: namespace},
				})
				Expect(err).NotTo(HaveOccurred())
			}

			// Verify file was re-enqueued with incremented attempts
			Eventually(func() bool {
				for _, enqueued := range mockValkey.EnqueuedFiles {
					if enqueued.Filepath == "failed-file.txt" && enqueued.Attempts == 2 {
						return true
					}
				}
				return false
			}, timeout, interval).Should(BeTrue())
		})

		It("should send files to DLQ after max attempts", func() {
			pipelineRun = &pipelinesv1alpha1.PipelineRun{
				ObjectMeta: metav1.ObjectMeta{
					Name:      pipelineRunName,
					Namespace: namespace,
				},
				Spec: pipelinesv1alpha1.PipelineRunSpec{
					PipelineRef: pipelinesv1alpha1.PipelineReference{
						Name: pipelineName,
					},
					Execution: &pipelinesv1alpha1.ExecutionConfig{
						MaxAttempts: ptr.To(int32(3)),
					},
				},
			}
			Expect(k8sClient.Create(ctx, pipelineRun)).To(Succeed())

			// Get the runID from the created PipelineRun
			Eventually(func() bool {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: pipelineRunName, Namespace: namespace}, pipelineRun)
				return err == nil && pipelineRun.UID != ""
			}, timeout, interval).Should(BeTrue())
			runID := pipelineRun.GetRunID()

			// Create a failed pod with max attempts
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      fmt.Sprintf("test-pod-max-attempts-%d", testCounter),
					Namespace: namespace,
					Labels: map[string]string{
						"filter.plainsight.ai/run": runID,
					},
					Annotations: map[string]string{
						AnnotationMessageID: "msg-789",
						AnnotationFile:      "dlq-file.txt",
						AnnotationAttempts:  "2", // Will be 3 after increment
					},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{Name: "test", Image: "test:latest"},
					},
				},
				Status: corev1.PodStatus{
					Phase: corev1.PodFailed,
					ContainerStatuses: []corev1.ContainerStatus{
						{
							Name: "test",
							State: corev1.ContainerState{
								Terminated: &corev1.ContainerStateTerminated{
									ExitCode: 1,
								},
							},
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, pod)).To(Succeed())

			// Seed queue mapping and message for this consumer (attempts will become 3)
			mockValkey.PendingByConsumer = map[string][]string{
				pod.Name: {"msg-789"},
			}
			mockValkey.Messages = []mockMessage{{
				ID: "msg-789",
				Values: map[string]string{
					"file":     "dlq-file.txt",
					"attempts": "2",
				},
			}}

			// Wait for the pod to exist before updating status
			Eventually(func() bool {
				podName := fmt.Sprintf("test-pod-max-attempts-%d", testCounter)
				return k8sClient.Get(ctx, types.NamespacedName{Name: podName, Namespace: namespace}, pod) == nil
			}, timeout, interval).Should(BeTrue())

			setPodStatus(
				fmt.Sprintf("test-pod-max-attempts-%d", testCounter),
				corev1.PodFailed,
				[]corev1.ContainerStatus{
					{
						Name: "test",
						State: corev1.ContainerState{
							Terminated: &corev1.ContainerStateTerminated{
								ExitCode: 1,
							},
						},
					},
				},
			)

			// Reconcile multiple times
			for i := 0; i < 3; i++ {
				_, err := reconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: pipelineRunName, Namespace: namespace},
				})
				Expect(err).NotTo(HaveOccurred())
			}

			// Verify file was added to DLQ
			Eventually(func() bool {
				for _, entry := range mockValkey.DLQEntries {
					if entry.Filepath == "dlq-file.txt" && entry.Attempts == 3 {
						return true
					}
				}
				return false
			}, timeout, interval).Should(BeTrue())
		})

		It("should set Progressing condition during execution", func() {
			pipelineRun = &pipelinesv1alpha1.PipelineRun{
				ObjectMeta: metav1.ObjectMeta{
					Name:      pipelineRunName,
					Namespace: namespace,
				},
				Spec: pipelinesv1alpha1.PipelineRunSpec{
					PipelineRef: pipelinesv1alpha1.PipelineReference{
						Name: pipelineName,
					},
				},
			}
			Expect(k8sClient.Create(ctx, pipelineRun)).To(Succeed())

			// First reconcile initializes TotalFiles
			_, err := reconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: pipelineRunName, Namespace: namespace},
			})
			Expect(err).NotTo(HaveOccurred())

			// Wait for initialization
			Eventually(func() bool {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: pipelineRunName, Namespace: namespace}, pipelineRun)
				return err == nil && pipelineRun.Status.Counts != nil && pipelineRun.Status.Counts.TotalFiles == 100
			}, timeout, interval).Should(BeTrue())

			// Second reconcile creates job and sets conditions
			_, err = reconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: pipelineRunName, Namespace: namespace},
			})
			Expect(err).NotTo(HaveOccurred())

			// Verify Progressing condition is set
			Eventually(func() bool {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: pipelineRunName, Namespace: namespace}, pipelineRun)
				if err != nil {
					return false
				}
				progressingCond := meta.FindStatusCondition(pipelineRun.Status.Conditions, ConditionTypeProgressing)
				return progressingCond != nil && progressingCond.Status == metav1.ConditionTrue
			}, timeout, interval).Should(BeTrue())
		})

		It("should detect completion when queued and running are zero", func() {
			mockValkey.ConsumerGroupLag = 0
			mockValkey.PendingCount = 0

			pipelineRun = &pipelinesv1alpha1.PipelineRun{
				ObjectMeta: metav1.ObjectMeta{
					Name:      pipelineRunName,
					Namespace: namespace,
				},
				Spec: pipelinesv1alpha1.PipelineRunSpec{
					PipelineRef: pipelinesv1alpha1.PipelineReference{
						Name: pipelineName,
					},
				},
			}
			Expect(k8sClient.Create(ctx, pipelineRun)).To(Succeed())

			// Reconcile to initialize and create job
			_, err := reconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: pipelineRunName, Namespace: namespace},
			})
			Expect(err).NotTo(HaveOccurred())

			_, err = reconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: pipelineRunName, Namespace: namespace},
			})
			Expect(err).NotTo(HaveOccurred())

			// Get the job and mark it as complete
			Eventually(func() error {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: pipelineRunName, Namespace: namespace}, pipelineRun)
				if err != nil {
					return err
				}
				if pipelineRun.Status.JobName == "" {
					return fmt.Errorf("job not created yet")
				}

				job := &batchv1.Job{}
				err = k8sClient.Get(ctx, types.NamespacedName{
					Name:      pipelineRun.Status.JobName,
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
				NamespacedName: types.NamespacedName{Name: pipelineRunName, Namespace: namespace},
			})
			Expect(err).NotTo(HaveOccurred())

			// Verify Succeeded condition is set
			Eventually(func() bool {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: pipelineRunName, Namespace: namespace}, pipelineRun)
				if err != nil {
					return false
				}
				succeededCond := meta.FindStatusCondition(pipelineRun.Status.Conditions, ConditionTypeSucceeded)
				return succeededCond != nil && succeededCond.Status == metav1.ConditionTrue
			}, timeout, interval).Should(BeTrue())

			// Verify CompletionTime is set
			Expect(pipelineRun.Status.CompletionTime).NotTo(BeNil())
		})

		It("should mark PipelineRun as Degraded when the backing Job fails", func() {
			mockValkey.ConsumerGroupLag = 0
			mockValkey.PendingCount = 0

			pipelineRun = &pipelinesv1alpha1.PipelineRun{
				ObjectMeta: metav1.ObjectMeta{
					Name:      pipelineRunName,
					Namespace: namespace,
				},
				Spec: pipelinesv1alpha1.PipelineRunSpec{
					PipelineRef: pipelinesv1alpha1.PipelineReference{
						Name: pipelineName,
					},
				},
			}
			Expect(k8sClient.Create(ctx, pipelineRun)).To(Succeed())

			// First reconcile initializes TotalFiles
			_, err := reconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: pipelineRunName, Namespace: namespace},
			})
			Expect(err).NotTo(HaveOccurred())

			// Ensure TotalFiles populated before job creation
			Eventually(func() bool {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: pipelineRunName, Namespace: namespace}, pipelineRun)
				return err == nil && pipelineRun.Status.Counts != nil && pipelineRun.Status.Counts.TotalFiles == mockValkey.StreamLength
			}, timeout, interval).Should(BeTrue())

			// Second reconcile should create the Job
			_, err = reconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: pipelineRunName, Namespace: namespace},
			})
			Expect(err).NotTo(HaveOccurred())

			job := &batchv1.Job{}
			Eventually(func() error {
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: pipelineRunName, Namespace: namespace}, pipelineRun); err != nil {
					return err
				}
				if pipelineRun.Status.JobName == "" {
					return fmt.Errorf("job name not set yet")
				}
				return k8sClient.Get(ctx, types.NamespacedName{
					Name:      pipelineRun.Status.JobName,
					Namespace: namespace,
				}, job)
			}, timeout, interval).Should(Succeed())

			// Simulate job failure
			now := metav1.Now()
			job.Status.StartTime = &now
			job.Status.Conditions = []batchv1.JobCondition{
				{
					Type:               batchv1.JobFailureTarget,
					Status:             corev1.ConditionTrue,
					Reason:             "BackoffLimitExceeded",
					Message:            "Job has reached the specified backoff limit",
					LastTransitionTime: now,
				},
				{
					Type:               batchv1.JobFailed,
					Status:             corev1.ConditionTrue,
					Reason:             "BackoffLimitExceeded",
					Message:            "Job has reached the specified backoff limit",
					LastTransitionTime: now,
				},
			}
			job.Status.Failed = 1
			Expect(k8sClient.Status().Update(ctx, job)).To(Succeed())

			// Reconcile to process the failed job state
			_, err = reconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: pipelineRunName, Namespace: namespace},
			})
			Expect(err).NotTo(HaveOccurred())

			// Validate degraded status conditions
			Eventually(func() bool {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: pipelineRunName, Namespace: namespace}, pipelineRun)
				if err != nil {
					return false
				}

				degradedCond := meta.FindStatusCondition(pipelineRun.Status.Conditions, ConditionTypeDegraded)
				progressingCond := meta.FindStatusCondition(pipelineRun.Status.Conditions, ConditionTypeProgressing)
				succeededCond := meta.FindStatusCondition(pipelineRun.Status.Conditions, ConditionTypeSucceeded)

				if degradedCond == nil || degradedCond.Status != metav1.ConditionTrue {
					return false
				}

				if progressingCond == nil || progressingCond.Status != metav1.ConditionFalse {
					return false
				}

				if succeededCond == nil || succeededCond.Status != metav1.ConditionFalse {
					return false
				}

				return pipelineRun.Status.CompletionTime != nil && degradedCond.Reason == "BackoffLimitExceeded"
			}, timeout, interval).Should(BeTrue())
		})

		It("should re-enqueue when a pod is stuck in ImagePullBackOff", func() {
			pipelineRun = &pipelinesv1alpha1.PipelineRun{
				ObjectMeta: metav1.ObjectMeta{
					Name:      pipelineRunName,
					Namespace: namespace,
				},
				Spec: pipelinesv1alpha1.PipelineRunSpec{
					PipelineRef: pipelinesv1alpha1.PipelineReference{
						Name: pipelineName,
					},
					Execution: &pipelinesv1alpha1.ExecutionConfig{
						MaxAttempts: ptr.To(int32(3)),
					},
				},
			}
			Expect(k8sClient.Create(ctx, pipelineRun)).To(Succeed())

			// Get the runID from the created PipelineRun
			Eventually(func() bool {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: pipelineRunName, Namespace: namespace}, pipelineRun)
				return err == nil && pipelineRun.UID != ""
			}, timeout, interval).Should(BeTrue())
			runID := pipelineRun.GetRunID()

			for i := 0; i < 2; i++ {
				_, err := reconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: pipelineRunName, Namespace: namespace},
				})
				Expect(err).NotTo(HaveOccurred())
			}

			Eventually(func() bool {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: pipelineRunName, Namespace: namespace}, pipelineRun)
				return err == nil && pipelineRun.Status.JobName != ""
			}, timeout, interval).Should(BeTrue())

			imagePullPod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      fmt.Sprintf("image-pull-pod-%d", testCounter),
					Namespace: namespace,
					Labels: map[string]string{
						"filter.plainsight.ai/run": runID,
					},
					Annotations: map[string]string{
						AnnotationMessageID: "msg-imagepull",
						AnnotationFile:      "videos/file1.mp4",
						AnnotationAttempts:  "1",
					},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "test-filter",
							Image: "ghcr.io/does/not-exist:latest",
						},
					},
				},
				Status: corev1.PodStatus{
					Phase: corev1.PodPending,
					ContainerStatuses: []corev1.ContainerStatus{
						{
							Name: "test-filter",
							State: corev1.ContainerState{
								Waiting: &corev1.ContainerStateWaiting{
									Reason:  "ImagePullBackOff",
									Message: "Back-off pulling image",
								},
							},
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, imagePullPod)).To(Succeed())

			// Seed queue mapping and message for this consumer
			mockValkey.PendingByConsumer = map[string][]string{
				imagePullPod.Name: {"msg-imagepull"},
			}
			mockValkey.Messages = []mockMessage{{
				ID: "msg-imagepull",
				Values: map[string]string{
					"file":     "videos/file1.mp4",
					"attempts": "1",
				},
			}}

			Eventually(func() error {
				existing := &corev1.Pod{}
				if err := k8sClient.Get(ctx, types.NamespacedName{
					Name:      imagePullPod.Name,
					Namespace: namespace,
				}, existing); err != nil {
					return err
				}
				existing.Status = corev1.PodStatus{
					Phase: corev1.PodPending,
					ContainerStatuses: []corev1.ContainerStatus{
						{
							Name: "test-filter",
							State: corev1.ContainerState{
								Waiting: &corev1.ContainerStateWaiting{
									Reason:  "ImagePullBackOff",
									Message: "Back-off pulling image",
								},
							},
						},
					},
				}
				return k8sClient.Status().Update(ctx, existing)
			}, timeout, interval).Should(Succeed())

			_, err := reconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: pipelineRunName, Namespace: namespace},
			})
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() bool {
				for _, entry := range mockValkey.EnqueuedFiles {
					if entry.RunID == runID && entry.Attempts == 2 && entry.Filepath == "videos/file1.mp4" {
						return true
					}
				}
				return false
			}, timeout, interval).Should(BeTrue())

			Expect(mockValkey.AckedMessages).To(ContainElement("msg-imagepull"))

			Eventually(func() bool {
				pod := &corev1.Pod{}
				err := k8sClient.Get(ctx, types.NamespacedName{
					Name:      imagePullPod.Name,
					Namespace: namespace,
				}, pod)
				return errors.IsNotFound(err)
			}, timeout, interval).Should(BeTrue())
		})

		It("should re-enqueue when a pod is crash looping", func() {
			pipelineRun = &pipelinesv1alpha1.PipelineRun{
				ObjectMeta: metav1.ObjectMeta{
					Name:      pipelineRunName,
					Namespace: namespace,
				},
				Spec: pipelinesv1alpha1.PipelineRunSpec{
					PipelineRef: pipelinesv1alpha1.PipelineReference{
						Name: pipelineName,
					},
					Execution: &pipelinesv1alpha1.ExecutionConfig{
						MaxAttempts: ptr.To(int32(2)),
					},
				},
			}
			Expect(k8sClient.Create(ctx, pipelineRun)).To(Succeed())

			// Get the runID from the created PipelineRun
			Eventually(func() bool {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: pipelineRunName, Namespace: namespace}, pipelineRun)
				return err == nil && pipelineRun.UID != ""
			}, timeout, interval).Should(BeTrue())
			runID := pipelineRun.GetRunID()

			for i := 0; i < 2; i++ {
				_, err := reconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: pipelineRunName, Namespace: namespace},
				})
				Expect(err).NotTo(HaveOccurred())
			}

			Eventually(func() bool {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: pipelineRunName, Namespace: namespace}, pipelineRun)
				return err == nil && pipelineRun.Status.JobName != ""
			}, timeout, interval).Should(BeTrue())

			crashLoopPod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      fmt.Sprintf("crashloop-pod-%d", testCounter),
					Namespace: namespace,
					Labels: map[string]string{
						"filter.plainsight.ai/run": runID,
					},
					Annotations: map[string]string{
						AnnotationMessageID: "msg-crashloop",
						AnnotationFile:      "videos/file2.mp4",
						AnnotationAttempts:  "1",
					},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "test-filter",
							Image: "example/crashloop:latest",
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, crashLoopPod)).To(Succeed())

			// Seed queue mapping and message for this consumer
			mockValkey.PendingByConsumer = map[string][]string{
				crashLoopPod.Name: {"msg-crashloop"},
			}
			mockValkey.Messages = []mockMessage{{
				ID: "msg-crashloop",
				Values: map[string]string{
					"file":     "videos/file2.mp4",
					"attempts": "1",
				},
			}}

			Eventually(func() error {
				existing := &corev1.Pod{}
				if err := k8sClient.Get(ctx, types.NamespacedName{
					Name:      crashLoopPod.Name,
					Namespace: namespace,
				}, existing); err != nil {
					return err
				}
				existing.Status = corev1.PodStatus{
					Phase: corev1.PodPending,
					ContainerStatuses: []corev1.ContainerStatus{
						{
							Name: "test-filter",
							State: corev1.ContainerState{
								Waiting: &corev1.ContainerStateWaiting{
									Reason:  "CrashLoopBackOff",
									Message: "back-off 5m0s restarting failed container",
								},
							},
							LastTerminationState: corev1.ContainerState{
								Terminated: &corev1.ContainerStateTerminated{
									ExitCode: 137,
									Reason:   "OOMKilled",
								},
							},
						},
					},
				}
				return k8sClient.Status().Update(ctx, existing)
			}, timeout, interval).Should(Succeed())

			_, err := reconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: pipelineRunName, Namespace: namespace},
			})
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() bool {
				for _, entry := range mockValkey.DLQEntries {
					if entry.RunID == runID && entry.Attempts == 2 && entry.Filepath == "videos/file2.mp4" {
						return true
					}
				}
				return false
			}, timeout, interval).Should(BeTrue())

			Expect(mockValkey.AckedMessages).To(ContainElement("msg-crashloop"))

			Eventually(func() bool {
				pod := &corev1.Pod{}
				err := k8sClient.Get(ctx, types.NamespacedName{
					Name:      crashLoopPod.Name,
					Namespace: namespace,
				}, pod)
				return errors.IsNotFound(err)
			}, timeout, interval).Should(BeTrue())
		})

		It("should handle Pipeline not found error", func() {
			pipelineRun = &pipelinesv1alpha1.PipelineRun{
				ObjectMeta: metav1.ObjectMeta{
					Name:      pipelineRunName,
					Namespace: namespace,
				},
				Spec: pipelinesv1alpha1.PipelineRunSpec{
					PipelineRef: pipelinesv1alpha1.PipelineReference{
						Name: "nonexistent-pipeline",
					},
				},
			}
			Expect(k8sClient.Create(ctx, pipelineRun)).To(Succeed())

			// Reconcile should handle error gracefully
			_, err := reconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: pipelineRunName, Namespace: namespace},
			})

			Expect(err).To(HaveOccurred())

			// Verify Degraded condition is set
			Eventually(func() bool {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: pipelineRunName, Namespace: namespace}, pipelineRun)
				if err != nil {
					return false
				}
				degradedCond := meta.FindStatusCondition(pipelineRun.Status.Conditions, ConditionTypeDegraded)
				return degradedCond != nil && degradedCond.Status == metav1.ConditionTrue
			}, timeout, interval).Should(BeTrue())
		})

		It("should build streaming Deployment without a ServiceAccountName", func() {
			// Create a streaming Pipeline
			pipelineName = fmt.Sprintf("test-pipeline-stream-%d", testCounter)
			streamPipeline := &pipelinesv1alpha1.Pipeline{
				ObjectMeta: metav1.ObjectMeta{
					Name:      pipelineName,
					Namespace: namespace,
				},
				Spec: pipelinesv1alpha1.PipelineSpec{
					Mode: pipelinesv1alpha1.PipelineModeStream,
					Source: pipelinesv1alpha1.Source{
						RTSP: &pipelinesv1alpha1.RTSPSource{Host: "rtsp-video-stream", Port: 8554, Path: "/live"},
					},
					Filters: []pipelinesv1alpha1.Filter{
						{Name: "video-in", Image: "busybox:latest"},
					},
				},
			}
			Expect(k8sClient.Create(ctx, streamPipeline)).To(Succeed())

			// Create PipelineRun
			pipelineRunName = fmt.Sprintf("test-pipelinerun-stream-%d", testCounter)
			pipelineRun = &pipelinesv1alpha1.PipelineRun{
				ObjectMeta: metav1.ObjectMeta{
					Name:      pipelineRunName,
					Namespace: namespace,
				},
				Spec: pipelinesv1alpha1.PipelineRunSpec{
					PipelineRef: pipelinesv1alpha1.PipelineReference{Name: pipelineName},
				},
			}
			Expect(k8sClient.Create(ctx, pipelineRun)).To(Succeed())

			// Reconcile to create Deployment
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: pipelineRunName, Namespace: namespace}})
			Expect(err).NotTo(HaveOccurred())

			// Deployment name should be recorded in status
			Eventually(func() bool {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: pipelineRunName, Namespace: namespace}, pipelineRun)
				return err == nil && pipelineRun.Status.Streaming != nil && pipelineRun.Status.Streaming.DeploymentName != ""
			}, timeout, interval).Should(BeTrue())

			// Deployment should not specify serviceAccountName
			dep := &appsv1.Deployment{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: pipelineRun.Status.Streaming.DeploymentName, Namespace: namespace}, dep)).To(Succeed())
			Expect(dep.Spec.Template.Spec.ServiceAccountName).To(Equal(""))
		})
	})
})
