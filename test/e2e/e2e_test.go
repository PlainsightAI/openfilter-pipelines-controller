//go:build e2e
// +build e2e

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

package e2e

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/PlainsightAI/openfilter-pipelines-controller/test/utils"
)

// namespace where the project is deployed in
const namespace = "openfilter-pipelines-controller-system"

// serviceAccountName created for the project
const serviceAccountName = "openfilter-pipelines-controller-controller-manager"

// metricsServiceName is the name of the metrics service of the project
const metricsServiceName = "openfilter-pipelines-controller-metrics-service"

// metricsRoleBindingName is the name of the RBAC that will be created to allow get the metrics data
const metricsRoleBindingName = "openfilter-pipelines-controller-metrics-binding"

var _ = Describe("Manager", Ordered, func() {
	var controllerPodName string

	// Before running the tests, set up the environment by creating the namespace,
	// enforce the restricted security policy to the namespace, installing CRDs,
	// and deploying the controller.
	BeforeAll(func() {
		By("creating manager namespace")
		cmd := exec.Command("kubectl", "create", "ns", namespace)
		_, err := utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to create namespace")

		By("labeling the namespace to enforce the restricted security policy")
		cmd = exec.Command("kubectl", "label", "--overwrite", "ns", namespace,
			"pod-security.kubernetes.io/enforce=restricted")
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to label namespace with restricted policy")

		By("installing CRDs")
		cmd = exec.Command("make", "install")
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to install CRDs")

		By("deploying the controller-manager with test configuration")
		// Use the testing overlay which includes Valkey
		cmd = exec.Command("kubectl", "apply", "-k", "config/testing")
		output, err := utils.Run(cmd)
		if err != nil {
			fmt.Fprintf(GinkgoWriter, "Deploy output: %s\n", output)
		}
		Expect(err).NotTo(HaveOccurred(), "Failed to deploy the controller-manager")

		// Patch the controller image
		cmd = exec.Command("kubectl", "set", "image", "deployment/openfilter-pipelines-controller-controller-manager",
			fmt.Sprintf("manager=%s", projectImage), "-n", namespace)
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to set controller image")
	})

	// After all tests have been executed, clean up by undeploying the controller, uninstalling CRDs,
	// and deleting the namespace.
	AfterAll(func() {
		By("cleaning up the curl pod for metrics")
		cmd := exec.Command("kubectl", "delete", "pod", "curl-metrics", "-n", namespace)
		_, _ = utils.Run(cmd)

		By("cleaning up the ClusterRoleBinding")
		cmd = exec.Command("kubectl", "delete", "clusterrolebinding", metricsRoleBindingName, "--ignore-not-found")
		_, _ = utils.Run(cmd)

		By("undeploying the controller-manager")
		cmd = exec.Command("kubectl", "delete", "-k", "config/testing", "--ignore-not-found")
		_, _ = utils.Run(cmd)

		By("uninstalling CRDs")
		cmd = exec.Command("make", "uninstall")
		_, _ = utils.Run(cmd)

		By("removing manager namespace")
		cmd = exec.Command("kubectl", "delete", "ns", namespace)
		_, _ = utils.Run(cmd)
	})

	// After each test, check for failures and collect logs, events,
	// and pod descriptions for debugging.
	AfterEach(func() {
		specReport := CurrentSpecReport()
		if specReport.Failed() {
			By("Fetching controller manager pod logs")
			cmd := exec.Command("kubectl", "logs", controllerPodName, "-n", namespace)
			controllerLogs, err := utils.Run(cmd)
			if err == nil {
				_, _ = fmt.Fprintf(GinkgoWriter, "Controller logs:\n %s", controllerLogs)
			} else {
				_, _ = fmt.Fprintf(GinkgoWriter, "Failed to get Controller logs: %s", err)
			}

			By("Fetching Kubernetes events")
			cmd = exec.Command("kubectl", "get", "events", "-n", namespace, "--sort-by=.lastTimestamp")
			eventsOutput, err := utils.Run(cmd)
			if err == nil {
				_, _ = fmt.Fprintf(GinkgoWriter, "Kubernetes events:\n%s", eventsOutput)
			} else {
				_, _ = fmt.Fprintf(GinkgoWriter, "Failed to get Kubernetes events: %s", err)
			}

			By("Fetching curl-metrics logs")
			cmd = exec.Command("kubectl", "logs", "curl-metrics", "-n", namespace)
			metricsOutput, err := utils.Run(cmd)
			if err == nil {
				_, _ = fmt.Fprintf(GinkgoWriter, "Metrics logs:\n %s", metricsOutput)
			} else {
				_, _ = fmt.Fprintf(GinkgoWriter, "Failed to get curl-metrics logs: %s", err)
			}

			By("Fetching controller manager pod description")
			cmd = exec.Command("kubectl", "describe", "pod", controllerPodName, "-n", namespace)
			podDescription, err := utils.Run(cmd)
			if err == nil {
				fmt.Println("Pod description:\n", podDescription)
			} else {
				fmt.Println("Failed to describe controller pod")
			}
		}
	})

	SetDefaultEventuallyTimeout(2 * time.Minute)
	SetDefaultEventuallyPollingInterval(time.Second)

	Context("Manager", func() {
		It("should run successfully", func() {
			By("validating that the controller-manager pod is running as expected")
			verifyControllerUp := func(g Gomega) {
				// Get the name of the controller-manager pod
				cmd := exec.Command("kubectl", "get",
					"pods", "-l", "control-plane=controller-manager",
					"-o", "go-template={{ range .items }}"+
						"{{ if not .metadata.deletionTimestamp }}"+
						"{{ .metadata.name }}"+
						"{{ \"\\n\" }}{{ end }}{{ end }}",
					"-n", namespace,
				)

				podOutput, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred(), "Failed to retrieve controller-manager pod information")
				podNames := utils.GetNonEmptyLines(podOutput)
				g.Expect(podNames).To(HaveLen(1), "expected 1 controller pod running")
				controllerPodName = podNames[0]
				g.Expect(controllerPodName).To(ContainSubstring("controller-manager"))

				// Validate the pod's status
				cmd = exec.Command("kubectl", "get",
					"pods", controllerPodName, "-o", "jsonpath={.status.phase}",
					"-n", namespace,
				)
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("Running"), "Incorrect controller-manager pod status")
			}
			Eventually(verifyControllerUp).Should(Succeed())
		})

		It("should ensure the metrics endpoint is serving metrics", func() {
			By("creating a ClusterRoleBinding for the service account to allow access to metrics")
			cmd := exec.Command("kubectl", "create", "clusterrolebinding", metricsRoleBindingName,
				"--clusterrole=openfilter-pipelines-controller-metrics-reader",
				fmt.Sprintf("--serviceaccount=%s:%s", namespace, serviceAccountName),
			)
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to create ClusterRoleBinding")

			By("validating that the metrics service is available")
			cmd = exec.Command("kubectl", "get", "service", metricsServiceName, "-n", namespace)
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Metrics service should exist")

			By("getting the service account token")
			token, err := serviceAccountToken()
			Expect(err).NotTo(HaveOccurred())
			Expect(token).NotTo(BeEmpty())

			By("waiting for the metrics endpoint to be ready")
			verifyMetricsEndpointReady := func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "endpoints", metricsServiceName, "-n", namespace)
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(ContainSubstring("8080"), "Metrics endpoint is not ready")
			}
			Eventually(verifyMetricsEndpointReady).Should(Succeed())

			By("verifying that the controller manager is serving the metrics server")
			verifyMetricsServerStarted := func(g Gomega) {
				cmd := exec.Command("kubectl", "logs", controllerPodName, "-n", namespace)
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(ContainSubstring("controller-runtime.metrics\tServing metrics server"),
					"Metrics server not yet started")
			}
			Eventually(verifyMetricsServerStarted).Should(Succeed())

			By("creating the curl-metrics pod to access the metrics endpoint")
			cmd = exec.Command("kubectl", "run", "curl-metrics", "--restart=Never",
				"--namespace", namespace,
				"--image=curlimages/curl:latest",
				"--overrides",
				fmt.Sprintf(`{
					"spec": {
						"containers": [{
							"name": "curl",
							"image": "curlimages/curl:latest",
							"command": ["/bin/sh", "-c"],
							"args": ["curl -v http://%s.%s.svc.cluster.local:8080/metrics"],
							"securityContext": {
								"readOnlyRootFilesystem": true,
								"allowPrivilegeEscalation": false,
								"capabilities": {
									"drop": ["ALL"]
								},
								"runAsNonRoot": true,
								"runAsUser": 1000,
								"seccompProfile": {
									"type": "RuntimeDefault"
								}
							}
						}],
						"serviceAccountName": "%s"
					}
				}`, metricsServiceName, namespace, serviceAccountName))
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to create curl-metrics pod")

			By("waiting for the curl-metrics pod to complete.")
			verifyCurlUp := func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "pods", "curl-metrics",
					"-o", "jsonpath={.status.phase}",
					"-n", namespace)
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("Succeeded"), "curl pod in wrong status")
			}
			Eventually(verifyCurlUp, 5*time.Minute).Should(Succeed())

			By("getting the metrics by checking curl-metrics logs")
			verifyMetricsAvailable := func(g Gomega) {
				metricsOutput, err := getMetricsOutput()
				g.Expect(err).NotTo(HaveOccurred(), "Failed to retrieve logs from curl pod")
				g.Expect(metricsOutput).NotTo(BeEmpty())
				g.Expect(metricsOutput).To(ContainSubstring("< HTTP/1.1 200 OK"))
			}
			Eventually(verifyMetricsAvailable, 2*time.Minute).Should(Succeed())
		})

		// +kubebuilder:scaffold:e2e-webhooks-checks

		// TODO: Customize the e2e test suite with scenarios specific to your project.
		// Consider applying sample/CR(s) and check their status and/or verifying
		// the reconciliation by using the metrics, i.e.:
		// metricsOutput, err := getMetricsOutput()
		// Expect(err).NotTo(HaveOccurred(), "Failed to retrieve logs from curl pod")
		// Expect(metricsOutput).To(ContainSubstring(
		//    fmt.Sprintf(`controller_runtime_reconcile_total{controller="%s",result="success"} 1`,
		//    strings.ToLower(<Kind>),
		// ))
	})

	// Filter image volumes (PLAT-1097): a streaming pipeline whose filter
	// mounts an OCI image as a read-only volume. Streaming is used (not
	// batch) because its pods run the filter containers directly — no
	// claimer waiting on Valkey/S3 — so the pod goes Ready if and only if
	// the kubelet actually pulled and mounted the image volume. Requires
	// K8s 1.35+ (first release with the ImageVolume gate on by default);
	// skipped on older clusters, where the controller rejects such
	// instances at admission (PLAT-1096) instead.
	Context("Filter image volumes", func() {
		const (
			ivNamespace = "default"
			ivPipeline  = "e2e-imagevolume-pipeline"
			ivSource    = "e2e-imagevolume-source"
			ivInstance  = "e2e-imagevolume-instance"
			// The "model" OCI image: busybox's filesystem must appear at
			// the mount path (checked via /bin/busybox below).
			ivModelImage = "busybox:1.36"
			ivMountPath  = "/opt/model"
		)
		ivDeployment := ivInstance + "-deploy"

		ivManifests := fmt.Sprintf(`
apiVersion: filter.plainsight.ai/v1alpha1
kind: Pipeline
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  mode: stream
  filters:
    - name: sleeper
      image: busybox:1.36
      command: ["/bin/sh", "-c"]
      args: ["sleep 3600"]
      imageVolumes:
        - name: model
          image: %[3]s
          mountPath: %[4]s
---
apiVersion: filter.plainsight.ai/v1alpha1
kind: PipelineSource
metadata:
  name: %[5]s
  namespace: %[2]s
spec:
  rtsp:
    host: rtsp-nonexistent.default
    port: 8554
    path: /stream
---
apiVersion: filter.plainsight.ai/v1alpha1
kind: PipelineInstance
metadata:
  name: %[6]s
  namespace: %[2]s
spec:
  pipelineRef:
    name: %[1]s
  sourceRef:
    name: %[5]s
`, ivPipeline, ivNamespace, ivModelImage, ivMountPath, ivSource, ivInstance)

		ivManifestFile := filepath.Join(os.TempDir(), "e2e-image-volume-crs.yaml")

		BeforeAll(func() {
			By("checking the cluster serves the image volume source by default (K8s 1.35+)")
			major, minor := serverVersion()
			if major == 1 && minor < 35 {
				Skip(fmt.Sprintf("cluster is v%d.%d; the image volume source is default-enabled from 1.35", major, minor))
			}

			By("applying the image-volume Pipeline, PipelineSource, and PipelineInstance")
			Expect(os.WriteFile(ivManifestFile, []byte(ivManifests), os.FileMode(0o644))).To(Succeed())
			cmd := exec.Command("kubectl", "apply", "-f", ivManifestFile)
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to apply image-volume CRs")
		})

		AfterAll(func() {
			// Wait for full deletion (finalizer processing) while the
			// controller is still deployed: the Manager-level teardown that
			// follows deletes the CRDs via `kubectl delete -k`, and a
			// PipelineInstance still carrying finalizers at that point wedges
			// the CRD in Terminating and hangs the whole teardown.
			By("deleting the image-volume CRs and waiting for finalizers")
			cmd := exec.Command("kubectl", "delete", "-f", ivManifestFile, "--ignore-not-found", "--timeout=120s")
			if _, err := utils.Run(cmd); err != nil {
				// The controller did not process the finalizers in time.
				// Dump its logs for diagnosis, then strip the finalizers so
				// the suite-level teardown cannot wedge on a Terminating CR.
				_, _ = fmt.Fprintf(GinkgoWriter, "graceful CR deletion failed (%v); dumping controller logs and force-removing finalizers\n", err)
				logsCmd := exec.Command("kubectl", "logs", "-n", namespace, "-l", "control-plane=controller-manager", "--tail=100")
				if out, lerr := utils.Run(logsCmd); lerr == nil {
					_, _ = fmt.Fprintf(GinkgoWriter, "controller logs:\n%s\n", out)
				}
				patchCmd := exec.Command("kubectl", "patch", "pipelineinstance", ivInstance, "-n", ivNamespace,
					"--type=merge", "-p", `{"metadata":{"finalizers":null}}`)
				_, _ = utils.Run(patchCmd)
			}
			_ = os.Remove(ivManifestFile)
		})

		It("renders the image volume into the streaming pod spec", func() {
			By("waiting for the streaming Deployment to exist")
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "deployment", ivDeployment, "-n", ivNamespace)
				_, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred(), "streaming Deployment not created yet")
			}).Should(Succeed())

			By("asserting the pod template has the image-source volume")
			cmd := exec.Command("kubectl", "get", "deployment", ivDeployment, "-n", ivNamespace,
				"-o", "jsonpath={.spec.template.spec.volumes[0].image.reference}")
			out, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			Expect(out).To(Equal(ivModelImage))

			By("asserting the filter container has the read-only mount")
			cmd = exec.Command("kubectl", "get", "deployment", ivDeployment, "-n", ivNamespace,
				"-o", "jsonpath={.spec.template.spec.containers[0].volumeMounts[0]}")
			out, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			Expect(out).To(ContainSubstring(fmt.Sprintf(`"mountPath":"%s"`, ivMountPath)))
			Expect(out).To(ContainSubstring(`"readOnly":true`))
		})

		It("runs the pod with the OCI image filesystem mounted", func() {
			By("waiting for the streaming pod to become Ready (kubelet pulled and mounted the image volume)")
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "wait", "--for=condition=Ready", "pod",
					"-l", fmt.Sprintf("pipelineinstance=%s", ivInstance),
					"-n", ivNamespace, "--timeout=10s")
				_, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred(), "streaming pod not Ready yet")
			}, 4*time.Minute).Should(Succeed())

			By("asserting the mounted image's filesystem is visible in the filter container")
			cmd := exec.Command("kubectl", "exec", "deploy/"+ivDeployment, "-n", ivNamespace,
				"--", "ls", ivMountPath+"/bin/busybox")
			out, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "mounted image filesystem not visible: %s", out)
		})
	})
})

// serverVersion returns the cluster's major/minor server version, tolerating
// vendor suffixes like "33+" in the minor field.
func serverVersion() (int, int) {
	GinkgoHelper()
	cmd := exec.Command("kubectl", "version", "-o", "json")
	out, err := utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred(), "Failed to query server version")
	var v struct {
		ServerVersion struct {
			Major string `json:"major"`
			Minor string `json:"minor"`
		} `json:"serverVersion"`
	}
	Expect(json.Unmarshal([]byte(out), &v)).To(Succeed())
	digits := func(s string) int {
		n, err := strconv.Atoi(strings.TrimRight(s, "+"))
		Expect(err).NotTo(HaveOccurred(), "unparseable server version component %q", s)
		return n
	}
	return digits(v.ServerVersion.Major), digits(v.ServerVersion.Minor)
}

// serviceAccountToken returns a token for the specified service account in the given namespace.
// It uses the Kubernetes TokenRequest API to generate a token by directly sending a request
// and parsing the resulting token from the API response.
func serviceAccountToken() (string, error) {
	const tokenRequestRawString = `{
		"apiVersion": "authentication.k8s.io/v1",
		"kind": "TokenRequest"
	}`

	// Temporary file to store the token request
	secretName := fmt.Sprintf("%s-token-request", serviceAccountName)
	tokenRequestFile := filepath.Join("/tmp", secretName)
	err := os.WriteFile(tokenRequestFile, []byte(tokenRequestRawString), os.FileMode(0o644))
	if err != nil {
		return "", err
	}

	var out string
	verifyTokenCreation := func(g Gomega) {
		// Execute kubectl command to create the token
		cmd := exec.Command("kubectl", "create", "--raw", fmt.Sprintf(
			"/api/v1/namespaces/%s/serviceaccounts/%s/token",
			namespace,
			serviceAccountName,
		), "-f", tokenRequestFile)

		output, err := cmd.CombinedOutput()
		g.Expect(err).NotTo(HaveOccurred())

		// Parse the JSON output to extract the token
		var token tokenRequest
		err = json.Unmarshal(output, &token)
		g.Expect(err).NotTo(HaveOccurred())

		out = token.Status.Token
	}
	Eventually(verifyTokenCreation).Should(Succeed())

	return out, err
}

// getMetricsOutput retrieves and returns the logs from the curl pod used to access the metrics endpoint.
func getMetricsOutput() (string, error) {
	By("getting the curl-metrics logs")
	cmd := exec.Command("kubectl", "logs", "curl-metrics", "-n", namespace)
	return utils.Run(cmd)
}

// tokenRequest is a simplified representation of the Kubernetes TokenRequest API response,
// containing only the token field that we need to extract.
type tokenRequest struct {
	Status struct {
		Token string `json:"token"`
	} `json:"status"`
}
