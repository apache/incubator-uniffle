/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package inspector

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubefake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/pointer"

	uniffleapi "github.com/apache/incubator-uniffle/deploy/kubernetes/operator/api/uniffle/v1alpha1"
	"github.com/apache/incubator-uniffle/deploy/kubernetes/operator/pkg/constants"
	"github.com/apache/incubator-uniffle/deploy/kubernetes/operator/pkg/controller/sync/shuffleserver"
	"github.com/apache/incubator-uniffle/deploy/kubernetes/operator/pkg/generated/clientset/versioned/fake"
	"github.com/apache/incubator-uniffle/deploy/kubernetes/operator/pkg/utils"
	"github.com/apache/incubator-uniffle/deploy/kubernetes/operator/pkg/webhook/config"
)

// TestDeletingShuffleServer tests delete shuffle server in rss-webhook.
func TestDeletingShuffleServer(t *testing.T) {
	testInspector := newInspector(&config.Config{}, nil)

	rssWithCooNodePort := wrapRssObj(func(rss *uniffleapi.RemoteShuffleService) {
		rss.Spec.Coordinator.Count = pointer.Int32(2)
		rss.Spec.Coordinator.RPCNodePort = []int32{30001, 30002}
		rss.Spec.Coordinator.HTTPNodePort = []int32{30011, 30012}
		rss.Spec.Coordinator.ExcludeNodesFilePath = ""
	})

	rssWithRunningStatus := rssWithCooNodePort.DeepCopy()
	rssWithRunningStatus.Status.Phase = uniffleapi.RSSRunning

	rssWithTerminatingStatus := rssWithCooNodePort.DeepCopy()
	rssWithTerminatingStatus.Status.Phase = uniffleapi.RSSTerminating

	rssWithUpgradingStatus := rssWithCooNodePort.DeepCopy()
	rssWithUpgradingStatus.Status.Phase = uniffleapi.RSSUpgrading

	_ = testInspector.rssInformer.GetIndexer().Add(rssWithCooNodePort)
	testNodeName := "node-1"
	for _, tt := range []struct {
		name    string
		rss     *uniffleapi.RemoteShuffleService
		pod     *corev1.Pod
		allowed bool
	}{
		{
			name:    "delete pod in running status on rss in running status",
			rss:     rssWithRunningStatus,
			pod:     buildTestShuffleServerPod(corev1.PodRunning, testNodeName),
			allowed: false,
		},
		{
			name:    "delete pod in failed status on rss in running status",
			rss:     rssWithRunningStatus,
			pod:     buildTestShuffleServerPod(corev1.PodFailed, testNodeName),
			allowed: true,
		},
		{
			name:    "delete pod in succeeded status on rss in running status",
			rss:     rssWithRunningStatus,
			pod:     buildTestShuffleServerPod(corev1.PodSucceeded, testNodeName),
			allowed: true,
		},
		{
			name:    "delete unscheduled pod rss in running status",
			rss:     rssWithRunningStatus,
			pod:     buildTestShuffleServerPod(corev1.PodPending, ""),
			allowed: true,
		},
		{
			name:    "delete pod in running status on rss in terminating status",
			rss:     rssWithTerminatingStatus,
			pod:     buildTestShuffleServerPod(corev1.PodRunning, testNodeName),
			allowed: true,
		},
		{
			name:    "delete pod in failed status on rss in terminating status",
			rss:     rssWithTerminatingStatus,
			pod:     buildTestShuffleServerPod(corev1.PodFailed, testNodeName),
			allowed: true,
		},
		{
			name:    "delete pod in succeeded status on rss in terminating status",
			rss:     rssWithRunningStatus,
			pod:     buildTestShuffleServerPod(corev1.PodSucceeded, testNodeName),
			allowed: true,
		},
		{
			name:    "delete unscheduled pod rss in terminating status",
			rss:     rssWithRunningStatus,
			pod:     buildTestShuffleServerPod(corev1.PodPending, ""),
			allowed: true,
		},
		{
			name:    "delete pod in running status on rss in upgrading status",
			rss:     rssWithUpgradingStatus,
			pod:     buildTestShuffleServerPod(corev1.PodRunning, testNodeName),
			allowed: true,
		},
		{
			name:    "delete pod in failed status on rss in upgrading status",
			rss:     rssWithUpgradingStatus,
			pod:     buildTestShuffleServerPod(corev1.PodFailed, testNodeName),
			allowed: true,
		},
		{
			name:    "delete pod in succeeded status on rss in upgrading status",
			rss:     rssWithRunningStatus,
			pod:     buildTestShuffleServerPod(corev1.PodSucceeded, testNodeName),
			allowed: true,
		},
		{
			name:    "delete unscheduled pod rss in upgrading status",
			rss:     rssWithRunningStatus,
			pod:     buildTestShuffleServerPod(corev1.PodPending, ""),
			allowed: true,
		},
	} {
		t.Run(tt.name, func(tc *testing.T) {
			canBeDeleted := testInspector.ifShuffleServerCanBeDeleted(tt.rss, tt.pod)
			if canBeDeleted != tt.allowed {
				tc.Errorf("invalid 'allowed' field in response: %v <> %v", canBeDeleted, tt.allowed)
			}
		})
	}
}

// TestDeletingShuffleServerWithHPA tests deleting shuffle server when using hpa objects.
func TestDeletingShuffleServerWithHPA(t *testing.T) {
	rss := utils.BuildRSSWithDefaultValue()
	rss.Name = "test"
	rss.Spec.ShuffleServer.Autoscaler = uniffleapi.RSSAutoscaler{
		Enable: true,
	}

	hpa, _ := shuffleserver.GenerateHPA(rss)
	hpa.Status.DesiredReplicas = 3
	_, sts := shuffleserver.GenerateShuffleServers(nil, rss)
	sts.Spec.Replicas = pointer.Int32(hpa.Status.DesiredReplicas)
	sts.Status.Replicas = 5

	rssClient := fake.NewSimpleClientset(rss)
	kubeClient := kubefake.NewSimpleClientset(sts, hpa)
	testInspector := newInspector(&config.Config{
		GenericConfig: utils.GenericConfig{
			KubeClient: kubeClient,
			RSSClient:  rssClient,
		},
	}, nil)
	if err := testInspector.startInformerFactories(context.TODO()); err != nil {
		t.Errorf("failed to start test inspector: %v", err)
	}

	testNodeName := "node-2"
	testPod := buildTestShuffleServerPod(corev1.PodRunning, testNodeName)
	if canBeDeleted := testInspector.ifShuffleServerCanBeDeleted(rss, testPod); !canBeDeleted {
		t.Errorf("invalid 'allowed' field in response: %v <> %v", canBeDeleted, true)
	}
}

func buildTestShuffleServerPod(podPhase corev1.PodPhase, nodeName string) *corev1.Pod {
	testShuffleServerPodName := constants.RSSShuffleServer + "-test-0"
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testShuffleServerPodName,
			Namespace: corev1.NamespaceDefault,
			Annotations: map[string]string{
				constants.AnnotationShuffleServerPort: "8080",
				constants.AnnotationRssName:           "rss",
			},
			Labels: map[string]string{
				appsv1.ControllerRevisionHashLabelKey: "test-revision-1",
				constants.LabelShuffleServer:          "true",
			},
		},
		Spec: corev1.PodSpec{
			NodeName: nodeName,
		},
		Status: corev1.PodStatus{
			PodIP: "xxx.xxx.xxx.xxx",
			Phase: podPhase,
		},
	}
}
