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
	"encoding/json"
	"testing"

	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/pointer"

	unifflev1alpha1 "github.com/apache/incubator-uniffle/deploy/kubernetes/operator/api/uniffle/v1alpha1"
	"github.com/apache/incubator-uniffle/deploy/kubernetes/operator/pkg/webhook/config"
)

func wrapTestRssObj(rss *unifflev1alpha1.RemoteShuffleService) *unifflev1alpha1.RemoteShuffleService {
	rss.Name = "test"
	rss.Namespace = corev1.NamespaceDefault
	rss.UID = "uid-test"
	return rss
}

// convertRssToRawExtension converts a rss object to runtime.RawExtension for testing.
func convertRssToRawExtension(rss *unifflev1alpha1.RemoteShuffleService) (runtime.RawExtension, error) {
	if rss == nil {
		return convertRssToRawExtension(&unifflev1alpha1.RemoteShuffleService{})
	}
	body, err := json.Marshal(rss)
	if err != nil {
		return runtime.RawExtension{}, err
	}
	return runtime.RawExtension{
		Raw:    body,
		Object: rss,
	}, nil
}

// buildTestAdmissionReview builds an AdmissionReview object for testing.
func buildTestAdmissionReview(op admissionv1.Operation,
	oldRss, newRss *unifflev1alpha1.RemoteShuffleService) *admissionv1.AdmissionReview {
	oldObject, err := convertRssToRawExtension(oldRss)
	if err != nil {
		panic(err)
	}
	var object runtime.RawExtension
	object, err = convertRssToRawExtension(newRss)
	if err != nil {
		panic(err)
	}
	return &admissionv1.AdmissionReview{
		Request: &admissionv1.AdmissionRequest{
			Operation: op,
			Object:    object,
			OldObject: oldObject,
		},
	}
}

// TestValidateRSS tests rss objects' validation in rss-webhook.
func TestValidateRSS(t *testing.T) {
	testInspector := newInspector(&config.Config{}, nil)

	rssWithCooNodePort := &unifflev1alpha1.RemoteShuffleService{
		Spec: unifflev1alpha1.RemoteShuffleServiceSpec{
			Coordinator: &unifflev1alpha1.CoordinatorConfig{
				Count:        pointer.Int32(2),
				RPCNodePort:  []int32{30001, 30002},
				HTTPNodePort: []int32{30011, 30012},
			},
		},
	}

	rssWithoutLogInCooMounts := rssWithCooNodePort.DeepCopy()
	rssWithoutLogInCooMounts.Spec.Coordinator.ExcludeNodesFilePath = "/exclude_nodes"
	rssWithoutLogInCooMounts.Spec.Coordinator.CommonConfig = &unifflev1alpha1.CommonConfig{
		RSSPodSpec: &unifflev1alpha1.RSSPodSpec{
			LogHostPath:    "/data/logs",
			HostPathMounts: map[string]string{},
		},
	}

	rssWithoutLogInServerMounts := rssWithoutLogInCooMounts.DeepCopy()
	rssWithoutLogInServerMounts.Spec.Coordinator.CommonConfig.RSSPodSpec.HostPathMounts["/data/logs"] = "/data/logs"
	rssWithoutLogInServerMounts.Spec.ShuffleServer = &unifflev1alpha1.ShuffleServerConfig{
		CommonConfig: &unifflev1alpha1.CommonConfig{
			RSSPodSpec: &unifflev1alpha1.RSSPodSpec{
				LogHostPath:    "/data/logs",
				HostPathMounts: map[string]string{},
			},
		},
	}

	rssWithoutPartition := rssWithoutLogInServerMounts.DeepCopy()
	rssWithoutPartition.Spec.ShuffleServer.CommonConfig.RSSPodSpec.HostPathMounts["/data/logs"] = "/data/logs"
	rssWithoutPartition.Spec.ShuffleServer.UpgradeStrategy = &unifflev1alpha1.ShuffleServerUpgradeStrategy{
		Type: unifflev1alpha1.PartitionUpgrade,
	}

	rssWithInvalidPartition := rssWithoutLogInServerMounts.DeepCopy()
	rssWithInvalidPartition.Spec.ShuffleServer.CommonConfig.RSSPodSpec.HostPathMounts["/data/logs"] = "/data/logs"
	rssWithInvalidPartition.Spec.ShuffleServer.UpgradeStrategy = &unifflev1alpha1.ShuffleServerUpgradeStrategy{
		Type:      unifflev1alpha1.PartitionUpgrade,
		Partition: pointer.Int32(-1),
	}

	rssWithoutSpecificNames := rssWithoutLogInServerMounts.DeepCopy()
	rssWithoutSpecificNames.Spec.ShuffleServer.CommonConfig.RSSPodSpec.HostPathMounts["/data/logs"] = "/data/logs"
	rssWithoutSpecificNames.Spec.ShuffleServer.UpgradeStrategy = &unifflev1alpha1.ShuffleServerUpgradeStrategy{
		Type: unifflev1alpha1.SpecificUpgrade,
	}

	rssWithoutUpgradeStrategyType := rssWithoutLogInServerMounts.DeepCopy()
	rssWithoutUpgradeStrategyType.Spec.ShuffleServer.CommonConfig.RSSPodSpec.HostPathMounts["/data/logs"] = "/data/logs"
	rssWithoutUpgradeStrategyType.Spec.ShuffleServer.UpgradeStrategy = &unifflev1alpha1.ShuffleServerUpgradeStrategy{}

	for _, tt := range []struct {
		name    string
		ar      *admissionv1.AdmissionReview
		allowed bool
	}{
		{
			name: "try to modify a upgrading rss object",
			ar: buildTestAdmissionReview(admissionv1.Update, wrapTestRssObj(&unifflev1alpha1.RemoteShuffleService{
				Status: unifflev1alpha1.RemoteShuffleServiceStatus{
					Phase: unifflev1alpha1.RSSUpgrading,
				},
			}), nil),
			allowed: false,
		},
		{
			name: "invalid rpc node port number in a rss object",
			ar: buildTestAdmissionReview(admissionv1.Update, nil, wrapTestRssObj(&unifflev1alpha1.RemoteShuffleService{
				Spec: unifflev1alpha1.RemoteShuffleServiceSpec{
					Coordinator: &unifflev1alpha1.CoordinatorConfig{
						Count:       pointer.Int32(2),
						RPCNodePort: []int32{30001},
					},
				},
			})),
			allowed: false,
		},
		{
			name: "invalid http node port number in a rss object",
			ar: buildTestAdmissionReview(admissionv1.Update, nil, wrapTestRssObj(&unifflev1alpha1.RemoteShuffleService{
				Spec: unifflev1alpha1.RemoteShuffleServiceSpec{
					Coordinator: &unifflev1alpha1.CoordinatorConfig{
						Count:        pointer.Int32(2),
						RPCNodePort:  []int32{30001, 30002},
						HTTPNodePort: []int32{30011},
					},
				},
			})),
			allowed: false,
		},
		{
			name:    "empty exclude nodes file path field in a rss object",
			ar:      buildTestAdmissionReview(admissionv1.Update, nil, wrapTestRssObj(rssWithCooNodePort)),
			allowed: false,
		},
		{
			name:    "can not find log host path in coordinators' host path mounts field in a rss object",
			ar:      buildTestAdmissionReview(admissionv1.Update, nil, wrapTestRssObj(rssWithoutLogInCooMounts)),
			allowed: false,
		},
		{
			name:    "can not find log host path in shuffle server' host path mounts field in a rss object",
			ar:      buildTestAdmissionReview(admissionv1.Update, nil, wrapTestRssObj(rssWithoutLogInServerMounts)),
			allowed: false,
		},
		{
			name:    "empty partition field when shuffler server of a rss object need partition upgrade",
			ar:      buildTestAdmissionReview(admissionv1.Update, nil, wrapTestRssObj(rssWithoutPartition)),
			allowed: false,
		},
		{
			name:    "invalid partition field when shuffler server of a rss object need partition upgrade",
			ar:      buildTestAdmissionReview(admissionv1.Update, nil, wrapTestRssObj(rssWithInvalidPartition)),
			allowed: false,
		},
		{
			name:    "empty specific names field when shuffler server of a rss object need specific upgrade",
			ar:      buildTestAdmissionReview(admissionv1.Update, nil, wrapTestRssObj(rssWithoutSpecificNames)),
			allowed: false,
		},
		{
			name:    "empty upgrade strategy type in shuffler server of a rss object",
			ar:      buildTestAdmissionReview(admissionv1.Update, nil, wrapTestRssObj(rssWithoutUpgradeStrategyType)),
			allowed: false,
		},
	} {
		t.Run(tt.name, func(tc *testing.T) {
			updatedAR := testInspector.validateRSS(tt.ar)
			if !updatedAR.Response.Allowed {
				tc.Logf("==> message in result: %+v", updatedAR.Response.Result.Message)
			}
			if updatedAR.Response.Allowed != tt.allowed {
				tc.Errorf("invalid 'allowed' field in response: %v <> %v", updatedAR.Response.Allowed, tt.allowed)
			}
		})
	}
}
