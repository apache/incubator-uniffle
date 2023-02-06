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

package coordinator

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/utils/pointer"

	uniffleapi "github.com/apache/incubator-uniffle/deploy/kubernetes/operator/api/uniffle/v1alpha1"
	controllerconstants "github.com/apache/incubator-uniffle/deploy/kubernetes/operator/pkg/controller/constants"
	"github.com/apache/incubator-uniffle/deploy/kubernetes/operator/pkg/utils"
)

const (
	testRuntimeClassName = "test-runtime"
)

// IsValidDeploy checks generated deployment, returns whether it is valid and error message.
type IsValidDeploy func(*appsv1.Deployment, *uniffleapi.RemoteShuffleService) (bool, error)

var (
	testLabels = map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
	}
	testENVs = []corev1.EnvVar{
		{
			Name:  "ENV1",
			Value: "Value1",
		},
		{
			Name:  "ENV2",
			Value: "Value2",
		},
		{
			Name:  "ENV3",
			Value: "Value3",
		},
		{
			Name:  controllerconstants.RssIPEnv,
			Value: "127.0.0.1",
		},
	}
)

func buildRssWithLabels() *uniffleapi.RemoteShuffleService {
	rss := utils.BuildRSSWithDefaultValue()
	rss.Spec.Coordinator.Labels = testLabels
	return rss
}

func buildRssWithRuntimeClassName() *uniffleapi.RemoteShuffleService {
	rss := utils.BuildRSSWithDefaultValue()
	rss.Spec.Coordinator.RuntimeClassName = pointer.String(testRuntimeClassName)
	return rss
}

func buildRssWithCustomENVs() *uniffleapi.RemoteShuffleService {
	rss := utils.BuildRSSWithDefaultValue()
	rss.Spec.Coordinator.Env = testENVs
	return rss
}

func TestGenerateDeploy(t *testing.T) {
	for _, tt := range []struct {
		name string
		rss  *uniffleapi.RemoteShuffleService
		IsValidDeploy
	}{
		{
			name: "add custom labels",
			rss:  buildRssWithLabels(),
			IsValidDeploy: func(deploy *appsv1.Deployment, rss *uniffleapi.RemoteShuffleService) (
				valid bool, err error) {
				valid = true

				expectedLabels := map[string]string{
					"app": "rss-coordinator-rss-0",
				}
				for k := range testLabels {
					expectedLabels[k] = testLabels[k]
				}

				currentLabels := deploy.Spec.Template.Labels
				if len(expectedLabels) != len(currentLabels) {
					valid = false
				} else {
					for k := range currentLabels {
						if expectedLabels[k] != currentLabels[k] {
							valid = false
							break
						}
					}
				}
				if !valid {
					err = fmt.Errorf("unexpected labels: %+v, expected: %+v", currentLabels, expectedLabels)
				}
				return valid, err
			},
		},
		{
			name: "set custom runtime class name",
			rss:  buildRssWithRuntimeClassName(),
			IsValidDeploy: func(deploy *appsv1.Deployment, rss *uniffleapi.RemoteShuffleService) (
				valid bool, err error) {
				currentRuntimeClassName := deploy.Spec.Template.Spec.RuntimeClassName
				if currentRuntimeClassName == nil {
					return false, fmt.Errorf("unexpected empty runtime class, expected: %v",
						testRuntimeClassName)
				}
				if *currentRuntimeClassName != testRuntimeClassName {
					return false, fmt.Errorf("unexpected runtime class name: %v, expected: %v",
						*currentRuntimeClassName, testRuntimeClassName)
				}
				return true, nil
			},
		},
		{
			name: "set custom environment variables",
			rss:  buildRssWithCustomENVs(),
			IsValidDeploy: func(deploy *appsv1.Deployment, rss *uniffleapi.RemoteShuffleService) (
				valid bool, err error) {
				expectENVs := []corev1.EnvVar{
					{
						Name:  controllerconstants.CoordinatorRPCPortEnv,
						Value: strconv.FormatInt(int64(controllerconstants.ContainerCoordinatorRPCPort), 10),
					},
					{
						Name:  controllerconstants.CoordinatorHTTPPortEnv,
						Value: strconv.FormatInt(int64(controllerconstants.ContainerCoordinatorHTTPPort), 10),
					},
					{
						Name:  controllerconstants.XmxSizeEnv,
						Value: rss.Spec.Coordinator.XmxSize,
					},
					{
						Name:  controllerconstants.ServiceNameEnv,
						Value: controllerconstants.CoordinatorServiceName,
					},
					{
						Name: controllerconstants.NodeNameEnv,
						ValueFrom: &corev1.EnvVarSource{
							FieldRef: &corev1.ObjectFieldSelector{
								APIVersion: "v1",
								FieldPath:  "spec.nodeName",
							},
						},
					},
					{
						Name: controllerconstants.RssIPEnv,
						ValueFrom: &corev1.EnvVarSource{
							FieldRef: &corev1.ObjectFieldSelector{
								APIVersion: "v1",
								FieldPath:  "status.podIP",
							},
						},
					},
				}
				defaultEnvNames := sets.NewString()
				for i := range expectENVs {
					defaultEnvNames.Insert(expectENVs[i].Name)
				}
				for i := range testENVs {
					if !defaultEnvNames.Has(testENVs[i].Name) {
						expectENVs = append(expectENVs, testENVs[i])
					}
				}

				actualENVs := deploy.Spec.Template.Spec.Containers[0].Env
				valid = reflect.DeepEqual(expectENVs, actualENVs)
				if !valid {
					actualEnvBody, _ := json.Marshal(actualENVs)
					expectEnvBody, _ := json.Marshal(expectENVs)
					err = fmt.Errorf("unexpected ENVs:\n%v,\nexpected:\n%v",
						string(actualEnvBody), string(expectEnvBody))
				}
				return
			},
		},
	} {
		t.Run(tt.name, func(tc *testing.T) {
			deploy := GenerateDeploy(tt.rss, 0)
			if valid, err := tt.IsValidDeploy(deploy, tt.rss); !valid {
				tc.Error(err)
			}
		})
	}
}

// generateServiceCuntMap generates a map with service type and its corresponding count. The headless svc is treated
//   differently: the service type for headless is treated as an empty service.
func generateServiceCountMap(services []*corev1.Service) map[corev1.ServiceType]int {
	result := make(map[corev1.ServiceType]int)
	var empty corev1.ServiceType = ""
	for _, service := range services {
		sType := service.Spec.Type
		if (sType == corev1.ServiceTypeClusterIP || sType == empty) && service.Spec.ClusterIP == corev1.ClusterIPNone {
			result[empty]++
		} else {
			result[service.Spec.Type]++
		}
	}
	return result
}

func TestGenerateSvcForCoordinator(t *testing.T) {
	for _, tt := range []struct {
		name          string
		rss           *uniffleapi.RemoteShuffleService
		serviceCntMap map[corev1.ServiceType]int
	}{
		{
			name: "with RPCNodePort",
			rss:  buildRssWithLabels(),
			serviceCntMap: map[corev1.ServiceType]int{
				"":                         2, // defaults to headless service
				corev1.ServiceTypeNodePort: 2,
			},
		},
		{
			name: "without RPCNodePort",
			rss: func() *uniffleapi.RemoteShuffleService {
				withoutRPCNodePortRss := buildRssWithLabels()
				withoutRPCNodePortRss.Spec.Coordinator.RPCNodePort = make([]int32, 0)
				withoutRPCNodePortRss.Spec.Coordinator.HTTPNodePort = make([]int32, 0)
				return withoutRPCNodePortRss
			}(),
			serviceCntMap: map[corev1.ServiceType]int{
				"": 2,
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			assertion := assert.New(t)
			_, _, services, _ := GenerateCoordinators(tt.rss)
			result := generateServiceCountMap(services)
			assertion.Equal(tt.serviceCntMap, result)
		})
	}
}

func TestGenerateAddresses(t *testing.T) {
	assertion := assert.New(t)
	rss := buildRssWithLabels()
	quorum := GenerateAddresses(rss)
	assertion.Contains(quorum, "headless")
}
