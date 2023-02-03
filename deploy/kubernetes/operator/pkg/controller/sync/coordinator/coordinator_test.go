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
	"fmt"
	"testing"

	appsv1 "k8s.io/api/apps/v1"

	unifflev1alpha1 "github.com/apache/incubator-uniffle/deploy/kubernetes/operator/api/uniffle/v1alpha1"
	"github.com/apache/incubator-uniffle/deploy/kubernetes/operator/pkg/utils"
)

// IsValidDeploy checks generated deployment, returns whether it is valid and error message.
type IsValidDeploy func(*appsv1.Deployment) (bool, error)

var commonLabels = map[string]string{
	"key1": "value1",
	"key2": "value2",
	"key3": "value3",
}

func buildRssWithLabels() *unifflev1alpha1.RemoteShuffleService {
	rss := utils.BuildRSSWithDefaultValue()
	rss.Spec.Coordinator.Labels = commonLabels
	return rss
}

func TestGenerateDeploy(t *testing.T) {
	for _, tt := range []struct {
		name string
		rss  *unifflev1alpha1.RemoteShuffleService
		IsValidDeploy
	}{
		{
			name: "add custom labels",
			rss:  buildRssWithLabels(),
			IsValidDeploy: func(deploy *appsv1.Deployment) (bool, error) {
				var valid = true
				var err error

				expectedLabels := map[string]string{
					"app": "rss-coordinator-rss-0",
				}
				for k := range commonLabels {
					expectedLabels[k] = commonLabels[k]
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
	} {
		t.Run(tt.name, func(tc *testing.T) {
			deploy := GenerateDeploy(tt.rss, 0)
			if valid, err := tt.IsValidDeploy(deploy); !valid {
				tc.Error(err)
			}
		})
	}
}
