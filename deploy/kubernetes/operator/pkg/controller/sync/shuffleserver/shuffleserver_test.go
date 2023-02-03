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

package shuffleserver

import (
	"fmt"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/utils/pointer"

	unifflev1alpha1 "github.com/apache/incubator-uniffle/deploy/kubernetes/operator/api/uniffle/v1alpha1"
	"github.com/apache/incubator-uniffle/deploy/kubernetes/operator/pkg/utils"
)

const (
	testRuntimeClassName = "test-runtime"
)

// IsValidSts checks generated statefulSet, returns whether it is valid and error message.
type IsValidSts func(*appsv1.StatefulSet) (bool, error)

var commonLabels = map[string]string{
	"key1": "value1",
	"key2": "value2",
	"key3": "value3",
}

func buildRssWithLabels() *unifflev1alpha1.RemoteShuffleService {
	rss := utils.BuildRSSWithDefaultValue()
	rss.Spec.ShuffleServer.Labels = map[string]string{
		"uniffle.apache.org/shuffle-server": "change-test",
	}
	for k := range commonLabels {
		rss.Spec.ShuffleServer.Labels[k] = commonLabels[k]
	}
	return rss
}

func buildRssWithRuntimeClassName() *unifflev1alpha1.RemoteShuffleService {
	rss := utils.BuildRSSWithDefaultValue()
	rss.Spec.ShuffleServer.RuntimeClassName = pointer.String(testRuntimeClassName)
	return rss
}

func TestGenerateSts(t *testing.T) {
	for _, tt := range []struct {
		name string
		rss  *unifflev1alpha1.RemoteShuffleService
		IsValidSts
	}{
		{
			name: "add custom labels",
			rss:  buildRssWithLabels(),
			IsValidSts: func(sts *appsv1.StatefulSet) (valid bool, err error) {
				valid = true

				expectedLabels := map[string]string{
					"app":                               "rss-shuffle-server-rss",
					"uniffle.apache.org/shuffle-server": "true",
				}
				for k := range commonLabels {
					expectedLabels[k] = commonLabels[k]
				}

				currentLabels := sts.Spec.Template.Labels
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
			IsValidSts: func(sts *appsv1.StatefulSet) (valid bool, err error) {
				currentRuntimeClassName := sts.Spec.Template.Spec.RuntimeClassName
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
	} {
		t.Run(tt.name, func(tc *testing.T) {
			sts := GenerateSts(tt.rss)
			if valid, err := tt.IsValidSts(sts); !valid {
				tc.Error(err)
			}
		})
	}
}
