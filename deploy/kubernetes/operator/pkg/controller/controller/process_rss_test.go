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

package controller

import (
	"context"
	"reflect"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubefake "k8s.io/client-go/kubernetes/fake"

	unifflev1alpha1 "github.com/apache/incubator-uniffle/deploy/kubernetes/operator/api/uniffle/v1alpha1"
	"github.com/apache/incubator-uniffle/deploy/kubernetes/operator/pkg/controller/config"
	"github.com/apache/incubator-uniffle/deploy/kubernetes/operator/pkg/generated/clientset/versioned/fake"
	"github.com/apache/incubator-uniffle/deploy/kubernetes/operator/pkg/utils"
)

// buildEmptyPhaseRssObj builds a rss object with empty phase for testing.
func buildEmptyPhaseRssObj() *unifflev1alpha1.RemoteShuffleService {
	return &unifflev1alpha1.RemoteShuffleService{
		ObjectMeta: metav1.ObjectMeta{
			Name:            testRssName,
			Namespace:       testNamespace,
			ResourceVersion: "test",
		},
		Spec: unifflev1alpha1.RemoteShuffleServiceSpec{
			Coordinator: &unifflev1alpha1.CoordinatorConfig{
				ExcludeNodesFilePath: "/exclude_nodes",
			},
		},
		Status: unifflev1alpha1.RemoteShuffleServiceStatus{},
	}
}

// TestProcessEmptyPhaseRss tests rss objects' process of rss-controller
func TestProcessEmptyPhaseRss(t *testing.T) {
	rss := buildEmptyPhaseRssObj()

	rssClient := fake.NewSimpleClientset(rss)
	kubeClient := kubefake.NewSimpleClientset()

	rc := newRSSController(&config.Config{
		GenericConfig: utils.GenericConfig{
			KubeClient: kubeClient,
			RSSClient:  rssClient,
		},
	})

	for _, tt := range []struct {
		name              string
		expectedRssStatus unifflev1alpha1.RemoteShuffleServiceStatus
		expectedNeedRetry bool
		expectedError     error
	}{
		{
			name: "process rss object which has just been created, and whose status phase is empty",
			expectedRssStatus: unifflev1alpha1.RemoteShuffleServiceStatus{
				Phase: unifflev1alpha1.RSSPending,
			},
			expectedNeedRetry: false,
		},
	} {
		t.Run(tt.name, func(tc *testing.T) {
			needRetry, err := rc.processNormal(rss)
			if err != nil {
				tc.Errorf("process rss object failed: %v", err)
				return
			}
			if needRetry != tt.expectedNeedRetry {
				tc.Errorf("unexpected result indicates whether to retrys: %v, expected: %v",
					needRetry, tt.expectedNeedRetry)
				return
			}
			updatedRss, getErr := rssClient.UniffleV1alpha1().RemoteShuffleServices(rss.Namespace).
				Get(context.TODO(), rss.Name, metav1.GetOptions{})
			if getErr != nil {
				tc.Errorf("get updated rss object failed: %v", err)
				return
			}
			if !reflect.DeepEqual(updatedRss.Status, tt.expectedRssStatus) {
				tc.Errorf("unexpected status of updated rss object: %+v, expected: %+v",
					updatedRss.Status, tt.expectedRssStatus)
				return
			}
		})
	}
}
