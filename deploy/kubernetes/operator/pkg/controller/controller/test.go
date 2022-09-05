package controller

import (
	corev1 "k8s.io/api/core/v1"

	"github.com/apache/incubator-uniffle/deploy/kubernetes/operator/pkg/constants"
)

const (
	testRssName           = "test"
	testNamespace         = corev1.NamespaceDefault
	testCoordinatorImage1 = "rss-coordinator:test1"
	testCoordinatorImage2 = "rss-coordinator:test2"

	testShuffleServerPodName = constants.RSSShuffleServer + "-" + testRssName + "-0"
	testExcludeNodeFileKey   = "exclude_nodes"
)
