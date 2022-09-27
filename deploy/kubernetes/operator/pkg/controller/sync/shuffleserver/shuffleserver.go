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
	"sort"
	"strconv"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/pointer"

	unifflev1alpha1 "github.com/apache/incubator-uniffle/deploy/kubernetes/operator/api/uniffle/v1alpha1"
	"github.com/apache/incubator-uniffle/deploy/kubernetes/operator/pkg/constants"
	controllerconstants "github.com/apache/incubator-uniffle/deploy/kubernetes/operator/pkg/controller/constants"
	"github.com/apache/incubator-uniffle/deploy/kubernetes/operator/pkg/controller/sync/coordinator"
	"github.com/apache/incubator-uniffle/deploy/kubernetes/operator/pkg/controller/util"
	"github.com/apache/incubator-uniffle/deploy/kubernetes/operator/pkg/utils"
)

// GenerateShuffleServers generates objects related to shuffle servers.
func GenerateShuffleServers(rss *unifflev1alpha1.RemoteShuffleService) (
	*corev1.ServiceAccount, []*corev1.Service, *appsv1.StatefulSet) {
	sa := GenerateSA(rss)
	var services []*corev1.Service
	if needGenerateHeadlessSVC(rss) {
		services = append(services, GenerateHeadlessSVC(rss))
	}
	if needGenerateNodePortSVC(rss) {
		services = append(services, GenerateNodePortSVC(rss))
	}
	sts := GenerateSts(rss)
	return sa, services, sts
}

// GenerateSA generates service account of shuffle servers.
func GenerateSA(rss *unifflev1alpha1.RemoteShuffleService) *corev1.ServiceAccount {
	sa := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      GenerateName(rss),
			Namespace: rss.Namespace,
		},
	}
	util.AddOwnerReference(&sa.ObjectMeta, rss)
	return sa
}

// GenerateHeadlessSVC generates headless service used by shuffle servers.
func GenerateHeadlessSVC(rss *unifflev1alpha1.RemoteShuffleService) *corev1.Service {
	name := generateHeadlessSVCName(rss)
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: rss.Namespace,
		},
		Spec: corev1.ServiceSpec{
			ClusterIP: corev1.ClusterIPNone,
			Selector: map[string]string{
				"app": GenerateName(rss),
			},
		},
	}
	if rss.Spec.ShuffleServer.RPCPort != nil {
		svc.Spec.Ports = append(svc.Spec.Ports, corev1.ServicePort{
			Name:       "rpc",
			Protocol:   corev1.ProtocolTCP,
			Port:       controllerconstants.ContainerShuffleServerRPCPort,
			TargetPort: intstr.FromInt(int(*rss.Spec.ShuffleServer.RPCPort)),
		})
	}
	if rss.Spec.ShuffleServer.HTTPPort != nil {
		svc.Spec.Ports = append(svc.Spec.Ports, corev1.ServicePort{
			Name:       "http",
			Protocol:   corev1.ProtocolTCP,
			Port:       controllerconstants.ContainerShuffleServerHTTPPort,
			TargetPort: intstr.FromInt(int(*rss.Spec.ShuffleServer.HTTPPort)),
		})
	}
	util.AddOwnerReference(&svc.ObjectMeta, rss)
	return svc
}

// GenerateNodePortSVC generates nodePort service used by shuffle servers.
func GenerateNodePortSVC(rss *unifflev1alpha1.RemoteShuffleService) *corev1.Service {
	name := GenerateName(rss)
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: rss.Namespace,
		},
		Spec: corev1.ServiceSpec{
			Type: corev1.ServiceTypeNodePort,
			Selector: map[string]string{
				"app": name,
			},
		},
	}
	if needNodePortForRPC(rss) {
		svc.Spec.Ports = append(svc.Spec.Ports, corev1.ServicePort{
			Protocol:   corev1.ProtocolTCP,
			Port:       controllerconstants.ContainerShuffleServerRPCPort,
			TargetPort: intstr.FromInt(int(*rss.Spec.ShuffleServer.RPCPort)),
			NodePort:   *rss.Spec.ShuffleServer.RPCNodePort,
		})
	}
	if needNodePortForHTTP(rss) {
		svc.Spec.Ports = append(svc.Spec.Ports, corev1.ServicePort{
			Protocol:   corev1.ProtocolTCP,
			Port:       controllerconstants.ContainerShuffleServerHTTPPort,
			TargetPort: intstr.FromInt(int(*rss.Spec.ShuffleServer.HTTPPort)),
			NodePort:   *rss.Spec.ShuffleServer.HTTPNodePort,
		})
	}
	util.AddOwnerReference(&svc.ObjectMeta, rss)
	return svc
}

// getReplicas returns replicas of shuffle servers.
func getReplicas(rss *unifflev1alpha1.RemoteShuffleService) *int32 {
	// TODO: we will support hpa for rss object,
	// and when we enable hpa, we should not return replicas in .spec.shuffleServer field.
	return rss.Spec.ShuffleServer.Replicas
}

// GenerateSts generates statefulSet of shuffle servers.
func GenerateSts(rss *unifflev1alpha1.RemoteShuffleService) *appsv1.StatefulSet {
	name := GenerateName(rss)
	replicas := getReplicas(rss)

	podSpec := corev1.PodSpec{
		SecurityContext:    rss.Spec.ShuffleServer.SecurityContext,
		HostNetwork:        *rss.Spec.ShuffleServer.HostNetwork,
		ServiceAccountName: GenerateName(rss),
		Tolerations: []corev1.Toleration{
			{
				Effect: corev1.TaintEffectNoSchedule,
				Key:    "node-role.kubernetes.io/master",
			},
		},
		Volumes: []corev1.Volume{
			{
				Name: controllerconstants.ConfigurationVolumeName,
				VolumeSource: corev1.VolumeSource{
					ConfigMap: &corev1.ConfigMapVolumeSource{
						LocalObjectReference: corev1.LocalObjectReference{
							Name: rss.Spec.ConfigMapName,
						},
						DefaultMode: pointer.Int32(0777),
					},
				},
			},
		},
		NodeSelector: rss.Spec.ShuffleServer.NodeSelector,
	}
	if podSpec.HostNetwork {
		podSpec.DNSPolicy = corev1.DNSClusterFirstWithHostNet
	}

	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: rss.Namespace,
			Labels:    utils.GenerateShuffleServerLabels(rss),
		},
		Spec: appsv1.StatefulSetSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: utils.GenerateShuffleServerLabels(rss),
			},
			UpdateStrategy: appsv1.StatefulSetUpdateStrategy{
				Type: appsv1.RollingUpdateStatefulSetStrategyType,
				RollingUpdate: &appsv1.RollingUpdateStatefulSetStrategy{
					Partition: replicas,
				},
			},
			ServiceName: generateHeadlessSVCName(rss),
			Replicas:    replicas,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: utils.GenerateShuffleServerLabels(rss),
					Annotations: map[string]string{
						constants.AnnotationRssName: rss.Name,
						constants.AnnotationRssUID:  string(rss.UID),
						constants.AnnotationMetricsServerPort: fmt.Sprintf("%v",
							controllerconstants.ContainerShuffleServerHTTPPort),
						constants.AnnotationShuffleServerPort: fmt.Sprintf("%v",
							controllerconstants.ContainerShuffleServerRPCPort),
					},
				},
				Spec: podSpec,
			},
		},
	}

	// add init containers, the main container and other containers.
	sts.Spec.Template.Spec.InitContainers = util.GenerateInitContainers(rss.Spec.ShuffleServer.RSSPodSpec)
	containers := []corev1.Container{*generateMainContainer(rss)}
	containers = append(containers, rss.Spec.ShuffleServer.SidecarContainers...)
	sts.Spec.Template.Spec.Containers = containers

	// add hostPath volumes for shuffle servers.
	hostPathMounts := rss.Spec.ShuffleServer.HostPathMounts
	logHostPath := rss.Spec.ShuffleServer.LogHostPath
	sts.Spec.Template.Spec.Volumes = append(sts.Spec.Template.Spec.Volumes,
		util.GenerateHostPathVolumes(hostPathMounts, logHostPath, name)...)

	util.AddOwnerReference(&sts.ObjectMeta, rss)
	return sts
}

// GenerateName returns workload or nodePort service name of shuffle server.
func GenerateName(rss *unifflev1alpha1.RemoteShuffleService) string {
	return utils.GenerateShuffleServerName(rss)
}

// GenerateProperties generates configuration properties of shuffle servers.
func GenerateProperties(rss *unifflev1alpha1.RemoteShuffleService) map[controllerconstants.PropertyKey]string {
	result := make(map[controllerconstants.PropertyKey]string)
	result[controllerconstants.RPCServerPort] = fmt.Sprintf("%v", *rss.Spec.ShuffleServer.RPCPort)
	result[controllerconstants.JettyHTTPPort] = fmt.Sprintf("%v", *rss.Spec.ShuffleServer.HTTPPort)
	result[controllerconstants.CoordinatorQuorum] = coordinator.GenerateAddresses(rss)
	result[controllerconstants.StorageBasePath] = generateStorageBasePath(rss)
	return result
}

// generateStorageBasePath generates storage base path in shuffle server's configuration.
func generateStorageBasePath(rss *unifflev1alpha1.RemoteShuffleService) string {
	var paths []string
	for k, v := range rss.Spec.ShuffleServer.HostPathMounts {
		if k == rss.Spec.ShuffleServer.LogHostPath {
			continue
		}
		paths = append(paths, strings.TrimSuffix(v, "/")+"/rssdata")
	}
	sort.Strings(paths)
	return strings.Join(paths, ",")
}

// generateHeadlessSVCName returns name of shuffle servers' headless service.
func generateHeadlessSVCName(rss *unifflev1alpha1.RemoteShuffleService) string {
	return GenerateName(rss) + "-headless"
}

// generateMainContainer generates main container of shuffle servers.
func generateMainContainer(rss *unifflev1alpha1.RemoteShuffleService) *corev1.Container {
	return util.GenerateMainContainer(constants.RSSShuffleServer, rss.Spec.ShuffleServer.ConfigDir,
		rss.Spec.ShuffleServer.RSSPodSpec.DeepCopy(), generateMainContainerPorts(rss),
		generateMainContainerENV(rss), nil)
}

// generateMainContainerPorts generates ports of main container of shuffle servers.
func generateMainContainerPorts(rss *unifflev1alpha1.RemoteShuffleService) []corev1.ContainerPort {
	ports := []corev1.ContainerPort{
		{
			ContainerPort: controllerconstants.ContainerShuffleServerRPCPort,
			Protocol:      corev1.ProtocolTCP,
		},
		{
			ContainerPort: controllerconstants.ContainerShuffleServerHTTPPort,
			Protocol:      corev1.ProtocolTCP,
		},
	}
	ports = append(ports, rss.Spec.ShuffleServer.Ports...)
	return ports
}

// generateMainContainerENV generates environment variables of main container of shuffle servers.
func generateMainContainerENV(rss *unifflev1alpha1.RemoteShuffleService) []corev1.EnvVar {
	return []corev1.EnvVar{
		{
			Name:  controllerconstants.ShuffleServerRPCPortEnv,
			Value: strconv.FormatInt(int64(controllerconstants.ContainerShuffleServerRPCPort), 10),
		},
		{
			Name:  controllerconstants.ShuffleServerHTTPPortEnv,
			Value: strconv.FormatInt(int64(controllerconstants.ContainerShuffleServerHTTPPort), 10),
		},
		{
			Name:  controllerconstants.RSSCoordinatorQuorumEnv,
			Value: coordinator.GenerateAddresses(rss),
		},
		{
			Name:  controllerconstants.XmxSizeEnv,
			Value: rss.Spec.ShuffleServer.XmxSize,
		},
		{
			Name:  controllerconstants.ServiceNameEnv,
			Value: controllerconstants.ShuffleServerServiceName,
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
}

// needGenerateNodePortSVC returns whether we need node port service for shuffle servers.
func needGenerateNodePortSVC(rss *unifflev1alpha1.RemoteShuffleService) bool {
	return needNodePortForRPC(rss) || needNodePortForHTTP(rss)
}

// needGenerateHeadlessSVC returns whether we need headless service for shuffle servers.
func needGenerateHeadlessSVC(rss *unifflev1alpha1.RemoteShuffleService) bool {
	return rss.Spec.ShuffleServer.RPCPort != nil || rss.Spec.ShuffleServer.HTTPPort != nil
}

// needNodePortForRPC returns whether we need node port service for rpc service of shuffle servers.
func needNodePortForRPC(rss *unifflev1alpha1.RemoteShuffleService) bool {
	return rss.Spec.ShuffleServer.RPCPort != nil && rss.Spec.ShuffleServer.RPCNodePort != nil
}

// needNodePortForRPC returns whether we need node port service for http service of shuffle servers.
func needNodePortForHTTP(rss *unifflev1alpha1.RemoteShuffleService) bool {
	return rss.Spec.ShuffleServer.HTTPPort != nil && rss.Spec.ShuffleServer.HTTPNodePort != nil
}
