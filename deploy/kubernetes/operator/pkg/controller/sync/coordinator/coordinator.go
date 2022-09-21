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
	"strconv"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/utils/pointer"

	unifflev1alpha1 "github.com/apache/incubator-uniffle/deploy/kubernetes/operator/api/uniffle/v1alpha1"
	"github.com/apache/incubator-uniffle/deploy/kubernetes/operator/pkg/constants"
	controllerconstants "github.com/apache/incubator-uniffle/deploy/kubernetes/operator/pkg/controller/constants"
	"github.com/apache/incubator-uniffle/deploy/kubernetes/operator/pkg/controller/util"
	"github.com/apache/incubator-uniffle/deploy/kubernetes/operator/pkg/utils"
)

var defaultENVs sets.String

func init() {
	defaultENVs = sets.NewString()
	defaultENVs.Insert(controllerconstants.CoordinatorRPCPortEnv,
		controllerconstants.CoordinatorHTTPPortEnv,
		controllerconstants.XmxSizeEnv,
		controllerconstants.ServiceNameEnv)
}

// GenerateCoordinators generates objects related to coordinators
func GenerateCoordinators(rss *unifflev1alpha1.RemoteShuffleService) (
	*corev1.ServiceAccount, *corev1.ConfigMap, []*corev1.Service, []*appsv1.Deployment) {
	sa := GenerateSA(rss)
	cm := GenerateCM(rss)
	count := *rss.Spec.Coordinator.Count
	services := make([]*corev1.Service, count)
	deployments := make([]*appsv1.Deployment, count)
	for i := 0; i < int(count); i++ {
		svc := GenerateSvc(rss, i)
		deploy := GenerateDeploy(rss, i)
		services[i] = svc
		deployments[i] = deploy
	}
	return sa, cm, services, deployments
}

// GenerateSA generates service account of coordinator.
func GenerateSA(rss *unifflev1alpha1.RemoteShuffleService) *corev1.ServiceAccount {
	sa := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      utils.GenerateCoordinatorName(rss),
			Namespace: rss.Namespace,
		},
	}
	util.AddOwnerReference(&sa.ObjectMeta, rss)
	return sa
}

// GenerateCM generates configMap used by coordinators.
func GenerateCM(rss *unifflev1alpha1.RemoteShuffleService) *corev1.ConfigMap {
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      utils.GenerateCoordinatorName(rss),
			Namespace: rss.Namespace,
			Labels: map[string]string{
				constants.LabelCoordinator: "true",
			},
		},
		Data: map[string]string{
			utils.GetExcludeNodesConfigMapKey(rss): "",
		},
	}
	util.AddOwnerReference(&cm.ObjectMeta, rss)
	return cm
}

// GenerateSvc generates service used by specific coordinator.
func GenerateSvc(rss *unifflev1alpha1.RemoteShuffleService, index int) *corev1.Service {
	name := GenerateNameByIndex(rss, index)
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
			Ports: []corev1.ServicePort{
				{
					Name:       "rpc",
					Protocol:   corev1.ProtocolTCP,
					Port:       controllerconstants.ContainerCoordinatorRPCPort,
					TargetPort: intstr.FromInt(int(*rss.Spec.Coordinator.RPCPort)),
					NodePort:   rss.Spec.Coordinator.RPCNodePort[index],
				},
				{
					Name:       "http",
					Protocol:   corev1.ProtocolTCP,
					Port:       controllerconstants.ContainerCoordinatorHTTPPort,
					TargetPort: intstr.FromInt(int(*rss.Spec.Coordinator.HTTPPort)),
					NodePort:   rss.Spec.Coordinator.HTTPNodePort[index],
				},
			},
		},
	}
	util.AddOwnerReference(&svc.ObjectMeta, rss)
	return svc
}

// GenerateDeploy generates deployment of specific coordinator.
func GenerateDeploy(rss *unifflev1alpha1.RemoteShuffleService, index int) *appsv1.Deployment {
	name := GenerateNameByIndex(rss, index)

	podSpec := corev1.PodSpec{
		HostNetwork:        *rss.Spec.Coordinator.HostNetwork,
		ServiceAccountName: utils.GenerateCoordinatorName(rss),
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
		NodeSelector: rss.Spec.Coordinator.NodeSelector,
	}
	if podSpec.HostNetwork {
		podSpec.DNSPolicy = corev1.DNSClusterFirstWithHostNet
	}

	deploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: rss.Namespace,
			Labels: map[string]string{
				"app": name,
			},
		},
		Spec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app": name,
				},
			},
			Replicas: rss.Spec.Coordinator.Replicas,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app": name,
					},
				},
				Spec: podSpec,
			},
		},
	}

	// add init containers, the main container and other containers.
	deploy.Spec.Template.Spec.InitContainers = util.GenerateInitContainers(rss.Spec.Coordinator.RSSPodSpec)
	containers := []corev1.Container{*generateMainContainer(rss)}
	containers = append(containers, rss.Spec.Coordinator.SidecarContainers...)
	deploy.Spec.Template.Spec.Containers = containers

	// add configMap volume to save exclude nodes.
	configMapVolume := corev1.Volume{
		Name: controllerconstants.ExcludeNodesFile,
		VolumeSource: corev1.VolumeSource{
			ConfigMap: &corev1.ConfigMapVolumeSource{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: utils.GenerateCoordinatorName(rss),
				},
				DefaultMode: pointer.Int32(0777),
			},
		},
	}
	deploy.Spec.Template.Spec.Volumes = append(deploy.Spec.Template.Spec.Volumes, configMapVolume)
	// add hostPath volumes for coordinators.
	hostPathMounts := rss.Spec.Coordinator.HostPathMounts
	logHostPath := rss.Spec.Coordinator.LogHostPath
	deploy.Spec.Template.Spec.Volumes = append(deploy.Spec.Template.Spec.Volumes,
		util.GenerateHostPathVolumes(hostPathMounts, logHostPath, name)...)

	util.AddOwnerReference(&deploy.ObjectMeta, rss)
	return deploy
}

// GenerateNameByIndex returns workload or service name of coordinator by index.
func GenerateNameByIndex(rss *unifflev1alpha1.RemoteShuffleService, index int) string {
	return fmt.Sprintf("%v-%v-%v", constants.RSSCoordinator, rss.Name, index)
}

// GenerateAddresses returns addresses of coordinators accessed by shuffle servers.
func GenerateAddresses(rss *unifflev1alpha1.RemoteShuffleService) string {
	var names []string
	for i := 0; i < int(*rss.Spec.Coordinator.Count); i++ {
		current := fmt.Sprintf("%v:%v", GenerateNameByIndex(rss, i),
			controllerconstants.ContainerShuffleServerRPCPort)
		names = append(names, current)
	}
	return strings.Join(names, ",")
}

// GenerateProperties generates configuration properties of coordinators.
func GenerateProperties(rss *unifflev1alpha1.RemoteShuffleService) map[controllerconstants.PropertyKey]string {
	result := make(map[controllerconstants.PropertyKey]string)
	result[controllerconstants.RPCServerPort] = fmt.Sprintf("%v", *rss.Spec.Coordinator.RPCPort)
	result[controllerconstants.JettyHTTPPort] = fmt.Sprintf("%v", *rss.Spec.Coordinator.HTTPPort)
	result[controllerconstants.CoordinatorExcludeNodesPath] = utils.GetExcludeNodesMountPath(rss)
	return result
}

// generateMainContainer generates main container of coordinators.
func generateMainContainer(rss *unifflev1alpha1.RemoteShuffleService) *corev1.Container {
	return util.GenerateMainContainer(constants.RSSCoordinator, rss.Spec.Coordinator.ConfigDir,
		rss.Spec.Coordinator.RSSPodSpec.DeepCopy(), generateMainContainerPorts(rss),
		generateMainContainerENV(rss), []corev1.VolumeMount{
			{
				Name:      controllerconstants.ExcludeNodesFile,
				MountPath: utils.GetExcludeNodesMountPath(rss),
			},
		})
}

// generateMainContainerPorts generates ports of main container of coordinators.
func generateMainContainerPorts(rss *unifflev1alpha1.RemoteShuffleService) []corev1.ContainerPort {
	ports := []corev1.ContainerPort{
		{
			ContainerPort: controllerconstants.ContainerCoordinatorRPCPort,
			Protocol:      corev1.ProtocolTCP,
		},
		{
			ContainerPort: controllerconstants.ContainerCoordinatorHTTPPort,
			Protocol:      corev1.ProtocolTCP,
		},
	}
	ports = append(ports, rss.Spec.Coordinator.Ports...)
	return ports
}

// generateMainContainerENV generates environment variables of main container of coordinators.
func generateMainContainerENV(rss *unifflev1alpha1.RemoteShuffleService) []corev1.EnvVar {
	env := []corev1.EnvVar{
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
	}
	for _, e := range rss.Spec.Coordinator.Env {
		if !defaultENVs.Has(e.Name) {
			env = append(env, e)
		}
	}
	return env
}
