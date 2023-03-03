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

package config

import (
	"flag"
	"time"

	"k8s.io/utils/pointer"

	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/apache/incubator-uniffle/deploy/kubernetes/operator/pkg/constants"
	"github.com/apache/incubator-uniffle/deploy/kubernetes/operator/pkg/utils"
)

const (
	flagWorkers                    = "workers"
	managerLeaderElection          = "leader-election"
	managerLeaderElectionID        = "leader-election-id"
	managerLeaderElectionNamespace = "leader-election-namespace"
	managerSyncPeriod              = "sync-period"
	managerRetryPeriod             = "retry-period"
	managerNamespace               = "namespace"
)

// Config contains all configurations.
type Config struct {
	Workers        int
	ManagerOptions ctrl.Options
	utils.GenericConfig
}

// LeaderElectionID returns leader election ID.
func (c *Config) LeaderElectionID() string {
	return "rss-controller-" + constants.LeaderIDSuffix
}

// AddFlags adds all configurations to the global flags.
func (c *Config) AddFlags() {
	c.ManagerOptions.SyncPeriod = pointer.Duration(time.Hour * 10)
	c.ManagerOptions.RetryPeriod = pointer.Duration(time.Second * 2)
	flag.IntVar(&c.Workers, flagWorkers, 1, "Concurrency of the rss controller.")
	flag.BoolVar(&c.ManagerOptions.LeaderElection, managerLeaderElection, true, "LeaderElection determines whether or not to use leader election when starting the manager.")
	flag.StringVar(&c.ManagerOptions.LeaderElectionID, managerLeaderElectionID, c.LeaderElectionID(), "LeaderElectionID determines the name of the resource that leader election will use for holding the leader lock.")
	flag.StringVar(&c.ManagerOptions.LeaderElectionNamespace, managerLeaderElectionNamespace, "kube-system", "LeaderElectionNamespace determines the namespace in which the leader election resource will be created.")
	flag.StringVar(&c.ManagerOptions.Namespace, managerNamespace, "", "Namespace if specified restricts the manager's cache to watch objects in the desired namespace Defaults to all namespaces.")
	flag.DurationVar(c.ManagerOptions.SyncPeriod, managerSyncPeriod, time.Hour*10, "SyncPeriod determines the minimum frequency at which watched resources are reconciled.")
	flag.DurationVar(c.ManagerOptions.RetryPeriod, managerRetryPeriod, time.Second*2, "RetryPeriod is the duration the LeaderElector clients should wait between tries of actions.")

	c.GenericConfig.AddFlags()
}

// Complete is called before rss-controller runs.
func (c *Config) Complete() {
	c.GenericConfig.Complete()
}
