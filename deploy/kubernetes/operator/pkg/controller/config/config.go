package config

import (
	"flag"

	"github.com/apache/incubator-uniffle/deploy/kubernetes/operator/pkg/constants"
	"github.com/apache/incubator-uniffle/deploy/kubernetes/operator/pkg/utils"
)

const (
	flagWorkers = "workers"
)

// Config contains all configurations.
type Config struct {
	Workers int
	utils.GenericConfig
}

// LeaderElectionID returns leader election ID.
func (c *Config) LeaderElectionID() string {
	return "rss-controller-" + constants.LeaderIDSuffix
}

// AddFlags adds all configurations to the global flags.
func (c *Config) AddFlags() {
	flag.IntVar(&c.Workers, flagWorkers, 1, "Concurrency of the rss controller.")
	c.GenericConfig.AddFlags()
}

// Complete is called before rss-controller runs.
func (c *Config) Complete() {
	c.GenericConfig.Complete()
}
