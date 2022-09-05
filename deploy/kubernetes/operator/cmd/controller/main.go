package main

import (
	"flag"

	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/apache/incubator-uniffle/deploy/kubernetes/operator/pkg/controller/config"
	"github.com/apache/incubator-uniffle/deploy/kubernetes/operator/pkg/controller/controller"
)

func main() {
	klog.InitFlags(nil)
	cfg := &config.Config{}
	cfg.AddFlags()
	flag.Parse()

	cfg.Complete()
	klog.Infof("run config: %+v", cfg)

	// create a manager for leader election.
	mgr, err := ctrl.NewManager(cfg.RESTConfig, ctrl.Options{
		LeaderElection:   true,
		LeaderElectionID: cfg.LeaderElectionID(),
	})
	if err != nil {
		klog.Fatal(err)
	}
	// create a rss controller.
	rc := controller.NewRSSController(cfg)
	if err = mgr.Add(rc); err != nil {
		klog.Fatal(err)
	}
	// start the rss controller.
	if err = mgr.Start(cfg.RunCtx); err != nil {
		klog.Fatal(err)
	}
}
