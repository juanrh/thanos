// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package relabel

import (
	"context"
	"fmt"

	"github.com/go-kit/log/level"
	"github.com/go-kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/relabel"
	"github.com/thanos-io/thanos/pkg/extkingpin"
	"gopkg.in/yaml.v2"
	"go.uber.org/atomic"
)

const (
	creationErrMsg = "creating relabel config watcher"
	configReloadCounterName = "relabel_config_reload_total"
	configReloadErrCounterName ="relabel_config_reload_err_total"
)

// Relabeller is responsible for managing the configuration and initialization of
// different types that apply relabel configurations to the Receive instance.
type Relabeller struct {
	fileContent extkingpin.FileContent
	relabelConfigs *atomic.Value
	logger log.Logger
	configReloadCounter    prometheus.Counter
	configReloadErrCounter prometheus.Counter
}

// RelabelConfig is a collection of relabel configurations.
type RelabelConfig []*relabel.Config

// NewRelabeller creates a new relabeller and loads the configuration to make sure loading is possible.
func NewRelabeller(fileContent extkingpin.FileContent, logger log.Logger, reg prometheus.Registerer) (*Relabeller, error) {
	var relabelConfigs atomic.Value
	relabeller := &Relabeller{
		fileContent: fileContent,
		relabelConfigs: &relabelConfigs,
		logger: logger,
	}

	// load first to fail if the flag was required or other load errors.
	if err := relabeller.loadConfig(); err != nil {
		return nil, errors.Wrap(err, creationErrMsg)
	}

	if reg != nil {
		relabeller.configReloadCounter = promauto.With(reg).NewCounter(
			prometheus.CounterOpts{
				Namespace: "thanos",
				Subsystem: "receive",
				Name:      configReloadCounterName,
				Help:      "How many times the relabel configuration was reloaded",
			},
		)
		relabeller.configReloadErrCounter = promauto.With(reg).NewCounter(
			prometheus.CounterOpts{
				Namespace: "thanos",
				Subsystem: "receive",
				Name:      configReloadErrCounterName,
				Help:      "How many times the relabel configuration failed to reload.",
			},
		)
	}

	return relabeller, nil
}

// NewConstantRelabeller creates a new relabeller that always loads the same content. 
// Metrics are logging are disabled. This is useful for testing.
func NewConstantRelabeller(content RelabelConfig) (*Relabeller, error) {
	relabelContent, err := yaml.Marshal(content)
	if err != nil {
		return nil, errors.Wrap(err, creationErrMsg)
	}
	relabeller, err := NewRelabeller(extkingpin.ConstantContentFileContent(relabelContent), log.NewNopLogger(), nil)
	if err != nil {
		return nil, err
	}
	return relabeller, nil
}

// RelabelConfig returns the current relabel config.
// This is concurrent safe.
func (r *Relabeller) RelabelConfig() RelabelConfig {
	if (r == nil) {
		var relabelConfig RelabelConfig
		return relabelConfig
	}
	return (r.relabelConfigs.Load()).(RelabelConfig)
}

// SetRelabelConfig sets the relabel config to the provided array.
// This is concurrent safe.
func (r *Relabeller) SetRelabelConfig(configs RelabelConfig) {
	r.relabelConfigs.Store(configs)
}

func (r *Relabeller) loadConfig() error {
	relabelContentYaml, err := r.fileContent.Content()
	if err != nil {
		return errors.Wrap(err, "getting content of relabel config")
	}
	var relabelConfig RelabelConfig
	if err := yaml.Unmarshal(relabelContentYaml, &relabelConfig); err != nil {
		return errors.Wrap(err, "parsing relabel config")
	}
	r.SetRelabelConfig(relabelConfig)
	return nil
}

// Run starts monitoring the file content of this Relabeller, updating the configuration when a change
// is detected. If there is an error reading the new configuration, the previous one is still used and
// returned in subsequent calls to `RelabelConfig`.
// If `errChan` is not nil, then the result of each update is sent through that channel, using `nil`
// for succesful updates.
// Returns whether or not there was an error setting up the monitoring. This call is not blocking,
// the background reloading process can be cancelled with `ctx`.
func (r *Relabeller) Run(ctx context.Context, errChan chan<- error) error {
	if r.fileContent.Path() == "" {
		// nothing to reload here
		return nil
	}

	err := extkingpin.PathContentReloader(ctx, r.fileContent, r.logger, func()  {
		level.Info(r.logger).Log("msg", "reloading relabel config.")
		err := r.loadConfig()

		if err != nil {
			if r.configReloadErrCounter != nil {
				r.configReloadErrCounter.Inc()
			}
			errMsg := fmt.Sprintf("error reloading relabel config from %s, will keep using the previous config.", r.fileContent.Path())
			level.Error(r.logger).Log("msg", errMsg, "err", err)
		}
		if r.configReloadCounter != nil {
			r.configReloadCounter.Inc()
		}

		if errChan != nil {
			errChan <- err
		}
	})

	return errors.Wrap(err, "setting up relabel config file watcher")
}