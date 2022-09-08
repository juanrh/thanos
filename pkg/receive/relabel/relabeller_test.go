// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package relabel

import (
	"context"
	"os"
	"path"
	"testing"

	"github.com/go-kit/log"
	"github.com/thanos-io/thanos/pkg/extkingpin"
	"github.com/thanos-io/thanos/pkg/testutil"
	"gopkg.in/yaml.v2"
)

const (
	testdataDir = "./testdata"
)

var (
	logger = log.NewLogfmtLogger(os.Stdout)
)


func TestRelabellerHappyPath(t *testing.T) {
	var relabelConfig RelabelConfig
	goodConf1 := "good_conf_1.yaml"
	goodConf2 := "good_conf_2.yaml"
	f := newTestFixture(t, goodConf1)

	f.Start(t)
	defer f.Stop()

	yaml.Unmarshal(readTestdataFile(t, goodConf1), &relabelConfig)
	testutil.Equals(t, relabelConfig, f.relabeller.RelabelConfig())
	
	for _, testDataFilename := range []string{goodConf2, goodConf1} {
		f.relabelConfigContent.Rewrite(readTestdataFile(t, testDataFilename))
		testutil.Ok(t, <- f.errChan)
		yaml.Unmarshal(readTestdataFile(t, testDataFilename), &relabelConfig)
		testutil.Equals(t, relabelConfig, f.relabeller.RelabelConfig())
	}
}

func TestRelabellerReloadFailureRecovery(t *testing.T) {
	f := newTestFixture(t, "good_conf_1.yaml")

	f.Start(t)
	defer f.Stop()

	contentBeforeReload, err := yaml.Marshal(f.relabeller.RelabelConfig())
	testutil.Ok(t, err)
	f.relabelConfigContent.Rewrite(readTestdataFile(t, "malformed_conf.yaml"))
	testutil.NotOk(t, <- f.errChan)
	contentAfterReload, err := yaml.Marshal(f.relabeller.RelabelConfig())
	testutil.Ok(t, err)
	testutil.Equals(t, contentBeforeReload, contentAfterReload)
}

func TestRelabellerCreationFailureAborts(t *testing.T) {
	relabeller, err := NewRelabeller(staticPathContentForTestadataFile(t, "malformed_conf.yaml"), logger, nil)
	
	testutil.NotOk(t, err)
	testutil.Assert(t, relabeller == nil)
}

func TestRelabellerRunFailureAborts(t *testing.T) {
	f := newTestFixture(t, "good_conf_1.yaml")
	
	os.RemoveAll(path.Dir(f.relabelConfigContent.Path()))
	
	testutil.NotOk(t, f.relabeller.Run(f.ctx, f.errChan))
}

type fixture struct {
	relabelConfigContent *extkingpin.StaticPathContent
	relabeller *Relabeller
	errChan chan error
	ctx context.Context
	cancel context.CancelFunc
}

func newTestFixture(t *testing.T, initialDataFilename string) *fixture {
	relabelConfigContent := staticPathContentForTestadataFile(t, initialDataFilename)
	relabeller, err := NewRelabeller(relabelConfigContent, logger, nil)
	testutil.Ok(t, err)
	ctx, cancel := context.WithCancel(context.Background())

	return &fixture{
		relabeller: relabeller,
		relabelConfigContent: relabelConfigContent,
		errChan: make(chan error),
		ctx: ctx,
		cancel: cancel,
	}
}

func (f *fixture) Start(t *testing.T) {
	testutil.Ok(t, f.relabeller.Run(f.ctx, f.errChan))
}

func (f *fixture) Stop() {
	f.cancel()
}

func staticPathContentForTestadataFile(t *testing.T, testDataFilename string) *extkingpin.StaticPathContent {
	relabelConfigPath := path.Join(t.TempDir(), "relabel-conf.yaml")
	conf := readTestdataFile(t, testDataFilename)
	testutil.Ok(t, os.WriteFile(relabelConfigPath, conf, 0666))
	relabelConfigContent, err := extkingpin.NewStaticPathContent(relabelConfigPath)
	testutil.Ok(t, err)
	return relabelConfigContent
}

func readTestdataFile(t *testing.T, testDataFilename string) []byte {
	contents, err := os.ReadFile(path.Join(testdataDir, testDataFilename))
	testutil.Ok(t, err)
	return contents
}
