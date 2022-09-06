package extkingpin

import (
	"context"
	"os"
	"path"
	"testing"

	"github.com/go-kit/log"
	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestPathContentReloader(t *testing.T) {
	type args struct {
		runSteps func(t *testing.T, testFile string, pathContent *staticPathContent)
	}
	tests := []struct {
		name        string
		args        args
		wantReloads int
	}{
		{
			name: "Many operations, only rewrite triggers reload",
			args: args{
				runSteps: func(t *testing.T, testFile string, pathContent *staticPathContent) {
					for i := 0; i < 10; i++ {
						testutil.Ok(t, os.Chmod(testFile, 0777))
						testutil.Ok(t, os.Chmod(testFile, 0700))
					}
					testutil.Ok(t, pathContent.Rewrite([]byte("test modified")))
				},
			},
			wantReloads: 1,
		},
		{
			name: "Chmod doesn't trigger reload",
			args: args{
				runSteps: func(t *testing.T, testFile string, pathContent *staticPathContent) {
					testutil.Ok(t, os.Chmod(testFile, 0777))
				},
			},
			wantReloads: 0,
		},
		{
			name: "Remove doesn't trigger reload",
			args: args{
				runSteps: func(t *testing.T, testFile string, pathContent *staticPathContent) {
					testutil.Ok(t, os.Remove(testFile))
				},
			},
			wantReloads: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testFile := path.Join(t.TempDir(), "test")
			testutil.Ok(t, os.WriteFile(testFile, []byte("test"), 0666))
			pathContent, err := NewStaticPathContent(testFile)
			if err != nil {
				t.Fatalf("error trying to save static limit config: %s", err)
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			reload := make(chan struct{})
			reloadCounts := make(chan int)
			go func() {
				reloadCount := 0
				for reloadCount < tt.wantReloads {
					<- reload
					reloadCount += 1
				}
				reloadCounts <- reloadCount
			}()
			err = PathContentReloader(ctx, pathContent, log.NewLogfmtLogger(os.Stdout), func() {
				reload <- struct{}{}
			})
			testutil.Ok(t, err)

			tt.args.runSteps(t, testFile, pathContent)

			reloadCount := <- reloadCounts
			testutil.Equals(t, tt.wantReloads, reloadCount)
		})
	}
}
