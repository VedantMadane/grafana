package migrate

import (
	"context"
	"errors"
	"fmt"

	provisioning "github.com/grafana/grafana/apps/provisioning/pkg/apis/provisioning/v0alpha1"
	"github.com/grafana/grafana/apps/provisioning/pkg/repository"
	"github.com/grafana/grafana/pkg/registry/apis/provisioning/jobs"
	"github.com/grafana/grafana/pkg/services/featuremgmt"
)

//go:generate mockery --name Migrator --structname MockMigrator --inpackage --filename mock_migrator.go --with-expecter
type Migrator interface {
	Migrate(ctx context.Context, rw repository.ReaderWriter, opts provisioning.MigrateJobOptions, progress jobs.JobProgressRecorder) error
}

type MigrationWorker struct {
	unifiedMigrator Migrator
	features        featuremgmt.FeatureToggles
}

func NewMigrationWorkerFromUnified(unifiedMigrator Migrator, features featuremgmt.FeatureToggles) *MigrationWorker {
	return &MigrationWorker{
		unifiedMigrator: unifiedMigrator,
		features:        features,
	}
}

func NewMigrationWorker(unifiedMigrator Migrator, features featuremgmt.FeatureToggles) *MigrationWorker {
	return &MigrationWorker{
		unifiedMigrator: unifiedMigrator,
		features:        features,
	}
}

func (w *MigrationWorker) IsSupported(ctx context.Context, job provisioning.Job) bool {
	return job.Spec.Action == provisioning.JobActionMigrate
}

func (w *MigrationWorker) Process(ctx context.Context, repo repository.Repository, job provisioning.Job, progress jobs.JobProgressRecorder) error {
	// Check if export feature is enabled
	if !w.features.IsEnabledGlobally(featuremgmt.FlagProvisioningExport) {
		return fmt.Errorf("migrate jobs are disabled: %s feature flag is not enabled", featuremgmt.FlagProvisioningExport)
	}

	options := job.Spec.Migrate
	if options == nil {
		return errors.New("missing migrate settings")
	}

	progress.SetTotal(ctx, 10) // will show a progress bar
	rw, ok := repo.(repository.ReaderWriter)
	if !ok {
		return errors.New("migration job submitted targeting repository that is not a ReaderWriter")
	}

	return w.unifiedMigrator.Migrate(ctx, rw, *options, progress)
}
