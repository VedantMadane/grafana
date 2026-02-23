package migrations

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/grafana/grafana/pkg/infra/db"
	"github.com/grafana/grafana/pkg/storage/legacysql"
	"github.com/grafana/grafana/pkg/storage/unified/resourcepb"
	"github.com/grafana/grafana/pkg/util/testutil"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

type tableLockerMock struct {
	unlockFunc func(context.Context) error
	tables     []string
}

func (m *tableLockerMock) LockMigrationTables(ctx context.Context, tables []string) (func(context.Context) error, error) {
	m.tables = tables
	return m.unlockFunc, nil
}

func TestIntegrationMigrationRunnerLocksTables(t *testing.T) {
	testutil.SkipIntegrationTestInShortMode(t)

	dbstore := db.InitTestDB(t)
	t.Cleanup(db.CleanupTestDB)

	dummyResource := schema.GroupResource{Group: "group", Resource: "resource"}
	unlockCalled := false
	locker := &tableLockerMock{
		unlockFunc: func(context.Context) error {
			unlockCalled = true
			return nil
		},
	}
	registry := NewMigrationRegistry()
	registry.Register(MigrationDefinition{
		ID: "dummy-resource-migration",
		Resources: []ResourceInfo{
			{GroupResource: dummyResource, LockTables: []string{"resource"}},
		},
		Migrators: map[schema.GroupResource]MigratorFunc{
			dummyResource: func(context.Context, int64, MigrateOptions, resourcepb.BulkStore_BulkProcessClient) error {
				return nil
			},
		},
	})
	mockMigrator := NewMockUnifiedMigrator(t)
	mockMigrator.EXPECT().Migrate(mock.Anything, mock.Anything).Return(&resourcepb.BulkResponse{}, nil)
	mockMigrator.EXPECT().RebuildIndexes(mock.Anything, mock.Anything).Return(nil)

	runner := NewMigrationRunner(mockMigrator, locker, registry, "test", []schema.GroupResource{dummyResource}, nil, nil)
	sess := dbstore.GetEngine().NewSession()
	defer sess.Close()

	// Ensure at least one org exists so Run() doesn't skip migration
	_, err := sess.Exec("INSERT INTO org (id, name, created, updated, version) VALUES (1, 'test', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 1)")
	require.NoError(t, err)

	err = runner.Run(context.Background(), sess, nil, RunOptions{})
	require.NoError(t, err)
	require.True(t, unlockCalled)
	require.Len(t, locker.tables, 1)
	require.Equal(t, []string{"resource"}, locker.tables)
}

// createTestTable creates a uniquely-named table for testing and returns its name.
// The table is automatically dropped when the test finishes.
func createTestTable(t *testing.T, dbstore db.DB) string {
	t.Helper()
	name := fmt.Sprintf("test_lock_%s", uuid.New().String()[:8])
	engine := dbstore.GetEngine()
	_, err := engine.Exec(fmt.Sprintf("CREATE TABLE %s (id INT PRIMARY KEY, val TEXT)", engine.Quote(name)))
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = engine.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", engine.Quote(name)))
	})
	return name
}

func TestIntegrationTableLocker(t *testing.T) {
	testutil.SkipIntegrationTestInShortMode(t)

	dbstore := db.InitTestDB(t)
	t.Cleanup(db.CleanupTestDB)
	sqlProvider := legacysql.NewDatabaseProvider(dbstore)

	t.Run("lock and unlock tables", func(t *testing.T) {
		locker := &legacyTableLocker{sql: sqlProvider}
		ctx := context.Background()

		table1 := createTestTable(t, dbstore)
		table2 := createTestTable(t, dbstore)

		unlock, err := locker.LockMigrationTables(ctx, []string{table1, table2})
		require.NoError(t, err)
		require.NotNil(t, unlock)

		err = unlock(ctx)
		require.NoError(t, err)
	})

	t.Run("empty tables list returns no-op", func(t *testing.T) {
		locker := &legacyTableLocker{sql: sqlProvider}
		ctx := context.Background()

		unlock, err := locker.LockMigrationTables(ctx, []string{})
		require.NoError(t, err)
		require.NotNil(t, unlock)

		err = unlock(ctx)
		require.NoError(t, err)
	})

	t.Run("duplicate tables are deduplicated", func(t *testing.T) {
		locker := &legacyTableLocker{sql: sqlProvider}
		ctx := context.Background()

		table1 := createTestTable(t, dbstore)
		table2 := createTestTable(t, dbstore)

		unlock, err := locker.LockMigrationTables(ctx, []string{table1, table1, table2})
		require.NoError(t, err)
		require.NotNil(t, unlock)

		err = unlock(ctx)
		require.NoError(t, err)
	})

	t.Run("non-existent tables are skipped", func(t *testing.T) {
		locker := &legacyTableLocker{sql: sqlProvider}
		ctx := context.Background()

		table1 := createTestTable(t, dbstore)
		nonExistent := "nonexistent_" + uuid.New().String()[:8]

		unlock, err := locker.LockMigrationTables(ctx, []string{table1, nonExistent})
		require.NoError(t, err)
		require.NotNil(t, unlock)

		err = unlock(ctx)
		require.NoError(t, err)
	})

	t.Run("all non-existent tables returns no-op", func(t *testing.T) {
		locker := &legacyTableLocker{sql: sqlProvider}
		ctx := context.Background()

		nonExistent1 := "nonexistent_" + uuid.New().String()[:8]
		nonExistent2 := "nonexistent_" + uuid.New().String()[:8]

		unlock, err := locker.LockMigrationTables(ctx, []string{nonExistent1, nonExistent2})
		require.NoError(t, err)
		require.NotNil(t, unlock)

		err = unlock(ctx)
		require.NoError(t, err)
	})

	t.Run("verify lock prevents writes via insert", func(t *testing.T) {
		if db.IsTestDbSQLite() {
			t.Skip("Skipping for SQLite, locking is not needed due to single writer connection")
		}

		locker := &legacyTableLocker{sql: sqlProvider}
		ctx := context.Background()

		sqlHelper, err := sqlProvider(ctx)
		require.NoError(t, err)

		table := createTestTable(t, dbstore)

		unlock, err := locker.LockMigrationTables(ctx, []string{table})
		require.NoError(t, err)
		require.NotNil(t, unlock)

		// Try to write to the locked table from a separate connection (goroutine).
		quotedTable := sqlHelper.DB.Quote(table)
		writeErr := make(chan error, 1)
		go func() {
			// UPDATE on a READ-locked table will block until the lock is released.
			_, werr := sqlHelper.DB.GetEngine().Exec(
				"UPDATE " + quotedTable + " SET val = val WHERE id = -1",
			)
			writeErr <- werr
		}()

		// Verify that the write is blocked while the lock is held
		select {
		case <-writeErr:
			t.Fatal("Write should be blocked while lock is held")
		case <-time.After(2 * time.Second):
			// Good — write is still blocked after 2s
		}

		// Release the lock
		require.NoError(t, unlock(ctx))

		// Now write should complete
		select {
		case err = <-writeErr:
			require.NoError(t, err, "Write should succeed after unlock")
		case <-time.After(10 * time.Second):
			t.Fatal("Write is still blocked after unlock")
		}
	})
}
