package migrations

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/grafana/grafana/pkg/infra/db"
	"github.com/grafana/grafana/pkg/services/sqlstore/migrator"
	"github.com/grafana/grafana/pkg/setting"
	"github.com/grafana/grafana/pkg/storage/legacysql"
	"github.com/grafana/grafana/pkg/storage/unified/resourcepb"
	"github.com/grafana/grafana/pkg/util/testutil"
	"github.com/grafana/grafana/pkg/util/xorm"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// --- helpers ----------------------------------------------------------------

// uniqueTable creates a table with a random name and returns the name. Cleanup is automatic.
func uniqueTable(t *testing.T, engine *xorm.Engine) string {
	t.Helper()
	name := fmt.Sprintf("test_%s", uuid.New().String()[:8])
	_, err := engine.Exec(fmt.Sprintf("CREATE TABLE %s (id INT PRIMARY KEY, val TEXT)", engine.Quote(name)))
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = engine.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", engine.Quote(name)))
		_, _ = engine.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", engine.Quote(name+legacySuffix)))
	})
	return name
}

// dummyGR returns a schema.GroupResource for testing.
func dummyGR() schema.GroupResource {
	return schema.GroupResource{Group: "test.group", Resource: "test-resource"}
}

// setupRunnerWithMocks creates a MigrationRunner with mocked UnifiedMigrator and the given locker.
// The mock migrator returns a successful BulkResponse by default.
func setupRunnerWithMocks(t *testing.T, locker MigrationTableLocker, registry *MigrationRegistry, resources []schema.GroupResource, renameTables []string) (*MigrationRunner, *MockUnifiedMigrator) {
	t.Helper()
	mockMigrator := NewMockUnifiedMigrator(t)
	mockMigrator.EXPECT().Migrate(mock.Anything, mock.Anything).Return(&resourcepb.BulkResponse{}, nil)
	mockMigrator.EXPECT().RebuildIndexes(mock.Anything, mock.Anything).Return(nil)

	runner := NewMigrationRunner(mockMigrator, locker, registry, "test-migration", resources, renameTables, nil)
	return runner, mockMigrator
}

// registryWithResource creates a registry with a single dummy resource and given lock tables.
func registryWithResource(gr schema.GroupResource, lockTables []string) *MigrationRegistry {
	registry := NewMigrationRegistry()
	registry.Register(MigrationDefinition{
		ID:          "test-def",
		MigrationID: "test-migration",
		Resources:   []ResourceInfo{{GroupResource: gr, LockTables: lockTables}},
		Migrators: map[schema.GroupResource]MigratorFunc{
			gr: func(context.Context, int64, MigrateOptions, resourcepb.BulkStore_BulkProcessClient) error {
				return nil
			},
		},
	})
	return registry
}

// ensureOrg inserts org row 1 if needed (Run() needs at least one org).
func ensureOrg(t *testing.T, engine *xorm.Engine) {
	t.Helper()
	var count int64
	has, err := engine.NewSession().SQL("SELECT COUNT(*) FROM org WHERE id = 1").Get(&count)
	require.NoError(t, err)
	if !has || count == 0 {
		_, err := engine.Exec("INSERT INTO org (id, name, created, updated, version) VALUES (1, 'test', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 1)")
		require.NoError(t, err)
	}
}

// --- Run() locking strategy tests -------------------------------------------

func TestIntegrationRun_SQLite_NoLock(t *testing.T) {
	testutil.SkipIntegrationTestInShortMode(t)
	if !db.IsTestDbSQLite() {
		t.Skip("SQLite-only test")
	}

	dbstore := db.InitTestDB(t)
	t.Cleanup(db.CleanupTestDB)
	engine := dbstore.GetEngine()
	ensureOrg(t, engine)

	gr := dummyGR()
	table := uniqueTable(t, engine)
	registry := registryWithResource(gr, []string{table})

	lockCalled := false
	locker := &tableLockerMock{
		unlockFunc: func(context.Context) error {
			lockCalled = true
			return nil
		},
	}

	runner, _ := setupRunnerWithMocks(t, locker, registry, []schema.GroupResource{gr}, nil)

	mg := migrator.NewMigrator(engine, setting.NewCfg())
	sess := engine.NewSession()
	defer sess.Close()
	require.NoError(t, sess.Begin())

	err := runner.Run(context.Background(), sess, mg, RunOptions{DriverName: migrator.SQLite})
	require.NoError(t, err)

	// SQLite should NOT call the table locker at all
	require.False(t, lockCalled, "SQLite should not call tableLocker")
	require.Empty(t, locker.tables, "SQLite should not pass tables to locker")
}

func TestIntegrationRun_Postgres_LocksOnSession(t *testing.T) {
	testutil.SkipIntegrationTestInShortMode(t)
	if !db.IsTestDbPostgres() {
		t.Skip("Postgres-only test")
	}

	dbstore := db.InitTestDB(t)
	t.Cleanup(db.CleanupTestDB)
	engine := dbstore.GetEngine()
	ensureOrg(t, engine)

	gr := dummyGR()
	table := uniqueTable(t, engine)
	registry := registryWithResource(gr, []string{table})

	lockCalled := false
	locker := &tableLockerMock{
		unlockFunc: func(context.Context) error {
			lockCalled = true
			return nil
		},
	}

	runner, _ := setupRunnerWithMocks(t, locker, registry, []schema.GroupResource{gr}, nil)

	mg := migrator.NewMigrator(engine, setting.NewCfg())
	sess := engine.NewSession()
	defer sess.Close()
	require.NoError(t, sess.Begin())

	err := runner.Run(context.Background(), sess, mg, RunOptions{DriverName: migrator.Postgres})
	require.NoError(t, err)

	// Postgres locks on sess, not via the separate tableLocker
	require.False(t, lockCalled, "Postgres should lock on sess, not via tableLocker")
}

func TestIntegrationRun_MySQL_UsesTableLocker(t *testing.T) {
	testutil.SkipIntegrationTestInShortMode(t)
	if !db.IsTestDbMySQL() {
		t.Skip("MySQL-only test")
	}

	dbstore := db.InitTestDB(t)
	t.Cleanup(db.CleanupTestDB)
	engine := dbstore.GetEngine()
	ensureOrg(t, engine)

	gr := dummyGR()
	table := uniqueTable(t, engine)
	registry := registryWithResource(gr, []string{table})

	unlockCalled := false
	locker := &tableLockerMock{
		unlockFunc: func(context.Context) error {
			unlockCalled = true
			return nil
		},
	}

	runner, _ := setupRunnerWithMocks(t, locker, registry, []schema.GroupResource{gr}, nil)

	mg := migrator.NewMigrator(engine, setting.NewCfg())
	sess := engine.NewSession()
	defer sess.Close()
	require.NoError(t, sess.Begin())

	err := runner.Run(context.Background(), sess, mg, RunOptions{DriverName: migrator.MySQL})
	require.NoError(t, err)

	require.True(t, unlockCalled, "MySQL should use tableLocker and call unlock")
	require.Equal(t, []string{table}, locker.tables)
}

// --- lockTablesOnSession tests ----------------------------------------------

func TestIntegrationLockTablesOnSession(t *testing.T) {
	testutil.SkipIntegrationTestInShortMode(t)
	if !db.IsTestDbPostgres() {
		t.Skip("Postgres-only test (LOCK TABLE ... IN SHARE MODE)")
	}

	dbstore := db.InitTestDB(t)
	t.Cleanup(db.CleanupTestDB)
	engine := dbstore.GetEngine()
	mg := migrator.NewMigrator(engine, setting.NewCfg())

	t.Run("locks existing tables", func(t *testing.T) {
		table := uniqueTable(t, engine)

		sess := engine.NewSession()
		defer sess.Close()
		require.NoError(t, sess.Begin())

		err := lockTablesOnSession(sess, mg, []string{table})
		require.NoError(t, err)

		// Verify the lock blocks writes from another session
		writeErr := make(chan error, 1)
		go func() {
			// UPDATE on a SHARE-locked table blocks until lock released
			_, werr := engine.Exec(
				fmt.Sprintf("UPDATE %s SET val = 'x' WHERE id = -1", engine.Quote(table)),
			)
			writeErr <- werr
		}()

		select {
		case <-writeErr:
			t.Fatal("Write should be blocked while SHARE lock is held")
		case <-time.After(1 * time.Second):
			// Good — write is blocked
		}

		// Rollback releases the lock
		require.NoError(t, sess.Rollback())

		select {
		case err := <-writeErr:
			require.NoError(t, err, "Write should succeed after lock released")
		case <-time.After(10 * time.Second):
			t.Fatal("Write still blocked after rollback")
		}
	})

	t.Run("skips non-existent tables", func(t *testing.T) {
		table := uniqueTable(t, engine)
		nonExistent := "nonexistent_" + uuid.New().String()[:8]

		sess := engine.NewSession()
		defer sess.Close()
		require.NoError(t, sess.Begin())

		err := lockTablesOnSession(sess, mg, []string{table, nonExistent})
		require.NoError(t, err)
		require.NoError(t, sess.Rollback())
	})

	t.Run("no-op for empty table list", func(t *testing.T) {
		sess := engine.NewSession()
		defer sess.Close()
		require.NoError(t, sess.Begin())

		err := lockTablesOnSession(sess, mg, nil)
		require.NoError(t, err)
		require.NoError(t, sess.Rollback())
	})
}

// --- Postgres rename in same transaction ------------------------------------

func TestIntegrationRun_Postgres_RenameInTransaction(t *testing.T) {
	testutil.SkipIntegrationTestInShortMode(t)
	if !db.IsTestDbPostgres() {
		t.Skip("Postgres-only test")
	}

	dbstore := db.InitTestDB(t)
	t.Cleanup(db.CleanupTestDB)
	engine := dbstore.GetEngine()
	ensureOrg(t, engine)

	gr := dummyGR()
	table := uniqueTable(t, engine)
	registry := registryWithResource(gr, []string{table})

	locker := &tableLockerMock{
		unlockFunc: func(context.Context) error { return nil },
	}

	runner, _ := setupRunnerWithMocks(t, locker, registry, []schema.GroupResource{gr}, []string{table})

	mg := migrator.NewMigrator(engine, setting.NewCfg())
	sess := engine.NewSession()
	defer sess.Close()
	require.NoError(t, sess.Begin())

	err := runner.Run(context.Background(), sess, mg, RunOptions{DriverName: migrator.Postgres})
	require.NoError(t, err)
	require.NoError(t, sess.Commit())

	// Verify rename happened
	exists, err := engine.IsTableExist(table)
	require.NoError(t, err)
	require.False(t, exists, "original table should be gone")

	exists, err = engine.IsTableExist(table + legacySuffix)
	require.NoError(t, err)
	require.True(t, exists, "legacy table should exist")
}

func TestIntegrationRun_MigratorRenames(t *testing.T) {
	testutil.SkipIntegrationTestInShortMode(t)
	if !db.IsTestDbMySQL() {
		t.Skip("MySQL-only test")
	}

	dbstore := db.InitTestDB(t)
	t.Cleanup(db.CleanupTestDB)
	engine := dbstore.GetEngine()
	ensureOrg(t, engine)

	gr := dummyGR()
	table := uniqueTable(t, engine)
	registry := registryWithResource(gr, []string{table})

	sqlProvider := legacysql.NewDatabaseProvider(dbstore)
	locker := &legacyTableLocker{sql: sqlProvider}

	runner, _ := setupRunnerWithMocks(t, locker, registry, []schema.GroupResource{gr}, []string{table})

	mg := migrator.NewMigrator(engine, setting.NewCfg())
	sess := engine.NewSession()
	defer sess.Close()
	require.NoError(t, sess.Begin())

	err := runner.Run(context.Background(), sess, mg, RunOptions{DriverName: migrator.MySQL})
	require.NoError(t, err)

	// Verify rename happened
	exists, err := engine.IsTableExist(table)
	require.NoError(t, err)
	require.False(t, exists, "original table should be gone after MySQL rename")

	exists, err = engine.IsTableExist(table + legacySuffix)
	require.NoError(t, err)
	require.True(t, exists, "legacy table should exist after MySQL rename")
}

// --- MySQL: verify all DML types (INSERT, UPDATE, DELETE) fail after rename ---

func TestIntegrationMySQL_DDLPriorityOverDML(t *testing.T) {
	testutil.SkipIntegrationTestInShortMode(t)
	if !db.IsTestDbMySQL() {
		t.Skip("MySQL-only test")
	}

	dbstore := db.InitTestDB(t)
	t.Cleanup(db.CleanupTestDB)
	engine := dbstore.GetEngine()

	table := uniqueTable(t, engine)

	// Insert rows so UPDATE and DELETE have something to target
	_, err := engine.Exec(fmt.Sprintf("INSERT INTO %s (id, val) VALUES (1, 'row1'), (2, 'row2'), (3, 'row3')", engine.Quote(table)))
	require.NoError(t, err)

	// Acquire READ lock
	lockConn, err := engine.DB().Conn(context.Background())
	require.NoError(t, err)
	defer lockConn.Close()

	_, err = lockConn.ExecContext(context.Background(), fmt.Sprintf("LOCK TABLES %s READ", engine.Quote(table)))
	require.NoError(t, err)

	// Queue RENAME (blocks on MDL)
	renameDone := make(chan error, 1)
	go func() {
		conn, cerr := engine.DB().Conn(context.Background())
		if cerr != nil {
			renameDone <- cerr
			return
		}
		defer conn.Close()
		_, rerr := conn.ExecContext(context.Background(),
			fmt.Sprintf("RENAME TABLE %s TO %s", engine.Quote(table), engine.Quote(table+legacySuffix)))
		renameDone <- rerr
	}()

	// Queue all three DML types concurrently
	type dmlResult struct {
		name string
		err  error
	}
	dmlResults := make(chan dmlResult, 3)

	dmlStatements := []struct {
		name string
		sql  string
	}{
		{"INSERT", fmt.Sprintf("INSERT INTO %s (id, val) VALUES (10, 'new')", engine.Quote(table))},
		{"UPDATE", fmt.Sprintf("UPDATE %s SET val = 'modified' WHERE id = 1", engine.Quote(table))},
		{"DELETE", fmt.Sprintf("DELETE FROM %s WHERE id = 2", engine.Quote(table))},
	}

	for _, stmt := range dmlStatements {
		go func(name, sql string) {
			conn, cerr := engine.DB().Conn(context.Background())
			if cerr != nil {
				dmlResults <- dmlResult{name, cerr}
				return
			}
			defer conn.Close()
			_, derr := conn.ExecContext(context.Background(), sql)
			dmlResults <- dmlResult{name, derr}
		}(stmt.name, stmt.sql)
	}

	// Wait for all to queue
	time.Sleep(500 * time.Millisecond)

	// Release READ lock — RENAME executes first (DDL priority)
	_, err = lockConn.ExecContext(context.Background(), "UNLOCK TABLES")
	require.NoError(t, err)

	// RENAME should succeed
	select {
	case err := <-renameDone:
		require.NoError(t, err, "RENAME should succeed")
	case <-time.After(10 * time.Second):
		t.Fatal("RENAME timed out")
	}

	// All DML should fail with "table doesn't exist"
	for i := 0; i < 3; i++ {
		select {
		case result := <-dmlResults:
			require.Error(t, result.err, "%s should fail because table was renamed", result.name)
			require.True(t,
				strings.Contains(result.err.Error(), "doesn't exist") || strings.Contains(result.err.Error(), "not found"),
				"%s: expected 'table doesn't exist' error, got: %v", result.name, result.err)
			t.Logf("%s correctly failed: %v", result.name, result.err)
		case <-time.After(10 * time.Second):
			t.Fatalf("DML %d/3 timed out", i+1)
		}
	}

	// Verify original data is intact in the renamed table
	var count int
	err = engine.DB().QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", engine.Quote(table+legacySuffix))).Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 3, count, "all 3 original rows should be intact — no DML should have succeeded")
}

// --- MySQL: per-table queueing (one connection per rename) ------------------

func TestIntegrationMySQL_PerTableRenameQueueing(t *testing.T) {
	testutil.SkipIntegrationTestInShortMode(t)
	if !db.IsTestDbMySQL() {
		t.Skip("MySQL-only test")
	}

	dbstore := db.InitTestDB(t)
	t.Cleanup(db.CleanupTestDB)
	engine := dbstore.GetEngine()
	ensureOrg(t, engine)

	gr := dummyGR()
	table1 := uniqueTable(t, engine)
	table2 := uniqueTable(t, engine)
	registry := registryWithResource(gr, []string{table1, table2})

	sqlProvider := legacysql.NewDatabaseProvider(dbstore)
	locker := &legacyTableLocker{sql: sqlProvider}

	runner, _ := setupRunnerWithMocks(t, locker, registry, []schema.GroupResource{gr}, []string{table1, table2})

	mg := migrator.NewMigrator(engine, setting.NewCfg())
	sess := engine.NewSession()
	defer sess.Close()
	require.NoError(t, sess.Begin())

	err := runner.Run(context.Background(), sess, mg, RunOptions{DriverName: migrator.MySQL})
	require.NoError(t, err)

	// Both tables should be renamed
	for _, table := range []string{table1, table2} {
		exists, err := engine.IsTableExist(table)
		require.NoError(t, err)
		require.False(t, exists, "%s should be gone", table)

		exists, err = engine.IsTableExist(table + legacySuffix)
		require.NoError(t, err)
		require.True(t, exists, "%s_legacy should exist", table)
	}
}

// --- MySQL: waitForRenamesQueued with processlist ---------------------------

func TestIntegrationMySQL_WaitForRenamesQueued(t *testing.T) {
	testutil.SkipIntegrationTestInShortMode(t)
	if !db.IsTestDbMySQL() {
		t.Skip("MySQL-only test")
	}

	dbstore := db.InitTestDB(t)
	t.Cleanup(db.CleanupTestDB)
	engine := dbstore.GetEngine()

	table := uniqueTable(t, engine)

	runner := &MigrationRunner{
		log:          logger,
		renameTables: []string{table},
	}

	// Create a session to use for processlist polling (same as Run() does)
	sess := engine.NewSession()
	defer sess.Close()
	require.NoError(t, sess.Begin())

	// Acquire READ lock on dedicated connection
	lockConn, err := engine.DB().Conn(context.Background())
	require.NoError(t, err)
	defer lockConn.Close()

	_, err = lockConn.ExecContext(context.Background(), fmt.Sprintf("LOCK TABLES %s READ", engine.Quote(table)))
	require.NoError(t, err)
	defer func() {
		_, _ = lockConn.ExecContext(context.Background(), "UNLOCK TABLES")
	}()

	// Queue RENAME on a separate connection (blocks on MDL)
	renameConn, err := engine.DB().Conn(context.Background())
	require.NoError(t, err)
	defer renameConn.Close()

	renameDone := make(chan error, 1)
	go func() {
		_, rerr := renameConn.ExecContext(context.Background(),
			fmt.Sprintf("RENAME TABLE %s TO %s", engine.Quote(table), engine.Quote(table+legacySuffix)))
		renameDone <- rerr
	}()

	// waitForRenamesQueued should detect the RENAME in processlist
	pairs := []renamePair{{oldName: table, newName: table + legacySuffix}}
	err = runner.waitForRenamesQueued(sess, pairs)
	require.NoError(t, err)

	// Release lock and let RENAME complete
	_, _ = lockConn.ExecContext(context.Background(), "UNLOCK TABLES")
	select {
	case err := <-renameDone:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("RENAME timed out")
	}
}

// --- MySQL: waitForRenamesQueued matches specific table names ---------------

func TestIntegrationMySQL_WaitForRenamesQueued_MatchesTableNames(t *testing.T) {
	testutil.SkipIntegrationTestInShortMode(t)
	if !db.IsTestDbMySQL() {
		t.Skip("MySQL-only test")
	}

	dbstore := db.InitTestDB(t)
	t.Cleanup(db.CleanupTestDB)
	engine := dbstore.GetEngine()

	table1 := uniqueTable(t, engine)
	table2 := uniqueTable(t, engine)

	runner := &MigrationRunner{
		log: logger,
	}

	// Create a session to use for processlist polling
	sess := engine.NewSession()
	defer sess.Close()
	require.NoError(t, sess.Begin())

	// Lock both tables
	lockConn, err := engine.DB().Conn(context.Background())
	require.NoError(t, err)
	defer lockConn.Close()

	_, err = lockConn.ExecContext(context.Background(),
		fmt.Sprintf("LOCK TABLES %s READ, %s READ", engine.Quote(table1), engine.Quote(table2)))
	require.NoError(t, err)
	defer func() {
		_, _ = lockConn.ExecContext(context.Background(), "UNLOCK TABLES")
	}()

	// Queue renames on separate connections
	var wg sync.WaitGroup
	renameResults := make([]chan error, 2)
	for i, table := range []string{table1, table2} {
		renameResults[i] = make(chan error, 1)
		wg.Add(1)
		go func(tbl string, ch chan<- error) {
			defer wg.Done()
			conn, cerr := engine.DB().Conn(context.Background())
			if cerr != nil {
				ch <- cerr
				return
			}
			defer conn.Close()
			_, rerr := conn.ExecContext(context.Background(),
				fmt.Sprintf("RENAME TABLE %s TO %s", engine.Quote(tbl), engine.Quote(tbl+legacySuffix)))
			ch <- rerr
		}(table, renameResults[i])
	}

	// Wait for both renames to appear in processlist
	pairs := []renamePair{
		{oldName: table1, newName: table1 + legacySuffix},
		{oldName: table2, newName: table2 + legacySuffix},
	}
	err = runner.waitForRenamesQueued(sess, pairs)
	require.NoError(t, err)

	// Now test that waiting for a non-existent table times out
	pairsMismatch := []renamePair{
		{oldName: table1, newName: table1 + legacySuffix},
		{oldName: "nonexistent_xyz", newName: "nonexistent_xyz" + legacySuffix},
	}
	// This should still return nil (timeout fallback) but log a warning
	err = runner.waitForRenamesQueued(sess, pairsMismatch)
	require.NoError(t, err) // timeout is non-fatal

	// Cleanup: release lock and wait for renames
	_, _ = lockConn.ExecContext(context.Background(), "UNLOCK TABLES")
	for _, ch := range renameResults {
		select {
		case err := <-ch:
			require.NoError(t, err)
		case <-time.After(10 * time.Second):
			t.Fatal("RENAME timed out")
		}
	}
}

// --- SQLite rename in same transaction --------------------------------------

func TestIntegrationRun_SQLite_RenameInTransaction(t *testing.T) {
	testutil.SkipIntegrationTestInShortMode(t)
	if !db.IsTestDbSQLite() {
		t.Skip("SQLite-only test")
	}

	dbstore := db.InitTestDB(t)
	t.Cleanup(db.CleanupTestDB)
	engine := dbstore.GetEngine()
	ensureOrg(t, engine)

	gr := dummyGR()
	table := uniqueTable(t, engine)
	registry := registryWithResource(gr, []string{table})

	locker := &tableLockerMock{
		unlockFunc: func(context.Context) error { return nil },
	}

	runner, _ := setupRunnerWithMocks(t, locker, registry, []schema.GroupResource{gr}, []string{table})

	mg := migrator.NewMigrator(engine, setting.NewCfg())
	sess := engine.NewSession()
	defer sess.Close()
	require.NoError(t, sess.Begin())

	err := runner.Run(context.Background(), sess, mg, RunOptions{DriverName: migrator.SQLite})
	require.NoError(t, err)
	require.NoError(t, sess.Commit())

	// Verify rename happened
	exists, err := engine.IsTableExist(table)
	require.NoError(t, err)
	require.False(t, exists, "original table should be gone")

	exists, err = engine.IsTableExist(table + legacySuffix)
	require.NoError(t, err)
	require.True(t, exists, "legacy table should exist")
}

// --- No rename: verify no rename attempt ------------------------------------

func TestIntegrationRun_NoRename(t *testing.T) {
	testutil.SkipIntegrationTestInShortMode(t)

	dbstore := db.InitTestDB(t)
	t.Cleanup(db.CleanupTestDB)
	engine := dbstore.GetEngine()
	ensureOrg(t, engine)

	gr := dummyGR()
	table := uniqueTable(t, engine)
	registry := registryWithResource(gr, []string{table})

	locker := &tableLockerMock{
		unlockFunc: func(context.Context) error { return nil },
	}

	// No rename tables
	runner, _ := setupRunnerWithMocks(t, locker, registry, []schema.GroupResource{gr}, nil)

	driverName := migrator.SQLite
	if db.IsTestDbMySQL() {
		driverName = migrator.MySQL
	} else if db.IsTestDbPostgres() {
		driverName = migrator.Postgres
	}

	mg := migrator.NewMigrator(engine, setting.NewCfg())
	sess := engine.NewSession()
	defer sess.Close()
	require.NoError(t, sess.Begin())

	err := runner.Run(context.Background(), sess, mg, RunOptions{DriverName: driverName})
	require.NoError(t, err)

	// Table should still exist (not renamed)
	exists, err := engine.IsTableExist(table)
	require.NoError(t, err)
	require.True(t, exists, "table should still exist when no rename configured")

	exists, err = engine.IsTableExist(table + legacySuffix)
	require.NoError(t, err)
	require.False(t, exists, "legacy table should not exist when no rename configured")
}

// --- Postgres: SHARE lock blocks writes, allows reads -----------------------

func TestIntegrationPostgres_ShareLockBlocksWritesAllowsReads(t *testing.T) {
	testutil.SkipIntegrationTestInShortMode(t)
	if !db.IsTestDbPostgres() {
		t.Skip("Postgres-only test")
	}

	dbstore := db.InitTestDB(t)
	t.Cleanup(db.CleanupTestDB)
	engine := dbstore.GetEngine()
	mg := migrator.NewMigrator(engine, setting.NewCfg())

	table := uniqueTable(t, engine)
	_, err := engine.Exec(fmt.Sprintf("INSERT INTO %s (id, val) VALUES (1, 'test')", engine.Quote(table)))
	require.NoError(t, err)

	// Acquire SHARE lock on sess
	sess := engine.NewSession()
	defer sess.Close()
	require.NoError(t, sess.Begin())
	require.NoError(t, lockTablesOnSession(sess, mg, []string{table}))

	// Reads should work from another connection
	var val string
	err = engine.DB().QueryRow(fmt.Sprintf("SELECT val FROM %s WHERE id = 1", engine.Quote(table))).Scan(&val)
	require.NoError(t, err)
	require.Equal(t, "test", val)

	// Writes should block
	writeErr := make(chan error, 1)
	go func() {
		_, werr := engine.Exec(fmt.Sprintf("INSERT INTO %s (id, val) VALUES (2, 'blocked')", engine.Quote(table)))
		writeErr <- werr
	}()

	select {
	case <-writeErr:
		t.Fatal("Write should be blocked while SHARE lock is held")
	case <-time.After(1 * time.Second):
		// Good — write is blocked
	}

	// Release lock
	require.NoError(t, sess.Rollback())

	select {
	case err := <-writeErr:
		require.NoError(t, err, "Write should succeed after lock released")
	case <-time.After(10 * time.Second):
		t.Fatal("Write still blocked after lock release")
	}
}

// --- No early log write for MySQL -------------------------------------------

func TestResourceMigration_NoEarlyLogWriteForMySQL(t *testing.T) {
	// Verify that ResourceMigration no longer has a logWritten field
	// and SkipMigrationLog only depends on autoMigrate && hadErrors
	m := &ResourceMigration{
		autoMigrate: false,
		hadErrors:   false,
	}
	require.False(t, m.SkipMigrationLog())

	m.autoMigrate = true
	m.hadErrors = false
	require.False(t, m.SkipMigrationLog())

	m.autoMigrate = false
	m.hadErrors = true
	require.False(t, m.SkipMigrationLog())

	m.autoMigrate = true
	m.hadErrors = true
	require.True(t, m.SkipMigrationLog())
}

// --- buildRenamePairs: already renamed (crash recovery) ---------------------

func TestIntegrationBuildRenamePairs_CrashRecovery(t *testing.T) {
	testutil.SkipIntegrationTestInShortMode(t)

	dbstore := db.InitTestDB(t)
	t.Cleanup(db.CleanupTestDB)
	engine := dbstore.GetEngine()
	mg := migrator.NewMigrator(engine, setting.NewCfg())

	t.Run("skips already renamed tables", func(t *testing.T) {
		// Create only the _legacy table (simulates crash after rename but before log commit)
		name := fmt.Sprintf("test_crash_%s", uuid.New().String()[:8])
		_, err := engine.Exec(fmt.Sprintf("CREATE TABLE %s (id INT PRIMARY KEY)", engine.Quote(name+legacySuffix)))
		require.NoError(t, err)
		t.Cleanup(func() {
			_, _ = engine.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", engine.Quote(name)))
			_, _ = engine.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", engine.Quote(name+legacySuffix)))
		})

		pairs, err := buildRenamePairs(mg, []string{name})
		require.NoError(t, err)
		require.Empty(t, pairs, "should skip already-renamed table")
	})

	t.Run("returns pair for table needing rename", func(t *testing.T) {
		table := uniqueTable(t, engine)

		pairs, err := buildRenamePairs(mg, []string{table})
		require.NoError(t, err)
		require.Len(t, pairs, 1)
		require.Equal(t, table, pairs[0].oldName)
		require.Equal(t, table+legacySuffix, pairs[0].newName)
	})

	t.Run("errors when both source and target exist", func(t *testing.T) {
		table := uniqueTable(t, engine)
		_, err := engine.Exec(fmt.Sprintf("CREATE TABLE %s (id INT PRIMARY KEY)", engine.Quote(table+legacySuffix)))
		require.NoError(t, err)

		_, err = buildRenamePairs(mg, []string{table})
		require.Error(t, err)
		require.Contains(t, err.Error(), "unexpected state")
	})
}
