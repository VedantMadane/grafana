package migrations

import (
	"testing"

	"github.com/grafana/grafana/pkg/infra/db"
	"github.com/grafana/grafana/pkg/services/sqlstore/migrator"
	"github.com/grafana/grafana/pkg/setting"
	"github.com/grafana/grafana/pkg/util/testutil"
	"github.com/stretchr/testify/require"
)

func TestIntegrationRenameLegacyTables(t *testing.T) {
	testutil.SkipIntegrationTestInShortMode(t)

	dbstore := db.InitTestDB(t)
	t.Cleanup(db.CleanupTestDB)
	engine := dbstore.GetEngine()

	mg := migrator.NewMigrator(engine, setting.NewCfg())

	t.Run("renames existing tables", func(t *testing.T) {
		_, err := engine.Exec("CREATE TABLE IF NOT EXISTS test_rename_src (id INTEGER PRIMARY KEY)")
		require.NoError(t, err)
		t.Cleanup(func() {
			_, _ = engine.Exec("DROP TABLE IF EXISTS test_rename_src")
			_, _ = engine.Exec("DROP TABLE IF EXISTS test_rename_src_legacy")
		})

		sess := engine.NewSession()
		defer sess.Close()
		require.NoError(t, sess.Begin())

		err = renameLegacyTables(sess, mg, []string{"test_rename_src"})
		require.NoError(t, err)
		require.NoError(t, sess.Commit())

		exists, err := engine.IsTableExist("test_rename_src")
		require.NoError(t, err)
		require.False(t, exists, "source table should no longer exist")

		exists, err = engine.IsTableExist("test_rename_src_legacy")
		require.NoError(t, err)
		require.True(t, exists, "legacy table should exist")
	})

	t.Run("skips already renamed tables", func(t *testing.T) {
		_, err := engine.Exec("CREATE TABLE IF NOT EXISTS test_skip_legacy (id INTEGER PRIMARY KEY)")
		require.NoError(t, err)
		t.Cleanup(func() {
			_, _ = engine.Exec("DROP TABLE IF EXISTS test_skip")
			_, _ = engine.Exec("DROP TABLE IF EXISTS test_skip_legacy")
		})

		sess := engine.NewSession()
		defer sess.Close()
		require.NoError(t, sess.Begin())

		err = renameLegacyTables(sess, mg, []string{"test_skip"})
		require.NoError(t, err)
	})

	t.Run("errors when source does not exist and target does not exist", func(t *testing.T) {
		sess := engine.NewSession()
		defer sess.Close()
		require.NoError(t, sess.Begin())

		err := renameLegacyTables(sess, mg, []string{"nonexistent_table_xyz"})
		require.Error(t, err)
		require.Contains(t, err.Error(), "does not exist")
	})

	t.Run("errors when both source and target exist", func(t *testing.T) {
		_, err := engine.Exec("CREATE TABLE IF NOT EXISTS test_both (id INTEGER PRIMARY KEY)")
		require.NoError(t, err)
		_, err = engine.Exec("CREATE TABLE IF NOT EXISTS test_both_legacy (id INTEGER PRIMARY KEY)")
		require.NoError(t, err)
		t.Cleanup(func() {
			_, _ = engine.Exec("DROP TABLE IF EXISTS test_both")
			_, _ = engine.Exec("DROP TABLE IF EXISTS test_both_legacy")
		})

		sess := engine.NewSession()
		defer sess.Close()
		require.NoError(t, sess.Begin())

		err = renameLegacyTables(sess, mg, []string{"test_both"})
		require.Error(t, err)
		require.Contains(t, err.Error(), "unexpected state")
	})

	t.Run("renames multiple tables", func(t *testing.T) {
		_, err := engine.Exec("CREATE TABLE IF NOT EXISTS test_multi_a (id INTEGER PRIMARY KEY)")
		require.NoError(t, err)
		_, err = engine.Exec("CREATE TABLE IF NOT EXISTS test_multi_b (id INTEGER PRIMARY KEY)")
		require.NoError(t, err)
		t.Cleanup(func() {
			_, _ = engine.Exec("DROP TABLE IF EXISTS test_multi_a")
			_, _ = engine.Exec("DROP TABLE IF EXISTS test_multi_a_legacy")
			_, _ = engine.Exec("DROP TABLE IF EXISTS test_multi_b")
			_, _ = engine.Exec("DROP TABLE IF EXISTS test_multi_b_legacy")
		})

		sess := engine.NewSession()
		defer sess.Close()
		require.NoError(t, sess.Begin())

		err = renameLegacyTables(sess, mg, []string{"test_multi_a", "test_multi_b"})
		require.NoError(t, err)
		require.NoError(t, sess.Commit())

		for _, name := range []string{"test_multi_a", "test_multi_b"} {
			exists, err := engine.IsTableExist(name)
			require.NoError(t, err)
			require.False(t, exists, "%s should no longer exist", name)

			exists, err = engine.IsTableExist(name + "_legacy")
			require.NoError(t, err)
			require.True(t, exists, "%s_legacy should exist", name)
		}
	})
}
