package migrations

import (
	"fmt"
	"strings"

	"github.com/grafana/grafana/pkg/infra/log"
	"github.com/grafana/grafana/pkg/services/sqlstore/migrator"
	"github.com/grafana/grafana/pkg/util/xorm"
)

const legacySuffix = "_legacy"

var tableRenameLog = log.New("migrations.table_rename")

// renamePair holds the old and new names for a table rename.
type renamePair struct {
	oldName string
	newName string
}

// buildRenamePairs validates table state and returns pairs that need renaming.
// It checks that the source table exists and the target (_legacy) table doesn't.
// Tables already renamed (source missing, target exists) are skipped.
func buildRenamePairs(mg *migrator.Migrator, tables []string) ([]renamePair, error) {
	var toRename []renamePair

	for _, table := range tables {
		newName := table + legacySuffix

		sourceExists, err := mg.DBEngine.IsTableExist(table)
		if err != nil {
			return nil, fmt.Errorf("failed to check if table %q exists: %w", table, err)
		}

		targetExists, err := mg.DBEngine.IsTableExist(newName)
		if err != nil {
			return nil, fmt.Errorf("failed to check if table %q exists: %w", newName, err)
		}

		if !sourceExists && targetExists {
			tableRenameLog.Info("table already renamed, skipping", "table", table, "newName", newName)
			continue
		}

		if !sourceExists {
			return nil, fmt.Errorf("table %q does not exist and neither does %q", table, newName)
		}

		if sourceExists && targetExists {
			return nil, fmt.Errorf("both %q and %q exist, unexpected state", table, newName)
		}

		toRename = append(toRename, renamePair{oldName: table, newName: newName})
	}

	return toRename, nil
}

// renameLegacyTables renames legacy tables by appending _legacy suffix.
// On MySQL, all renames are batched into a single atomic RENAME TABLE statement.
// On Postgres/SQLite, individual ALTER TABLE statements are used (DDL is transactional).
func renameLegacyTables(sess *xorm.Session, mg *migrator.Migrator, tables []string) error {
	toRename, err := buildRenamePairs(mg, tables)
	if err != nil {
		return err
	}

	if len(toRename) == 0 {
		return nil
	}

	for _, p := range toRename {
		renameSQL := mg.Dialect.RenameTable(p.oldName, p.newName)
		tableRenameLog.Info("renaming legacy table", "table", p.oldName, "newName", p.newName, "sql", renameSQL)
		if _, err := sess.Exec(renameSQL); err != nil {
			return fmt.Errorf("failed to rename table %q to %q: %w", p.oldName, p.newName, err)
		}
	}

	return nil
}

// buildMySQLBatchRename builds a single RENAME TABLE statement for multiple tables.
// e.g., RENAME TABLE `playlist` TO `playlist_legacy`, `playlist_item` TO `playlist_item_legacy`
func buildMySQLBatchRename(d migrator.Dialect, pairs []renamePair) string {
	parts := make([]string, 0, len(pairs))
	for _, p := range pairs {
		parts = append(parts, fmt.Sprintf("%s TO %s", d.Quote(p.oldName), d.Quote(p.newName)))
	}
	return "RENAME TABLE " + strings.Join(parts, ", ")
}
