package generic

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"text/template"

	"github.com/sirupsen/logrus"
)

var (
	columns = "kv.id as theid, kv.name, kv.created, kv.deleted, kv.create_revision, kv.prev_revision, kv.lease, kv.value, kv.old_value"

	revSQL = `
		SELECT rkv.id
		FROM {{ .TableName }} rkv
		ORDER BY rkv.id
		DESC LIMIT 1`

	getByRevSQL = `
		SELECT
		({{ .Rev }}), ({{ .CompactRev }}), {{ .Columns }}
		FROM {{ .TableName }} kv
		WHERE kv.id = ?`

	compactRevSQL = `
		SELECT crkv.prev_revision
		FROM {{ .TableName }} crkv
		WHERE crkv.name = 'compact_rev_key'
		ORDER BY crkv.id DESC LIMIT 1`

	idOfKey = `
		AND mkv.id <= ? AND mkv.id > (
			SELECT ikv.id
			FROM {{ .TableName }} ikv
			WHERE
				ikv.name = ? AND
				ikv.id <= ?
			ORDER BY ikv.id DESC LIMIT 1)`

	listSQL = `SELECT ({{ .Rev }}), ({{ .CompactRev }}), {{ .Columns }}
		FROM {{ .TableName }} kv
		JOIN (
			SELECT MAX(mkv.id) as id
			FROM {{ .TableName }} mkv
			WHERE
				mkv.name LIKE ?
				{{ .OptionalWhereClause }}
			GROUP BY mkv.name) maxkv
	    ON maxkv.id = kv.id
		WHERE
			  (kv.deleted = 0 OR ?)
		ORDER BY kv.id ASC
		`

	countSQL = `
		SELECT ({{ .Rev }}), COUNT(c.theid)
		FROM (
			` + listSQL + `
		) c`

	afterSQL = `
		SELECT ({{ .Rev }}), ({{ .CompactRev }}), {{ .Columns }}
		FROM {{ .TableName }} kv
		WHERE
			kv.name LIKE ? AND
			kv.id > ?
		ORDER BY kv.id ASC`

	deleteSQL = `
		DELETE FROM {{ .TableName }}
		WHERE id = ?`

	updateCompactSQL = `
		UPDATE {{ .TableName }}
		SET prev_revision = ?
		WHERE name = 'compact_rev_key'`

	insertLastInsertIDSQL = `INSERT INTO {{ .TableName }}(name, created, deleted, create_revision, prev_revision, lease, value, old_value)
			values(?, ?, ?, ?, ?, ?, ?, ?)`

	insertSQL = `INSERT INTO {{ .TableName }}(name, created, deleted, create_revision, prev_revision, lease, value, old_value)
			values(?, ?, ?, ?, ?, ?, ?, ?) RETURNING id`

	migrateCountKineSQL = `SELECT COUNT(*) FROM {{ .TableName }}`

	migrateDataSQL = `INSERT INTO {{ .TableName }}(deleted, create_revision, prev_revision, name, value, created, lease)
		SELECT 0, 0, 0, kv.name, kv.value, 1, CASE WHEN kv.ttl > 0 THEN 15 ELSE 0 END
		FROM key_value kv
			WHERE kv.id IN (SELECT MAX(kvd.id) FROM key_value kvd GROUP BY kvd.name)`
)

type Stripped string

func (s Stripped) String() string {
	str := strings.ReplaceAll(string(s), "\n", "")
	return regexp.MustCompile("[\t ]+").ReplaceAllString(str, " ")
}

type Generic struct {
	LastInsertID bool
	DB           *sql.DB

	GetCurrentSQL        string
	GetRevisionSQL       string
	RevisionSQL          string
	ListRevisionStartSQL string
	GetRevisionAfterSQL  string
	CountSQL             string
	AfterSQL             string
	DeleteSQL            string

	// compact SQLs
	UpdateCompactSQL string
	GetCompactRevSQL string

	// insertion SQLs
	InsertSQL             string
	InsertLastInsertIDSQL string

	// migration SQLs
	MigrateCountKineSQL string
	MigrateDataKineSQL  string
}

func q(sql, param string, numbered bool) string {
	if param == "?" && !numbered {
		return sql
	}

	regex := regexp.MustCompile(`\?`)
	n := 0
	return regex.ReplaceAllStringFunc(sql, func(string) string {
		if numbered {
			n++
			return param + strconv.Itoa(n)
		}
		return param
	})
}

func (d *Generic) Migrate(ctx context.Context) {
	var (
		count     = 0
		countKV   = d.queryRow(ctx, "SELECT COUNT(*) FROM key_value")
		countKine = d.queryRow(ctx, d.MigrateCountKineSQL)
	)

	if err := countKV.Scan(&count); err != nil || count == 0 {
		return
	}

	if err := countKine.Scan(&count); err != nil || count != 0 {
		return
	}

	logrus.Infof("Migrating content from old table")
	_, err := d.execute(ctx, d.MigrateDataKineSQL)
	if err != nil {
		logrus.Errorf("Migration failed: %v", err)
	}
}

func Open(driverName, dataSourceName, tableName string, paramCharacter string, numbered bool) (*Generic, error) {
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		return nil, err
	}

	renderedRevisionSQL, err := RenderSQLFromTemplate("revision-SQL", q(revSQL, paramCharacter, numbered), SQLTemplateParameter{
		TableName: tableName,
	})
	if err != nil {
		return nil, err
	}

	renderedGetCompactRevSQL, err := RenderSQLFromTemplate("get-compact-revision-SQL", q(compactRevSQL, paramCharacter, numbered), SQLTemplateParameter{
		TableName: tableName,
	})
	if err != nil {
		return nil, err
	}

	renderedGetRevisionSQL, err := RenderSQLFromTemplate("get-revision-SQL", q(getByRevSQL, paramCharacter, numbered), SQLTemplateParameter{
		TableName:  tableName,
		Rev:        "0",
		CompactRev: "0",
		Columns:    columns,
	})
	if err != nil {
		return nil, err
	}

	renderedGetCurrentSQL, err := RenderSQLFromTemplate("get-current-SQL", q(listSQL, paramCharacter, numbered), SQLTemplateParameter{
		TableName:           tableName,
		Rev:                 renderedRevisionSQL,
		CompactRev:          renderedGetCompactRevSQL,
		Columns:             columns,
		OptionalWhereClause: "",
	})
	if err != nil {
		return nil, err
	}

	renderedListRevisionStartSQL, err := RenderSQLFromTemplate("list-revision-start-SQL", q(listSQL, paramCharacter, numbered), SQLTemplateParameter{
		TableName:           tableName,
		Rev:                 renderedRevisionSQL,
		CompactRev:          renderedGetCompactRevSQL,
		Columns:             columns,
		OptionalWhereClause: "AND mkv.id <= ?",
	})
	if err != nil {
		return nil, err
	}

	renderedGetRevisionAfterSQL, err := RenderSQLFromTemplate("get-revision-after-SQL", q(listSQL, paramCharacter, numbered), SQLTemplateParameter{
		TableName:           tableName,
		Rev:                 renderedRevisionSQL,
		CompactRev:          renderedGetCompactRevSQL,
		Columns:             columns,
		OptionalWhereClause: idOfKey,
	})
	if err != nil {
		return nil, err
	}

	renderedCountSQL, err := RenderSQLFromTemplate("count-SQL", q(countSQL, paramCharacter, numbered), SQLTemplateParameter{
		TableName:           tableName,
		Rev:                 renderedRevisionSQL,
		CompactRev:          renderedGetCompactRevSQL,
		Columns:             columns,
		OptionalWhereClause: "",
	})
	if err != nil {
		return nil, err
	}

	renderedAfterSQL, err := RenderSQLFromTemplate("after-SQL", q(afterSQL, paramCharacter, numbered), SQLTemplateParameter{
		TableName:  tableName,
		Rev:        renderedRevisionSQL,
		CompactRev: renderedGetCompactRevSQL,
		Columns:    columns,
	})
	if err != nil {
		return nil, err
	}

	renderedDeleteSQL, err := RenderSQLFromTemplate("delete-SQL", q(deleteSQL, paramCharacter, numbered), SQLTemplateParameter{
		TableName: tableName,
	})
	if err != nil {
		return nil, err
	}

	renderedUpdateCompactSQL, err := RenderSQLFromTemplate("update-compact-SQL", q(updateCompactSQL, paramCharacter, numbered), SQLTemplateParameter{
		TableName: tableName,
	})
	if err != nil {
		return nil, err
	}

	renderedInsertLastInsertIDSQL, err := RenderSQLFromTemplate("insert-last-insert-id-SQL", q(insertLastInsertIDSQL, paramCharacter, numbered), SQLTemplateParameter{
		TableName: tableName,
	})
	if err != nil {
		return nil, err
	}

	renderedInsertSQL, err := RenderSQLFromTemplate("insert-SQL", q(insertSQL, paramCharacter, numbered), SQLTemplateParameter{TableName: tableName})
	if err != nil {
		return nil, err
	}

	renderedMigrateCountKineSQL, err := RenderSQLFromTemplate("migrate-count-SQL", migrateCountKineSQL, SQLTemplateParameter{TableName: tableName})
	if err != nil {
		return nil, err
	}

	renderedMigrateDataKineSQL, err := RenderSQLFromTemplate("migrate-data-SQL", migrateDataSQL, SQLTemplateParameter{TableName: tableName})
	if err != nil {
		return nil, err
	}

	return &Generic{
		DB: db,

		RevisionSQL:          renderedRevisionSQL,
		GetRevisionSQL:       renderedGetRevisionSQL,
		GetCurrentSQL:        renderedGetCurrentSQL,
		ListRevisionStartSQL: renderedListRevisionStartSQL,
		GetRevisionAfterSQL:  renderedGetRevisionAfterSQL,
		CountSQL:             renderedCountSQL,
		AfterSQL:             renderedAfterSQL,
		DeleteSQL:            renderedDeleteSQL,

		UpdateCompactSQL: renderedUpdateCompactSQL,
		GetCompactRevSQL: renderedGetCompactRevSQL,

		InsertLastInsertIDSQL: renderedInsertLastInsertIDSQL,
		InsertSQL:             renderedInsertSQL,

		MigrateCountKineSQL: renderedMigrateCountKineSQL,
		MigrateDataKineSQL:  renderedMigrateDataKineSQL,
	}, nil
}

func (d *Generic) query(ctx context.Context, sql string, args ...interface{}) (*sql.Rows, error) {
	logrus.Tracef("QUERY %v : %s", args, Stripped(sql))
	return d.DB.QueryContext(ctx, sql, args...)
}

func (d *Generic) queryRow(ctx context.Context, sql string, args ...interface{}) *sql.Row {
	logrus.Tracef("QUERY ROW %v : %s", args, Stripped(sql))
	return d.DB.QueryRowContext(ctx, sql, args...)
}

func (d *Generic) execute(ctx context.Context, sql string, args ...interface{}) (sql.Result, error) {
	logrus.Tracef("EXEC %v : %s", args, Stripped(sql))
	return d.DB.ExecContext(ctx, sql, args...)
}

func (d *Generic) GetCompactRevision(ctx context.Context) (int64, error) {
	var id int64
	row := d.queryRow(ctx, d.GetCompactRevSQL)
	err := row.Scan(&id)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return id, err
}

func (d *Generic) SetCompactRevision(ctx context.Context, revision int64) error {
	result, err := d.execute(ctx, d.UpdateCompactSQL, revision)
	if err != nil {
		return err
	}
	num, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if num != 0 {
		return nil
	}
	_, err = d.Insert(ctx, "compact_rev_key", false, false, 0, revision, 0, []byte(""), nil)
	return err
}

func (d *Generic) GetRevision(ctx context.Context, revision int64) (*sql.Rows, error) {
	return d.query(ctx, d.GetRevisionSQL, revision)
}

func (d *Generic) DeleteRevision(ctx context.Context, revision int64) error {
	_, err := d.execute(ctx, d.DeleteSQL, revision)
	return err
}

func (d *Generic) ListCurrent(ctx context.Context, prefix string, limit int64, includeDeleted bool) (*sql.Rows, error) {
	sql := d.GetCurrentSQL
	if limit > 0 {
		sql = fmt.Sprintf("%s LIMIT %d", sql, limit)
	}
	return d.query(ctx, sql, prefix, includeDeleted)
}

func (d *Generic) List(ctx context.Context, prefix, startKey string, limit, revision int64, includeDeleted bool) (*sql.Rows, error) {
	if startKey == "" {
		sql := d.ListRevisionStartSQL
		if limit > 0 {
			sql = fmt.Sprintf("%s LIMIT %d", sql, limit)
		}
		return d.query(ctx, sql, prefix, revision, includeDeleted)
	}

	sql := d.GetRevisionAfterSQL
	if limit > 0 {
		sql = fmt.Sprintf("%s LIMIT %d", sql, limit)
	}
	return d.query(ctx, sql, prefix, revision, startKey, revision, includeDeleted)
}

func (d *Generic) Count(ctx context.Context, prefix string) (int64, int64, error) {
	var (
		rev sql.NullInt64
		id  int64
	)

	row := d.queryRow(ctx, d.CountSQL, prefix, false)
	err := row.Scan(&rev, &id)
	return rev.Int64, id, err
}

func (d *Generic) CurrentRevision(ctx context.Context) (int64, error) {
	var id int64
	row := d.queryRow(ctx, d.RevisionSQL)
	err := row.Scan(&id)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return id, err
}

func (d *Generic) After(ctx context.Context, prefix string, rev int64) (*sql.Rows, error) {
	sql := d.AfterSQL
	return d.query(ctx, sql, prefix, rev)
}

func (d *Generic) Insert(ctx context.Context, key string, create, delete bool, createRevision, previousRevision int64, ttl int64, value, prevValue []byte) (id int64, err error) {
	cVal := 0
	dVal := 0
	if create {
		cVal = 1
	}
	if delete {
		dVal = 1
	}

	if d.LastInsertID {
		row, err := d.execute(ctx, d.InsertLastInsertIDSQL, key, cVal, dVal, createRevision, previousRevision, ttl, value, prevValue)
		if err != nil {
			return 00, err
		}
		return row.LastInsertId()
	}

	row := d.queryRow(ctx, d.InsertSQL, key, cVal, dVal, createRevision, previousRevision, ttl, value, prevValue)
	err = row.Scan(&id)
	return id, err
}

type SQLTemplateParameter struct {
	TableName           string
	Rev                 string
	CompactRev          string
	Columns             string
	OptionalWhereClause string
}

type SQLSchemaTemplateParameter struct {
	TableName string
}

func MustParseTemplate(templateName, templateBody string) *template.Template {
	t, err := template.New(templateName).Parse(templateBody)
	if err != nil {
		panic(err)
	}
	return t
}

func RenderSQLFromTemplate(templateName string, templateBody string, parameter SQLTemplateParameter) (string, error) {
	sqlStatement := &bytes.Buffer{}
	if err := MustParseTemplate(templateName, templateBody).Execute(
		sqlStatement,
		parameter,
	); err != nil {
		return "", fmt.Errorf("failed rendering SQL statement %s: %v", templateName, err)
	}
	return sqlStatement.String(), nil
}

func RenderSchemaSQLFromTemplate(templateName string, templateBody string, parameter SQLSchemaTemplateParameter) (string, error) {
	sqlStatement := &bytes.Buffer{}
	if err := MustParseTemplate(templateName, templateBody).Execute(
		sqlStatement,
		parameter,
	); err != nil {
		return "", fmt.Errorf("failed rendering SQL statement %s: %v", templateName, err)
	}
	return sqlStatement.String(), nil
}
