package types

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"log"
	"strings"
)

type DBConn struct {
	*sql.DB
	DBType string
}

var dbConn *DBConn

func InitDB() {
	if dbConn == nil {
		cfg := GetConfig()
		dataSource, err := parseDBLink(cfg.DBConfig.ConnLink)
		if err != nil {
			log.Println(err)
			return
		}

		db, err := sql.Open(cfg.DBConfig.DBType, dataSource)
		if err != nil {
			log.Println(err)
			return
		}

		err = db.Ping()
		if err != nil {
			log.Println(err)
			return
		}
		dbConn = &DBConn{
			DB:     db,
			DBType: cfg.DBType,
		}
		createTable(cfg.DBConfig.TableName)
	}
}

func parseDBLink(link string) (string, error) {
	// root:Huacheng987@tcp(192.168.30.25:3319)/hcuser
	dbConfig := mysql.Config{}
	args := strings.Split(link, "@")
	if len(args) != 2 {
		return "", errors.New("parse db link failed when split '@'")
	}

	uInfo := strings.Split(args[0], ":")
	if len(uInfo) != 2 {
		return "", errors.New("parse db link failed when split ':'")
	}
	dbConfig.User = uInfo[0]
	dbConfig.Passwd = uInfo[1]

	hostInfo := strings.Split(args[1], "(")
	if len(hostInfo) != 2 {
		return "", errors.New("parse db link failed when split '('")
	}
	dbConfig.Net = hostInfo[0]

	connInfo := strings.Split(hostInfo[1], ")/")
	if len(connInfo) != 2 {
		return "", errors.New("parse db link failed when split ')/'")
	}
	dbConfig.Addr = connInfo[0]
	dbConfig.DBName = connInfo[1]

	return dbConfig.FormatDSN(), nil
}

func CloseDb() {
	_ = dbConn.DB.Close()
}

func createTable(tableName string) {
	dbx := sqlx.NewDb(dbConn.DB, dbConn.DBType)
	// id INT AUTO_INCREMENT PRIMARY KEY,
	createSql := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS`+` %s (
    filepath VARCHAR(255) PRIMARY KEY,
    version INT NOT NULL,
    update_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);`, tableName)

	_, err := dbx.Exec(createSql)
	if err != nil {
		log.Println("create table", tableName, err)
	}
}

func UpdateDBVersion(path string, version int) (sql.Result, error) {
	dbx := sqlx.NewDb(dbConn.DB, dbConn.DBType)
	updateSql := fmt.Sprintf(`INSERT INTO %s`+` (filepath, version)
VALUES (?, ?)
ON DUPLICATE KEY UPDATE
version = VALUES(version),
update_at = VALUES(update_at);
`, GetConfig().TableName)
	return dbx.Exec(updateSql, path, version)
}

func QueryDBVersion(path string) (int, bool) {
	dbx := sqlx.NewDb(dbConn.DB, dbConn.DBType)
	querySql := fmt.Sprintf(`select version from %s where filepath = ?`, GetConfig().TableName)

	var version int
	err := dbx.Get(&version, querySql, path)
	if err != nil {
		return 0, false
	}
	return version, true
}

func Exec(sql string) (sql.Result, error) {
	dbx := sqlx.NewDb(dbConn.DB, dbConn.DBType)
	// dbx.Exec("INSERT INTO your_table (name) VALUES (?)", "John Doe")
	return dbx.Exec(sql)
}

// Get get通常查询单行数据
func Get(dest any, sql string, args ...any) error {
	// dbx.Get(&name, "SELECT id, name FROM your_table WHERE id = ?", 1)
	return sqlx.NewDb(dbConn.DB, dbConn.DBType).Get(dest, sql, args)
}
