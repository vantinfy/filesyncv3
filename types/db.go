package types

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"log"
	"strings"
)

type DBConn struct {
	*sqlx.DB
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
			DB: sqlx.NewDb(db, cfg.DBType),
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
	// id INT AUTO_INCREMENT PRIMARY KEY,
	createSql := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS`+` %s (
    filepath VARCHAR(255) PRIMARY KEY,
    version INT NOT NULL,
	flag INT DEFAULT 0,
    node_id VARCHAR(255) NOT NULL,
    update_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);`, tableName)

	_, err := dbConn.Exec(createSql)
	if err != nil {
		log.Println("create table", tableName, err)
	}
}

type VersionInfo struct {
	FilePath string `json:"file_path,omitempty" db:"filepath"`
	Version  int    `json:"version,omitempty" db:"version"`
	Flag     int    `json:"flag,omitempty" db:"flag"` // 二进制位 个位数表示删除位 十位数表示是否为目录
	NodeId   string `json:"node_id,omitempty" db:"node_id"`
	UpdateAt string `json:"update_at,omitempty" db:"update_at"` // 实际上对应数据库字段类型是时间
}

func (v *VersionInfo) TableName() string {
	return GetConfig().DBConfig.TableName
}

func (v *VersionInfo) MarshalBinary() ([]byte, error) {
	return json.Marshal(v)
}

func (v *VersionInfo) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, &v)
}

const (
	flagDel = 0b1      // 删除标志位
	flagDir = 0b1 << 1 // 目录标志位
)

func (v *VersionInfo) IsDir() bool {
	return v.Flag&flagDir != 0
}

func (v *VersionInfo) IsDel() bool {
	return v.Flag&flagDel != 0
}

func UpdateDBVersion(path string, vi VersionInfo) (sql.Result, error) {
	updateSql := fmt.Sprintf(`INSERT INTO %s`+` (filepath, version, flag, node_id)
VALUES (?, ?, ?, ?)
ON DUPLICATE KEY UPDATE
version = VALUES(version),
flag = VALUES(flag),
node_id = VALUES(node_id),
update_at = VALUES(update_at);
`, GetConfig().TableName)
	return dbConn.Exec(updateSql, path, vi.Version, vi.Flag, vi.NodeId)
}

func QueryDBVersion(path string) (VersionInfo, bool) {
	querySql := fmt.Sprintf(`select version, node_id from %s where filepath = ?`, GetConfig().TableName)

	vi := VersionInfo{}
	err := dbConn.Get(&vi, querySql, path)
	if err != nil {
		return vi, false
	}
	return vi, true
}

func AllVersionInfo() ([]VersionInfo, error) {
	querySql := fmt.Sprintf(`select filepath, version, flag, node_id from %s`, GetConfig().TableName)

	vis := make([]VersionInfo, 0)
	return vis, dbConn.Select(&vis, querySql)
}

func Exec(sql string) (sql.Result, error) {
	// dbx.Exec("INSERT INTO your_table (name) VALUES (?)", "John Doe")
	return dbConn.Exec(sql)
}

// Get get通常查询单行数据
func Get(dest any, sql string, args ...any) error {
	// dbx.Get(&name, "SELECT id, name FROM your_table WHERE id = ?", 1)
	return dbConn.Get(dest, sql, args)
}
