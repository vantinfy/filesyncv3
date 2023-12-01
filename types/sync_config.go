package types

import (
	"github.com/BurntSushi/toml"
	"log"
	"os"
	"strings"
)

/*
	假设 A服务器上需要同步的目录WatchPath是 /home/my_account/works/sync_a/...（`...`表示递归匹配该目录及其所有子目录中的文件夹或子目录）
	在B服务器上是这样的 /home/account_b/soft/workspace/sync_b/...
	在C服务器上是这样的 /root/account_c/workspace/sync_c/...

	当A在sync_a目录下的dir_a(这个目录原先存在)创建了目录dir_sub和文件file_a
	发送给其它节点的信息分别为：create dir_a/dir_sub (isDir=T) 和 create dir_a/file_a (isDir=F)
	BC接收到消息之后只需要将`dir_a/dir_sub`和`dir_a/file_a`分别与自己的RelativePath拼接起来 之后执行对应的创建逻辑即可
*/

// SyncConfig 同步配置信息
type SyncConfig struct {
	PathConfig  `toml:"path_config"`
	RedisConfig `toml:"redis_config"`
	DBConfig    `toml:"db_config"`
}

// PathConfig 路径相关配置
type PathConfig struct {
	WatchPath  string `toml:"watch_path"`  // 监听同步的路径
	PathPrefix string `toml:"path_prefix"` // 文件（夹）变化的路径前缀
}

// RedisConfig redis相关配置
type RedisConfig struct {
	RedisAddr        string `toml:"redis_addr"`          // redis连接地址
	RedisPwd         string `toml:"redis_pwd"`           // redis连接密码
	RedisDBIndex     int    `toml:"redis_db_index"`      // 根据index使用redis的指定db
	Redis2DBInterval int64  `toml:"redis_2_db_interval"` // 同步文件数据写到db间隔 单位秒
}

// DBConfig db相关配置 主要是记录和维护同步文件（夹）的信息
type DBConfig struct {
	// todo 建表
	Enable    bool   `toml:"enable"`     // 是否启用这个配置
	DBType    string `toml:"db_type"`    // db类型
	ConnLink  string `toml:"conn_link"`  // 连接参数
	TableName string `toml:"table_name"` // 表名
}

const ConfigFilePath = "config.toml"

func (sc *SyncConfig) LoadConfig() {
	_, err := toml.DecodeFile(ConfigFilePath, sc)
	if err != nil {
		log.Println("load config failed", err)
		return
	}

	sc.PathPrefix = strings.TrimSuffix(sc.WatchPath, "...")

	pathInfo, err := os.Stat(sc.PathPrefix)
	if err != nil {
		// 监听的路径不存在就创建
		_ = os.MkdirAll(sc.PathPrefix, 0644)
		return
	}
	if !pathInfo.IsDir() {
		log.Println("invalid sync path")
	}
}
