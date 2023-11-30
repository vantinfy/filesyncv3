package types

import (
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
	WatchPath        string `comment:"监听同步的路径" json:"watch_path"`
	PathPrefix       string `comment:"文件（夹）变化的路径前缀" json:"path_prefix"`
	Redis2DBInterval int64  `comment:"同步文件数据写到db间隔 单位毫秒" json:"redis_2_db_interval"`
	RedisDBIndex     int    `comment:"根据index使用redis的指定db" json:"redis_db_index"`
}

func (sc *SyncConfig) LoadConfig() {
	sc.PathPrefix = strings.TrimSuffix(sc.WatchPath, "...")

	pathInfo, err := os.Stat(sc.PathPrefix)
	if err != nil {
		// 监听的路径不存在就创建
		os.MkdirAll(sc.PathPrefix, 0644)
	}
	if !pathInfo.IsDir() {
		log.Println("同步文件夹错误")
	}
}

// todo 建表
