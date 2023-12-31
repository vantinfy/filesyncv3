package types

import (
	"encoding/json"
	"github.com/go-redis/redis"
	"github.com/rjeczalik/notify"
)

const (
	FileChange     = "file_changes"    // 用于文件变动时发送变动信息
	VersionCompare = "version_compare" // 用于节点启动时 检查文件版本是否为最新
	ChasingFile    = "chasing_file"    // 专门用于启动时 发送文件的订阅
)

type FileChanges struct {
	FilePath     string       // 变化的文件(或目录)相对路径
	PublishFrom  string       // 标识此消息来自哪个节点
	IsDir        bool         // 变化的是目录名
	ChangeType   notify.Event // 变化类型
	BeforeName   string       // 变化之前的名字 用于Rename的情况处理
	AfterContent []byte       // 变化之后的文件内容
	LastUpdate   int64        // 最后更新时间戳
	FileVersion  int          // 文件版本号 防止反复广播同一个文件
}

type Option func(*FileChanges)

func WithLastUpdate(stamp int64) Option {
	return func(changes *FileChanges) {
		changes.LastUpdate = stamp
	}
}

func WithFileContent(content []byte) Option {
	return func(changes *FileChanges) {
		changes.AfterContent = content
	}
}

func WithIsDir(isDir bool) Option {
	return func(changes *FileChanges) {
		changes.IsDir = isDir
	}
}

func WithBeforeName(beforeName string) Option {
	return func(changes *FileChanges) {
		changes.BeforeName = beforeName
	}
}

func WithPublishFrom(from string) Option {
	return func(changes *FileChanges) {
		changes.PublishFrom = from
	}
}

func WithVersion(version int) Option {
	return func(changes *FileChanges) {
		changes.FileVersion = version
	}
}

func NewFileChanges(filename string, changeType notify.Event, opts ...Option) []byte {
	fc := FileChanges{
		FilePath:   filename,
		ChangeType: changeType,
	}

	for _, opt := range opts {
		opt(&fc)
	}

	fcb, _ := json.Marshal(fc)
	return fcb
}

func Msg2FileChanges(msg *redis.Message) FileChanges {
	fc := FileChanges{}
	_ = json.Unmarshal([]byte(msg.Payload), &fc)

	return fc
}
