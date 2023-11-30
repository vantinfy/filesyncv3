//go:build filesyncv3
// +build filesyncv3

package types

import (
	"encoding/json"
	"github.com/rjeczalik/notify"
)

const FileChange = "file_changes"

type FileChanges struct {
	FilePath     string       // 变化的文件(或目录)相对路径
	IsDir        bool         // 变化的是目录名
	ChangeType   notify.Event // 变化类型
	BeforeName   string       // 变化之前的名字 用于Rename的情况处理
	AfterContent []byte       // 变化之后的文件内容
	LastUpdate   int64        // 最后更新时间戳
	//FileVersion  int64        // 文件版本号
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
