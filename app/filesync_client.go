//go:build filesyncv3
// +build filesyncv3

package app

import (
	"encoding/json"
	"filesyncv3/types"
	"fmt"
	"github.com/go-redis/redis"
	"github.com/rjeczalik/notify"
	"os"
	"path/filepath"
	"time"
)

type FileSyncClient struct {
	*redis.Client
	types.SyncConfig
}

var fileSyncClient *FileSyncClient

func NewFileSyncClient() *FileSyncClient {
	if fileSyncClient == nil {
		o := redis.Options{
			Addr:     "192.168.10.117:6379",
			Password: "",
			DB:       7,
		}
		fileSyncClient = &FileSyncClient{
			Client:     redis.NewClient(&o),
			SyncConfig: types.SyncConfig{},
		}
		fileSyncClient.LoadConfig()
	}
	return fileSyncClient
}

func (fsc *FileSyncClient) Watch(eventChan chan notify.EventInfo) {
	for {
		ei := <-eventChan
		fmt.Println("wait...", ei)
		switch ei.Event() {
		case notify.Write:
			fileInfo, err := os.Stat(ei.Path())
			if err != nil {
				continue
			}

			afterContent, err := os.ReadFile(ei.Path())
			if err != nil {
				continue
			}
			fmt.Println("after content", string(afterContent), ei.Path())
			fsc.Publish(types.FileChange, types.NewFileChanges(fsc.CalculateRelativePath(ei.Path()), ei.Event(), types.WithFileContent(afterContent), types.WithLastUpdate(fileInfo.ModTime().UnixNano())))
		case notify.Create, notify.Remove:
			fileInfo, err := os.Stat(ei.Path())
			if err != nil {
				continue
			}
			fsc.Publish(types.FileChange, types.NewFileChanges(fsc.CalculateRelativePath(ei.Path()), ei.Event(), types.WithIsDir(fileInfo.IsDir())))
		case notify.Rename:
			// linux下的重命名 跟windows逻辑不太一样 前者是rename+create 后者是double rename
			//if runtime.GOOS == "linux" {
			//	continue
			//}
			fmt.Println("", ei) // 重命名的情况下 会走两次 todo
			fsc.Publish(types.FileChange, types.NewFileChanges(fsc.CalculateRelativePath(ei.Path()), ei.Event()))
		}
	}
}

// CalculateRelativePath 计算变化的文件（夹）相对路径
func (fsc *FileSyncClient) CalculateRelativePath(changePath string) string {
	rel, _ := filepath.Rel(fsc.PathPrefix, changePath)
	return rel
}

// Synchronize 接收到文件（夹）变化时 进行同步
func (fsc *FileSyncClient) Synchronize(message *redis.Message) {
	fc := types.FileChanges{}
	_ = json.Unmarshal([]byte(message.Payload), &fc)
	fc.FilePath = filepath.Join(fsc.PathPrefix, fc.FilePath) // 相对路径还原绝对路径
	//fmt.Println("got file changes", fc)
	//return

	// todo 直接粘贴非空文件的时候 write+create（顺序不固定）
	switch fc.ChangeType {
	case notify.Write:
		err := os.WriteFile(fc.FilePath, fc.AfterContent, 0644)
		if err != nil {
			fmt.Println("write change content failed", err)
			// 协程定期尝试重试更新
			go fsc.TryWriteFile(fc)
		}

	case notify.Create:
		var err error
		if fc.IsDir { // 创建的是文件夹
			err = os.Mkdir(fc.FilePath, 0644)
		} else { // 新建文件
			err = os.WriteFile(fc.FilePath, nil, 0644)
		}
		if err != nil {
			fmt.Println("create file or dir failed", err)
		}

	case notify.Remove:
		// 使用RemoveAll 可以删除非空文件夹或文件
		err := os.RemoveAll(fc.FilePath)
		if err != nil {
			fmt.Println("remove file failed", err)
			go fsc.TryRemove(fc)
		}

	case notify.Rename:
		// todo
	}
}

func (fsc *FileSyncClient) TryWriteFile(changes types.FileChanges) {
	ticker := time.NewTicker(time.Second * 3)
	defer ticker.Stop()

	for {
		<-ticker.C
		// 例如文件最初内容是a 当更改为b时失败了 开启这个协程定期尝试更新文件内容为b 如果这个过程中文件被更改为了c lastUpdate也会被更新 此时放弃更新文件内容为b
		if changes.LastUpdate < time.Now().UnixNano() {
			return
		}

		err := os.WriteFile(changes.FilePath, changes.AfterContent, 0644)
		if err != nil {
			fmt.Printf("retry update file[%v] failed: [%v]", changes.FilePath, err)
		} else {
			return
		}
	}
}

func (fsc *FileSyncClient) TryRemove(changes types.FileChanges) {
	ticker := time.NewTicker(time.Second * 3)
	defer ticker.Stop()

	for {
		<-ticker.C

		err := os.RemoveAll(changes.FilePath)
		if err != nil {
			fmt.Printf("retry remove [%v] failed: [%v]", changes.FilePath, err)
		} else {
			return
		}
	}
}
