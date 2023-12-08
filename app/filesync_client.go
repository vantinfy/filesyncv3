package app

import (
	"context"
	"encoding/json"
	"filesyncv3/types"
	"fmt"
	"github.com/go-redis/redis"
	"github.com/rjeczalik/notify"
	"log"
	"os"
	"path/filepath"
	"time"
)

type FileSyncClient struct {
	*types.RedisClient
	*types.SyncConfig
	types.NodeInfo
}

var fileSyncClient *FileSyncClient

func NewFileSyncClient() *FileSyncClient {
	if fileSyncClient == nil {
		syncConfig := types.GetConfig()

		redisClient := types.NewRedisClient(types.WithRedisAddr(syncConfig.RedisAddr),
			types.WithRedisPwd(syncConfig.RedisPwd),
			types.WithRedisDB(syncConfig.RedisDBIndex))
		fileSyncClient = &FileSyncClient{
			RedisClient: redisClient,
			SyncConfig:  syncConfig,
			NodeInfo:    types.NodeInfo{},
		}
		_ = fileSyncClient.LoadPrivateKey()
	}
	return fileSyncClient
}

func (fsc *FileSyncClient) ListenAsk(ctx context.Context) {
	pubCh := fsc.Subscribe(types.VersionCompare)
	defer pubCh.Close()

	for {
		select {
		case <-ctx.Done():
			return
		default:
			msg, err := pubCh.ReceiveMessage()
			if err != nil {
				log.Println("listen ask: receive msg failed", err)
				continue
			}
			ak := ask{}
			_ = json.Unmarshal([]byte(msg.Payload), &ak)
			// 不是向自己ask的msg 忽略
			if ak.NodeId != fsc.UniqueId {
				continue
			}

			// 发送文件
			content, err := os.ReadFile(filepath.Join(fsc.PathPrefix, ak.FilePath))
			if err != nil {
				log.Println("listen ask: read file failed", err)
				continue
			}
			ans := answer{
				FileChanges: types.FileChanges{
					FilePath:     ak.FilePath,
					PublishFrom:  fsc.UniqueId,
					AfterContent: content,
				}, SendTo: ak.AskFrom}
			ansBytes, _ := json.Marshal(ans)
			fsc.Publish(types.ChasingFile, ansBytes)
		}
	}
}

type ask struct {
	types.VersionInfo
	AskFrom string `json:"ask_from"`
}

type answer struct {
	types.FileChanges
	SendTo string `json:"send_to"`
}

// Ask 启动的时候 通过redis与数据库查询所有文件版本 比对本地cache 如果本地version落后则向其它节点请求文件
func (fsc *FileSyncClient) Ask() {
	ctx, cancel := context.WithCancel(context.Background())
	dbVersions, err := types.AllVersionInfo()
	if err != nil {
		log.Println("db get all version info failed", err)
		cancel()
		return
	}

	go func() {
		pubCh := fsc.Subscribe(types.ChasingFile)
		defer pubCh.Close()

		for {
			select {
			case <-ctx.Done():
				return
			default:
				msg, err := pubCh.ReceiveMessage()
				if err != nil {
					log.Println("ask chasing file failed", err)
					continue
				}
				ans := answer{}
				_ = json.Unmarshal([]byte(msg.Payload), &ans)
				// 不是发送给自己的文件 忽略
				if ans.SendTo != fsc.UniqueId {
					continue
				}

				//log.Println("got answer file", ans.FilePath)
				err = os.WriteFile(filepath.Join(fsc.PathPrefix, ans.FilePath), ans.AfterContent, 0644)
				if err != nil {
					log.Println("ask chasing file failed", err)
				}
			}
		}
	}()

	// todo 后续还需要考虑path是目录的情况
	redisVersions := fsc.RedisClient.ScanKeys()
	for _, dbV := range dbVersions {
		// db与redis都有的filepath 以redis为准
		if _, ok := redisVersions[dbV.FilePath]; ok {
			continue
		} else {
			redisVersions[dbV.FilePath] = dbV
		}
	}
	//log.Println("try to chasing", redisVersions)
	for _, info := range redisVersions {
		cacheVersion, ok := types.GetCacheVersion(info.FilePath)
		if !ok || cacheVersion < info.Version {
			// publish 向目标节点请求下载文件
			ak := ask{VersionInfo: types.VersionInfo{
				FilePath: info.FilePath, // ask文件路径
				NodeId:   info.NodeId,   // 向此节点ask文件数据
			}, AskFrom: fsc.UniqueId}
			askBytes, _ := json.Marshal(ak)
			fsc.Publish(types.VersionCompare, askBytes)
		}
	}
	cancel()
}

func (fsc *FileSyncClient) Watch(eventChan chan notify.EventInfo) {
	for {
		ei := <-eventChan
		fmt.Println("wait...", ei)
		// .sync_cache和.key文件不同步
		if fsc.SameFile(ei.Path(), types.SyncCachePath) || fsc.SameFile(ei.Path(), types.KeyPath) {
			continue
		}

		switch ei.Event() {
		case notify.Write:
			fileInfo, err := os.Stat(ei.Path())
			if err != nil {
				continue
			}

			// write的情况下 变化的一定是文件 不是目录
			afterContent, err := os.ReadFile(ei.Path())
			if err != nil {
				continue
			}
			relPath := fsc.CalculateRelativePath(ei.Path())
			fmt.Println("after content", string(afterContent), relPath)

			version, ok := fsc.CheckVersion(relPath)
			if !ok {
				continue
			}
			fsc.Publish(types.FileChange, types.NewFileChanges(fsc.CalculateRelativePath(ei.Path()), ei.Event(),
				types.WithFileContent(afterContent),
				types.WithLastUpdate(fileInfo.ModTime().UnixNano()),
				types.WithPublishFrom(fsc.UniqueId),
				types.WithVersion(version)))
		case notify.Create, notify.Remove:
			fileInfo, err := os.Stat(ei.Path())
			if err != nil {
				continue
			}
			fsc.Publish(types.FileChange, types.NewFileChanges(fsc.CalculateRelativePath(ei.Path()), ei.Event(),
				types.WithIsDir(fileInfo.IsDir()),
				types.WithPublishFrom(fsc.UniqueId)))
		case notify.Rename:
			// linux下的重命名 跟windows逻辑不太一样 前者是rename+create 后者是double rename
			//if runtime.GOOS == "linux" {
			//	continue
			//}
			fmt.Println("", ei) // 重命名的情况下 会走两次 todo
			fsc.Publish(types.FileChange, types.NewFileChanges(fsc.CalculateRelativePath(ei.Path()), ei.Event(), types.WithPublishFrom(fsc.UniqueId)))
		}
	}
}

// CalculateRelativePath 计算变化的文件（夹）相对路径
func (fsc *FileSyncClient) CalculateRelativePath(changePath string) string {
	rel, _ := filepath.Rel(fsc.PathPrefix, changePath)
	return rel
}

func (fsc *FileSyncClient) SameFile(path1, path2 string) bool {
	absPath1, err := filepath.Abs(path1)
	if err != nil {
		return false
	}
	absPath2, err := filepath.Abs(path2)
	if err != nil {
		return false
	}

	return filepath.Clean(absPath1) == filepath.Clean(absPath2)
}

func (fsc *FileSyncClient) CheckVersion(key string) (int, bool) {
	redisVersion, err := fsc.RedisClient.GetKey(key)
	cacheVersion, ok := types.GetCacheVersion(key)
	if err != nil && !ok {
		// 大概率因为第一次对该key操作 所以redis、cache跟db都没有对应的值
		log.Println("get redis version failed", err)
	}

	if redisVersion == cacheVersion {
		err = fsc.RedisClient.SetKey(key, redisVersion+1, fsc.UniqueId)
		if err != nil {
			log.Println("redis set key failed", key, redisVersion+1, err)
			return 0, false
		}
		return redisVersion + 1, true
	}
	return 0, false
}

// Synchronize 接收到文件（夹）变化时 进行同步
func (fsc *FileSyncClient) Synchronize(message *redis.Message) {
	fc := types.Msg2FileChanges(message)
	fc.FilePath = filepath.Join(fsc.PathPrefix, fc.FilePath) // 相对路径还原绝对路径

	if fc.PublishFrom == fsc.UniqueId {
		// 如果是自己广播的消息 不写文件 只更新version
		types.UpdateCacheVersion(fc.FilePath, fc.FileVersion)
		return
	}
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
			break
		}
		types.UpdateCacheVersion(fc.FilePath, fc.FileVersion)

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
			types.UpdateCacheVersion(changes.FilePath, changes.FileVersion)
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
