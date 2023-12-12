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
	"sync"
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
		case msg := <-pubCh.Channel():
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
func (fsc *FileSyncClient) Ask(done chan struct{}) {
	ctx, cancel := context.WithCancel(context.Background())
	dbVersions, err := types.AllVersionInfo()
	if err != nil {
		log.Println("ask chasing: db get all version info failed", err)
		cancel()
		close(done)
		return
	}

	// wg用于确保追赶的每一个文件都写入到本地
	wg := sync.WaitGroup{}
	go func() {
		// 确保这个协程跑完再关闭管道
		defer close(done)
		pubCh := fsc.Subscribe(types.ChasingFile)
		defer pubCh.Close()

		for {
			select {
			case <-ctx.Done():
				return
			case msg := <-pubCh.Channel():
				// 这里改为了Channel 之前是default+ReceiveMessage 可能会阻塞ctx.Done的case
				ans := answer{}
				_ = json.Unmarshal([]byte(msg.Payload), &ans)
				// 不是发送给自己的文件 忽略
				if ans.SendTo != fsc.UniqueId {
					continue
				}

				//log.Println("got answer file", ans.FilePath)
				dir, _ := filepath.Split(filepath.Join(fsc.PathPrefix, ans.FilePath))
				_ = os.MkdirAll(dir, 0644)

				err = os.WriteFile(filepath.Join(fsc.PathPrefix, ans.FilePath), ans.AfterContent, 0644)
				if err != nil {
					log.Println("ask chasing file failed", err)
				}
				// 每下载完一个文件 wait-1
				wg.Done()
			}
		}
	}()

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

	//考虑了path是目录以及删除文件（夹）的情况
	for _, info := range redisVersions {
		// 如果该路径已经被标记删除 这里同样执行删除即可
		if info.IsDel() {
			err = os.RemoveAll(info.FilePath)
			if err != nil {
				log.Printf("ask chasing file, remove[%v] failed[%v]\n", info.FilePath, err)
			}
			continue
		}
		// 是目录的话 直接创建 不需要向其它节点请求下载
		if info.IsDir() {
			err = os.MkdirAll(info.FilePath, 0644)
			if err != nil {
				log.Printf("ask chasing file, mkdirAll[%v] failed[%v]\n", info.FilePath, err)
			}
			continue
		}
		cacheVersion, ok := types.GetCacheVersion(info.FilePath)
		// 如果目标节点是自己就不用下载了
		if (!ok || cacheVersion < info.Version) && !info.IsDir() && info.NodeId != fsc.UniqueId {
			// publish 向目标节点请求下载文件
			ak := ask{VersionInfo: types.VersionInfo{
				FilePath: info.FilePath, // ask文件路径
				NodeId:   info.NodeId,   // 向此节点ask文件数据
			}, AskFrom: fsc.UniqueId}
			askBytes, _ := json.Marshal(ak)

			//log.Println("ask file", ak.FilePath, ak.NodeId)

			// 每下载一个文件请求 wait+1
			wg.Add(1)
			fsc.Publish(types.VersionCompare, askBytes)
		}
	}

	// 等待每一个文件都下载完成再终止子协程
	wg.Wait()
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
			//fmt.Println("after content", string(afterContent), relPath)

			version, ok := fsc.CheckVersion(relPath)
			if !ok {
				continue
			}
			fsc.Publish(types.FileChange, types.NewFileChanges(relPath, ei.Event(),
				types.WithFileContent(afterContent),
				types.WithLastUpdate(fileInfo.ModTime().UnixNano()),
				types.WithPublishFrom(fsc.UniqueId),
				types.WithVersion(version)))
		case notify.Create:
			relPath := fsc.CalculateRelativePath(ei.Path())
			fileInfo, err := os.Stat(ei.Path())
			if err != nil {
				continue
			}
			version, ok := fsc.CheckVersion(relPath, types.SetWithDir(fileInfo.IsDir()))
			if !ok {
				continue
			}
			fsc.Publish(types.FileChange, types.NewFileChanges(relPath, ei.Event(),
				types.WithIsDir(fileInfo.IsDir()),
				types.WithPublishFrom(fsc.UniqueId),
				types.WithVersion(version)))
		case notify.Remove:
			relPath := fsc.CalculateRelativePath(ei.Path())
			version, ok := fsc.CheckVersion(relPath, types.SetWithDel(true))
			if !ok {
				continue
			}
			fsc.Publish(types.FileChange, types.NewFileChanges(relPath, ei.Event(),
				types.WithPublishFrom(fsc.UniqueId),
				types.WithVersion(version)))
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
	// 统一使用"/"作为路径分格符 否则filepath.Split在linux操作系统上可能无法正确解析"a\b\c\d.txt"这种路径
	return filepath.ToSlash(rel)
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

func (fsc *FileSyncClient) CheckVersion(key string, options ...types.SetOption) (int, bool) {
	redisVersion, err := fsc.RedisClient.GetKey(key)
	cacheVersion, ok := types.GetCacheVersion(key)
	if err != nil && !ok {
		// 大概率因为第一次对该key操作 所以redis、cache跟db都没有对应的值
		log.Println("get redis version failed", err)
	}

	if redisVersion == cacheVersion {
		err = fsc.RedisClient.SetKey(key, redisVersion+1, fsc.UniqueId, options...)
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
		// 防止后面因为路径包含本地尚未存在的目录导致写入失败 即使目录已经存在MkdirAll也不会报错
		dir, _ := filepath.Split(fc.FilePath)
		_ = os.MkdirAll(dir, 0644)

		err := os.WriteFile(fc.FilePath, fc.AfterContent, 0644)
		if err != nil {
			log.Println("write change content failed", err)
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
			log.Println("create file or dir failed", err)
		}

	case notify.Remove:
		// 使用RemoveAll 可以删除非空文件夹或文件
		err := os.RemoveAll(fc.FilePath)
		if err != nil {
			log.Println("remove file failed", err)
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
			log.Printf("retry update file[%v] failed: [%v]\n", changes.FilePath, err)
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
			log.Printf("retry remove [%v] failed: [%v]\n", changes.FilePath, err)
		} else {
			return
		}
	}
}
