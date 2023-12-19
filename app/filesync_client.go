package app

import (
	"context"
	"filesyncv3/types"
	"fmt"
	"github.com/go-redis/redis"
	"github.com/rjeczalik/notify"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type FileSyncClient struct {
	*types.RedisClient
	*types.SyncConfig
	types.NodeInfo
	Ctx    context.Context    // 主ctx
	Cancel context.CancelFunc // 正常退出的时候会调用这个Cancel方法
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

		// 全局上下文
		fileSyncClient.Ctx, fileSyncClient.Cancel = context.WithCancel(context.Background())
	}
	return fileSyncClient
}

func readFile(path string) ([]byte, time.Duration, error) {
	// windows.ERROR_SHARING_VIOLATION 也就是 syscall.Errno(0x20)
	const windowsErrSharingViolation = "The process cannot access the file because it is being used by another process."
	//retryCnt := 10 // 只尝试十次的话 大概1秒 测试复制一个348MB的视频不够
	start := time.Now()

retry:
	fBytes, err := os.ReadFile(path)
	if err != nil {
		// 这里不能直接用errors.Is(err, windows.ERROR_SHARING_VIOLATION) 因为windows包在linux上编译不通过
		// 直接用syscall.Errno(0x20)理论可行 但是linux上的0x20错误是"broken pipe"
		// 所以最后还是采用字符串判断的方式
		if strings.Contains(err.Error(), windowsErrSharingViolation) /* && retryCnt > 0 */ {
			time.Sleep(time.Millisecond * 100)
			//retryCnt--
			goto retry // 对于windows上被占用的正在写入的文件 无限重试 其它错误则返回
		} else {
			return fBytes, time.Since(start) * 2, err
		}
	}

	// 即使成功读取文件 还无法确定读取到的文件是否完整（linux系统支持一个进程写文件的过程中 另一个进程读取该文件）
	// 这个问题已经通过将linux上的监听信号从write细化为InCloseWrite解决
	return fBytes, time.Since(start) * 2, nil
}

func (fsc *FileSyncClient) setAfterContent(relPath string, content []byte, since time.Duration) []byte {
	// 将文件分片存储为多个key 每个key-value暂定1024*1024字节 这里不考虑rdm（超过1024*200就崩溃了）
	chunkSize := 1024 * 1024 // 1MB
	totalChunk := len(content) / chunkSize
	if len(content)%chunkSize != 0 {
		totalChunk++ // 如果文件大小不是1024*1024的整数倍，增加一片
	}
	key := fmt.Sprintf("OnRedis_%v_%d_%d", relPath, len(content), chunkSize)

	// 测试使用348MB的文件 for循环set约1s(如果是刷新覆盖则需要10s左右)
	for i := 0; i < totalChunk; i++ {
		startIndex := i * chunkSize
		endIndex := (i + 1) * chunkSize
		if endIndex > len(content) {
			endIndex = len(content)
		}
		fsc.Client.Set(fmt.Sprintf("%v_%d", key, i), content[startIndex:endIndex], time.Second*300+since)
	}
	return []byte(key)
}

func (fsc *FileSyncClient) analyseAfterContent(relPath string, content []byte) ([]byte, error) {
	reg := regexp.MustCompile(fmt.Sprintf(`OnRedis_%v_(\d+)_(\d+)`, relPath))
	match := reg.FindStringSubmatch(string(content))
	if len(match) > 0 {
		length, _ := strconv.Atoi(match[1])
		eachLength, _ := strconv.Atoi(match[2])
		totalChunk := length / eachLength
		if length%eachLength != 0 {
			totalChunk++
		}

		// 从redis逐片读取组装数据内容 这里预先指定了容量 避免后续append多次扩容导致效率问题
		assembledContent := make([]byte, 0, length)
		for i := 0; i < totalChunk; i++ {
			// 测试使用的348MB数据 for下来一共要1-2min才能取得完整数据
			// 如果后续问题依然明显 这里也可以改用协程来get 大概每10个i一组协程
			iBytes, err := fsc.Client.Get(fmt.Sprintf("%v_%d", string(content), i)).Bytes()
			if err != nil {
				return nil, err
			}
			assembledContent = append(assembledContent, iBytes...)
		}
		return assembledContent, nil
	}
	return content, nil
}

func (fsc *FileSyncClient) Watch(eventChan chan notify.EventInfo) {
	for {
		ei := <-eventChan
		//log.Printf("wait...%v event sys %#v\n", ei, ei.Sys())

		// .sync_cache和.key文件不同步
		if fsc.SameFile(ei.Path(), types.SyncCachePath) || fsc.SameFile(ei.Path(), types.KeyPath) {
			continue
		}

		switch ei.Event() {
		/* ------------------------------ Write --------------------------------- */
		case notify.Event(0x8):
			// linux inotify信号 InCloseWrite
			// 区分linux的write是因为 cp或者mv一个大文件的时候 linux上会分为多次写入
			// 此时要判断最后一次写入文件比较复杂 直接监听InCloseWrite信号方便很多
			fallthrough
		case notify.Write:
			fileInfo, err := os.Stat(ei.Path())
			if err != nil {
				continue
			}

			// write的情况下 变化的一定是文件 不是目录 且注意redis的publish/subscribe机制不适用大数据传输
			// windows中 直接粘贴大文件的情况下可能该文件被占用导致报错: The process cannot access the file because it is being used by another process.
			// 而在linux中 操作系统允许在写一个文件的时候让另一个文件读取 这个情况下读取的文件可能是不完整的
			afterContent, since, err := readFile(ei.Path())
			if err != nil {
				log.Println("watch write signal: read file failed", ei.Path(), err)
				continue
			}
			relPath := fsc.CalculateRelativePath(ei.Path())
			//fmt.Println("after content len", len(afterContent), relPath)

			version, ok := fsc.CheckVersion(relPath)
			if !ok {
				continue
			}
			// 大于8MB的数据不通过pub/sub发布 理论上最好是分片文件 发布变化的文件块 但是实现难度大
			// 这里使用redis set将文件临时存放到redis（5min+） 这个方案极限情况下如果短时间多个大文件变化 可能会导致redis占用内存过大导致崩溃
			// 另一个方案思路：afterContent提供一个下载链接，其它节点直接通过连接向发生变化的节点下载文件 但是同样要考虑一些特殊情况，比如文件下载过程中文件重新变化这些问题
			if len(afterContent) > 8*1024*1024 {
				afterContent = fsc.setAfterContent(relPath, afterContent, since)
			}
			fsc.Publish(types.FileChange, types.NewFileChanges(relPath, notify.Write,
				types.WithFileContent(afterContent),
				types.WithLastUpdate(fileInfo.ModTime().UnixNano()),
				types.WithPublishFrom(fsc.UniqueId),
				types.WithVersion(version)))

		/* ------------------------------ Create --------------------------------- */
		case notify.Event(0x100): // linux InCreate
			fallthrough
		case notify.Event(1) << 12:
			// windows Added
			// 这里还有一个小问题：windows上通过剪切粘贴（或者cmd的move命令），来移动文件的时候
			// 如果前后文件在一个目录下（也就是重命名） 是可以正确识别为rename的
			// 但是如果前后不在一个目录下（也就是真正的移动文件） windows信号是Added和Removed
			fallthrough
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
			fsc.Publish(types.FileChange, types.NewFileChanges(relPath, notify.Create,
				types.WithIsDir(fileInfo.IsDir()),
				types.WithPublishFrom(fsc.UniqueId),
				types.WithVersion(version)))

		/* ------------------------------ Delete --------------------------------- */
		case notify.Event(0x200): // linux InDelete
			fallthrough
		case notify.Event(2) << 12: // windows Removed
			fallthrough
		case notify.Remove:
			relPath := fsc.CalculateRelativePath(ei.Path())
			version, ok := fsc.CheckVersion(relPath, types.SetWithDel(true))
			if !ok {
				continue
			}
			fsc.Publish(types.FileChange, types.NewFileChanges(relPath, notify.Remove,
				types.WithPublishFrom(fsc.UniqueId),
				types.WithVersion(version)))

		/* ------------------------------ Rename --------------------------------- */
		case notify.Event(0x40): // linux MoveFrom
			fallthrough
		case notify.Event(4) << 15: // windows RenameOldName
			log.Println("rename before", ei.Path())

		case notify.Event(0x80): // linux MoveTo
			fallthrough
		case notify.Event(5) << 16: // windows RenameNewName
			log.Println("rename after", ei.Path())

		case notify.Rename:
			// linux下的重命名 跟windows逻辑不太一样 前者是rename+create 后者是double rename
			// 如果是目录的重命名差别更大 linux是两个rename+create windows依然double rename
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

	if fc.PublishFrom == fsc.UniqueId {
		// 如果是自己广播的消息 不写文件 只更新version
		types.UpdateCacheVersion(fc.FilePath, fc.FileVersion)
		return
	}
	relPath := fc.FilePath                                   // 相对路径
	fc.FilePath = filepath.Join(fsc.PathPrefix, fc.FilePath) // 相对路径还原绝对路径

	// 直接粘贴非空文件到监视目录下的时候 write+create（顺序不固定）
	// 且难点在于linux中cp、mv大文件（本地测试Ubuntu虚拟机18.04server 20M+的文件）的时候会触发多个write信号 如何判断最后一次write是文件写完
	// 这个问题已经通过监听linux InCloseWrite信号解决
	switch fc.ChangeType {
	case notify.Write:
		analyseContent, err := fsc.analyseAfterContent(relPath, fc.AfterContent)
		if err != nil {
			log.Println("analyse after content failed", err)
			return
		}
		fc.AfterContent = analyseContent

		// 防止后面因为路径包含本地尚未存在的目录导致写入失败 即使目录已经存在MkdirAll也不会报错
		dir, _ := filepath.Split(fc.FilePath)
		_ = os.MkdirAll(dir, 0644)

		err = os.WriteFile(fc.FilePath, fc.AfterContent, 0644)
		if err != nil {
			log.Println("write change content failed", err)
			// 协程定期尝试重试更新
			go fsc.TryWriteFile(fc)
			break
		}
		types.UpdateCacheVersion(fc.FilePath, fc.FileVersion)

	case notify.Create:
		var err error
		if _, err = os.Stat(fc.FilePath); err == nil {
			// 路径已经存在 跳过创建 （因为如果这个情况下create/write文件 会导致原有文件被清空）
			break
		}
		if fc.IsDir { // 创建的是文件夹
			err = os.MkdirAll(fc.FilePath, 0644)
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
