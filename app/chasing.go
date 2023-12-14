package app

import (
	"context"
	"encoding/json"
	"filesyncv3/types"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

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
				fsc.Publish(types.ChasingFile, answerWithErr(ak.FilePath, fsc.UniqueId, ak.AskFrom, err))
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

const answerError = "??ERROR??"

func answerWithErr(path, from, to string, err error) []byte {
	ans := answer{
		FileChanges: types.FileChanges{
			FilePath:     path,
			PublishFrom:  from,
			AfterContent: []byte(answerError + err.Error()),
		},
		SendTo: to,
	}

	ansBytes, _ := json.Marshal(ans)
	return ansBytes
}

func (a answer) isErr() (bool, string) {
	if strings.HasPrefix(string(a.AfterContent), answerError) {
		return true, strings.Split(string(a.AfterContent), answerError)[1]
	}
	return false, ""
}

// Ask 启动的时候 通过redis与数据库查询所有文件版本 比对本地cache 如果本地version落后则向其它节点请求文件
func (fsc *FileSyncClient) Ask(done chan string) {
	ctx, cancel := context.WithCancel(context.Background())
	dbVersions, err := types.AllVersionInfo()
	if err != nil {
		log.Println("ask chasing: db get all version info failed", err)
		cancel()
		done <- err.Error()
		return
	}

	nodeIdChan := make(chan string, 10)
	// wg用于确保追赶的每一个文件都写入到本地
	wg := sync.WaitGroup{}
	go func() {
		pubCh := fsc.Subscribe(types.ChasingFile)
		defer pubCh.Close()

		for {
			select {
			case <-ctx.Done():
				// 确保这个协程跑完再向管道写信号
				done <- "success"
				return
			case msg := <-pubCh.Channel():
				// 这里改为了Channel 之前是default+ReceiveMessage 可能会阻塞ctx.Done的case
				ans := answer{}
				_ = json.Unmarshal([]byte(msg.Payload), &ans)
				// 不是发送给自己的文件 忽略
				if ans.SendTo != fsc.UniqueId {
					continue
				}
				if isErr, errStr := ans.isErr(); isErr {
					log.Println("ask chasing file err:", errStr)
					wg.Done()
					continue
				}

				//log.Println("got answer file", ans.FilePath)
				dir, _ := filepath.Split(filepath.Join(fsc.PathPrefix, ans.FilePath))
				_ = os.MkdirAll(dir, 0644)

				err = os.WriteFile(filepath.Join(fsc.PathPrefix, ans.FilePath), ans.AfterContent, 0644)
				if err != nil {
					log.Println("ask chasing file failed", err)
				}

				// 启动的时候可能本地cache被删过 下载完成后才更新cache
				types.UpdateCacheVersion(ans.FilePath, ans.FileVersion)
				// 每下载完一个文件 wait-1
				wg.Done()
			case node := <-nodeIdChan:
				if !fsc.IsOnline(node) {
					// 任意节点不在线 写入错误
					done <- fmt.Sprintf("node[%v] is not online, pls make sure all node is startup.", node)
					wg.Done()
				}
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
			nodeIdChan <- info.NodeId
			wg.Add(1)
			fsc.Publish(types.VersionCompare, askBytes)
		}
	}

	// 等待每一个文件都下载完成再终止子协程
	// 假如请求后目标节点没有在线 会导致阻塞 所以增加了节点online检测机制 每次请求追赶数据的时候会同步判断该节点是否在线 如果不在线则报错
	// 一定要确认追赶完成 因为如果强制结束追赶 假设要追赶的文件a 版本是3 本地版本低于3的情况下开启同步 如果更新了文件a 会出现a内容分叉的情况
	log.Println("wait response...")
	wg.Wait()
	cancel()
}
