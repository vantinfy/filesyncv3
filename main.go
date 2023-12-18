package main

import (
	"filesyncv3/api"
	"filesyncv3/app"
	"filesyncv3/types"
	"github.com/rjeczalik/notify"
	"log"
	"runtime"
)

func main() {
	types.InitDB()
	syncClient := app.NewFileSyncClient()
	types.SetMainCtx(syncClient.Ctx)
	beforeExit := func() {
		// 退出前终止listen协程与关闭数据库连接
		syncClient.Cancel()
		types.VersionWriteWG.Wait() // 阻塞 等待所有的redis key写完db

		types.CloseDb()
		log.Println("sync service exit")
	}
	api.Init(beforeExit)

	go syncClient.HeartBeat()               // 定期维护本节点的在线状态
	go syncClient.Cmd()                     // 简易命令
	go syncClient.ListenAsk(syncClient.Ctx) // 负责响应其它节点追赶文件的请求

	// 启动时候先同步追赶文件到最新版本 之后再加入同步网络
	done := make(chan string, 10)
	log.Println("synchronize chasing...")
	syncClient.Ask(done)

	doneInfo := <-done // 在Ask（主要是Ask里面的协程）执行完前阻塞
	if doneInfo != "success" {
		log.Println(doneInfo) // 未追赶文件的情况下启动程序没有意义
		beforeExit()
		return // 使用return或os.Exit 会绕过signal.Notify 导致无法捕获到终止信号
	}

	eventChan := make(chan notify.EventInfo, 1e2)
	// 注意监听的目标是文件夹 不是具体文件
	// 另外如果有需要监听 home\A home\C 而不需要home\B目录的话 这种需求需要修改代码 对AC分别监听（或者监听home 判断B跳过） 目前就不改动了
	// watch路径带`...`表示递归监听子目录
	var watchTypes []notify.Event
	switch runtime.GOOS {
	case "linux":
		// 0x8-InCloseWrite 0x100-InCreate 0x200-InDelete 0x40-MoveFrom 0x80-MoveTo
		watchTypes = []notify.Event{notify.Event(0x8), notify.Event(0x100), notify.Event(0x200), notify.Event(0x40), notify.Event(0x80)}
	case "windows":
		// 1-FileNotifyChangeFileName 2-FileNotifyChangeDirName 这两个已经监视文件/目录的增删重命名
		// https://github.com/rjeczalik/notify/issues/10#issuecomment-66179535
		watchTypes = []notify.Event{notify.Event(1), notify.Event(2), notify.Write}
	default:
		log.Printf("unsupported OS type:[%v]\n", runtime.GOOS)
		beforeExit()
		return
	}
	if err := notify.Watch(syncClient.WatchPath, eventChan, watchTypes...); err != nil {
		log.Println(err)
		beforeExit()
		return
	}
	defer notify.Stop(eventChan)

	// 监听文件变化的协程
	go syncClient.Watch(eventChan)
	log.Println("service started")

	pubCh := syncClient.Subscribe(types.FileChange)
	defer pubCh.Close()
	for {
		msg, err := pubCh.ReceiveMessage()
		if err != nil {
			log.Println("receive msg err", err)
			continue
		}
		// 同步其它节点变化的文件（夹）
		syncClient.Synchronize(msg)
	}
}
