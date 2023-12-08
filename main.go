package main

import (
	"context"
	"filesyncv3/api"
	"filesyncv3/app"
	"filesyncv3/types"
	"github.com/rjeczalik/notify"
	"log"
)

func main() {
	types.InitDB()
	ctx, cancel := context.WithCancel(context.Background())
	api.Init(func() {
		// 退出前终止listen协程与关闭数据库连接
		cancel()
		types.CloseDb()
	})

	eventChan := make(chan notify.EventInfo, 1e2)
	syncClient := app.NewFileSyncClient()

	go syncClient.ListenAsk(ctx)
	// 启动时候先同步追赶文件到最新版本 之后再加入同步网络
	syncClient.Ask()

	// 注意监听的目标是文件夹 不是具体文件
	// 另外如果有需要监听 home\A home\C 而不需要home\B目录的话 这种需求需要修改代码 对AC分别监听（或者监听home 判断B跳过） 目前就不改动了
	// watch路径带`...`表示递归监听子目录
	if err := notify.Watch(syncClient.WatchPath, eventChan, notify.All); err != nil {
		log.Fatal(err)
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
