package main

import (
	"filesyncv3/api"
	"filesyncv3/app"
	"filesyncv3/types"
	"github.com/rjeczalik/notify"
	"log"
)

func main() {
	api.Init(func() {})

	eventChan := make(chan notify.EventInfo, 1e2)
	syncClient := app.NewFileSyncClient()

	// 注意监听的目标是文件夹 不是具体文件
	// 另外如果有需要监听 home\A home\C 而不需要home\B目录的话 这种需求需要修改代码 对AC分别监听（或者监听home 判断B跳过） 目前就不改动了
	// watch路径带`...`表示递归监听子目录
	if err := notify.Watch(syncClient.WatchPath, eventChan, notify.All); err != nil {
		log.Fatal(err)
	}
	defer notify.Stop(eventChan)

	// 监听文件变化的协程
	go syncClient.Watch(eventChan)

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