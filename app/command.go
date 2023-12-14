package app

import (
	"bufio"
	"filesyncv3/types"
	"fmt"
	"log"
	"os"
	"strings"
	"time"
)

func (fsc *FileSyncClient) Cmd() {
	for {
		// 使用 bufio 读取一行文本，直到回车
		reader := bufio.NewReader(os.Stdin)
		input, err := reader.ReadString('\n')
		if err != nil {
			log.Println("Error reading input:", err)
			return
		}

		// 移除换行符
		input = strings.TrimSuffix(input, "\n")
		input = strings.TrimSuffix(input, "\r") // 针对windows

		switch input {
		case "":
		case "cache":
			log.Println(types.ShowCache())
		case "id":
			log.Println("node_id:", fsc.UniqueId)
		case "online", "ol":
			fmt.Println("输入要检测的节点id 以空格分隔 或输入break退出online命令")
			for {
				node := ""
				_, err := fmt.Scan(&node)
				if err != nil || node == "break" {
					break
				}
				if fsc.IsOnline(node) {
					log.Printf("node[%v] is online\n", node)
				} else {
					log.Printf("node[%v] is not online\n", node)
				}
			}
		default:
			log.Printf("unknown command: {{%#v}}\n", input)
		}
	}
}

func (fsc *FileSyncClient) HeartBeat() {
	ticker := time.NewTicker(time.Minute * 9) // 重置间隔最好小于过期时间 否则极限情况下可能出错
	defer ticker.Stop()

	// 启动的时候设置一遍在线状态 注意有个过期时间 这样即使意外崩溃redis也能删除key
	fsc.Client.Set(fsc.UniqueId, 1, time.Minute*10)
	for {
		select {
		case <-ticker.C:
			// 定期重置在线状态的过期时间
			fsc.Client.Set(fsc.UniqueId, 1, time.Minute*10)
		case <-fsc.Ctx.Done():
			// 程序正常退出的时候 设置在线状态为0
			fsc.Client.Set(fsc.UniqueId, 0, 0)
			return
		}
	}
}

func (fsc *FileSyncClient) IsOnline(node string) bool {
	ol, err := fsc.Client.Get(node).Int()
	if err != nil || ol != 1 {
		return false
	}
	return true
}
