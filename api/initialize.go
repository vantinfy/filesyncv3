package api

import (
	"errors"
	"filesyncv3/app"
	"filesyncv3/types"
	"log"
	"os"
	"os/signal"
	"syscall"
)

// 有build tag的情况下不必担心init会入侵其它代码
func init() {}

var initDone = false

func Init(beforeExit func()) {
	if initDone {
		return
	}

	// 创建将发送接收信号的通道。发送信号且通道未就绪时，通知不会阻止。因此最好创建缓冲通道。
	sig := make(chan os.Signal, 1)
	// Notify将捕获给定的信号并通过sig发送os.Signal值。如果参数中未指定信号，则匹配所有信号。
	signal.Notify(sig, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGHUP)
	// 两种最常见的信号是SIGINT和SIGTERM。这两个信号将导致程序终止。SIGHUP表示调用该进程的终端已经关闭，程序可以通过该信号决定是否移动到后台执行

	// 创建要等待信号处理的通道
	exitChan := make(chan int)
	go func() {
		s := <-sig
		if i, ok := s.(syscall.Signal); ok {
			exitChan <- int(i)
		} else {
			exitChan <- 0
		}
	}()

	// 这个不能放到init中执行 因为init比main还要先执行 此时db是尚未连接的状态 相关数据获取不到
	loadCache()

	// 监听退出信号 注意：kill -9信号是无法被监听的
	go func() {
		code := <-exitChan
		_ = types.SaveCacheToFile(types.SyncCachePath)
		_ = app.NewFileSyncClient().SavePrivateKey()
		beforeExit()
		os.Exit(code)
	}()
}

func loadCache() {
	initDone = true
	_, err := os.Stat(types.SyncCachePath)
	if errors.Is(err, os.ErrNotExist) {
		// 第一次启动 缓存文件不存在 正常
		return
	} else if err != nil {
		log.Println(err)
		return
	}

	err = types.LoadCacheFromFile(types.SyncCachePath)
	if err != nil {
		log.Println(err)
	}
}

// 检查配置参数是否合法
func validConfig(config *types.SyncConfig) {
	if config.Redis2DBInterval == 0 {
		config.Redis2DBInterval = 600
		log.Println("写入间隔不能为0,已设置为600s")
	}
}
