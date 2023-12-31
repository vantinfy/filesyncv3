package types

import (
	"context"
	"errors"
	"fmt"
	"github.com/go-redis/redis"
	"github.com/jmoiron/sqlx"
	"log"
	"math/rand"
	"sync"
	"time"
)

type RedisClient struct {
	mu sync.Mutex
	*redis.Client
	WriteCancel map[string]context.CancelFunc
}

const KeyPrefix = "fs_" // 因为redis是公共的 所以针对文件同步所存储的数据增加一个前缀 便于与其它数据区分

var (
	_mainExitCtx   context.Context
	VersionWriteWG = sync.WaitGroup{} // 用于确保主程序退出时每个redis写db的协程能完成落库
	BaseExpire     = time.Minute * 10 // redis key基础过期时间
	RandSeed       = rand.New(rand.NewSource(time.Now().UnixNano()))
)

func SetMainCtx(ctx context.Context) {
	_mainExitCtx = ctx
}

type ROption func(opt *redis.Options)

func WithRedisAddr(addr string) ROption {
	return func(opt *redis.Options) {
		opt.Addr = addr
	}
}

func WithRedisPwd(pwd string) ROption {
	return func(opt *redis.Options) {
		opt.Password = pwd
	}
}

func WithRedisDB(dbIndex int) ROption {
	return func(opt *redis.Options) {
		opt.DB = dbIndex
	}
}

func NewRedisClient(opts ...ROption) *RedisClient {
	opt := &redis.Options{
		ReadTimeout: -1,
	}
	for _, option := range opts {
		option(opt)
	}
	return &RedisClient{Client: redis.NewClient(opt), WriteCancel: map[string]context.CancelFunc{}}
}

func (r *RedisClient) Close() {
	_ = r.Client.Close()
}

type SetOption func(info *VersionInfo)

func SetWithDel(isDel bool) SetOption {
	return func(info *VersionInfo) {
		if isDel {
			info.Flag = info.Flag | flagDel
		} else {
			info.Flag = info.Flag &^ flagDel // 按位与非 置个位数为0
		}
	}
}

func SetWithDir(isDir bool) SetOption {
	return func(info *VersionInfo) {
		if isDir {
			info.Flag = info.Flag | flagDir
		} else {
			info.Flag = info.Flag &^ flagDir
		}
	}
}

func (r *RedisClient) SetKey(key string, version int, nodeId string, options ...SetOption) error {
	// 如果key过期前被重新set了一遍 则取消之前的到期写db协程（也就是下面的go func）
	cancel, ok := r.WriteCancel[key]
	if ok {
		cancel()
	}

	randExp := time.Duration(RandSeed.Int63n(10)+1) * time.Minute
	vInfo := &VersionInfo{
		Version: version,
		NodeId:  nodeId,
	}
	for _, option := range options {
		option(vInfo)
	}
	_, err := r.Client.Set(KeyPrefix+key, vInfo, BaseExpire+randExp).Result() // 基础+随机过期时间
	if err != nil {
		//log.Printf("redis set key[%v] failed:[%v]\n", key, err)
		return err
	}

	// 也可以通过消息队列实现相同功能
	// 每set一次key，则往队列添加一条消息 只要一个协程定期将队列的数据写到数据库即可（注意该协程的ticker间隔小于key过期时间）
	VersionWriteWG.Add(1)
	go func() {
		defer VersionWriteWG.Done()
		ticker := time.NewTicker(BaseExpire + randExp)
		defer ticker.Stop()

		ctx, cancel := context.WithCancel(context.Background())
		r.mu.Lock()
		r.WriteCancel[key] = cancel
		r.mu.Unlock()

		f := func() {
			r.mu.Lock()
			defer r.mu.Unlock()

			// 走这个分支说明key到了过期的时间依然没有被更新过 因此可以将version写到db
			_, err = UpdateDBVersion(key, *vInfo)
			if err != nil {
				log.Println("redis setKey goroutine went wrong", key, *vInfo, err)
			}
			delete(r.WriteCancel, key)
		}

		select {
		case <-ctx.Done():
			// 触发了cancel() 取消此协程的执行 返回
			return
		case <-ticker.C: // 时间到了写db
			f()
		case <-_mainExitCtx.Done(): // 或者主程序退出 提前写db 该key也提前删除
			r.Client.Del(KeyPrefix + key)
			f()
		}
	}()

	return nil
}

func (r *RedisClient) GetKey(key string) (int, error) {
	// redis没找到key 尝试在db找 如果db有说明key过期被删除 则重新set一次 db也没有则有错误
	vInfo := VersionInfo{}
	err := r.Client.Get(KeyPrefix + key).Scan(&vInfo)
	if err != nil {
		dbv, ok := QueryDBVersion(key)
		if !ok {
			return 0, errors.New("both redis and db have not key[" + key + "]")
		}
		err = r.SetKey(key, dbv.Version, dbv.NodeId)
		if err != nil {
			return 0, err
		}
		vInfo.Version = dbv.Version
	}
	return vInfo.Version, nil
}

func (r *RedisClient) ScanKeys() map[string]VersionInfo {
	keys := r.Client.Keys(KeyPrefix + "*").Val()
	resp := make(map[string]VersionInfo, len(keys))

	for _, key := range keys {
		vi := VersionInfo{}
		err := r.Client.Get(key).Scan(&vi)
		if err != nil {
			log.Println("scan keys failed", err)
			return resp
		}
		vi.FilePath = key
		resp[key] = vi
	}

	return resp
}

func (r *RedisClient) syncToDB(dbx *sqlx.DB) {
	ticker := time.NewTicker(BaseExpire)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// 获取Redis中的数据
			keys := r.Client.Keys("").Val()
			for _, key := range keys {
				val, err := r.Client.Get(KeyPrefix + key).Result()
				if err != nil {
					log.Println("Error retrieving data from Redis:", err)
					continue
				}

				// 将数据同步到MySQL
				err = syncToMySQL(dbx, KeyPrefix+key, val)
				if err != nil {
					log.Println("Error syncing data to MySQL:", err)
				}
			}
		}
	}
}

func syncToMySQL(dbx *sqlx.DB, key, version string) error {
	_, err := dbx.Exec("INSERT INTO sync_status (path,version) VALUES (?,?)", key, version)
	if err != nil {
		return err
	}

	fmt.Println("Data synced to MySQL successfully.")
	return nil
}
