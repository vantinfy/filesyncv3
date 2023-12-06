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
	BaseExpire = time.Minute * 10 // redis key基础过期时间
	RandSeed   = rand.New(rand.NewSource(time.Now().UnixNano()))
)

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
	opt := &redis.Options{}
	for _, option := range opts {
		option(opt)
	}
	return &RedisClient{Client: redis.NewClient(opt)}
}

func (r *RedisClient) Close() {
	_ = r.Client.Close()
}

func (r *RedisClient) SetKey(key string, version int) error {
	// 如果key过期前被重新set了一遍 则取消之前的到期写db协程（也就是下面的go func）
	cancel, ok := r.WriteCancel[key]
	if ok {
		cancel()
	}

	randExp := time.Duration(RandSeed.Int63n(10)+1) * time.Minute
	_, err := r.Client.Set(KeyPrefix+key, version, BaseExpire+randExp).Result() // 基础+随机过期时间
	if err != nil {
		//log.Printf("redis set key[%v] failed:[%v]\n", key, err)
		return err
	}

	// 也可以通过消息队列实现相同功能
	// 每set一次key，则往队列添加一条消息 只要一个协程定期将队列的数据写到数据库即可（注意该协程的ticker间隔小于key过期时间）
	go func() {
		ticker := time.NewTicker(BaseExpire + randExp)
		defer ticker.Stop()

		ctx, cancel := context.WithCancel(context.Background())
		r.mu.Lock()
		r.WriteCancel[key] = cancel
		r.mu.Unlock()

		select {
		case <-ctx.Done():
			// 触发了cancel() 取消此协程的执行 返回
			return
		case <-ticker.C:
			r.mu.Lock()
			defer r.mu.Unlock()

			// 走这个分支说明key到了过期的时间依然没有被更新过 因此可以将version写到db
			_, err = UpdateDBVersion(key, version)
			if err != nil {
				log.Println(err)
			}
			delete(r.WriteCancel, key)
		}
	}()

	return nil
}

func (r *RedisClient) GetKey(key string) (int, error) {
	// redis没找到key 尝试在db找 如果db有说明key过期被删除 则重新set一次 db也没有则有错误
	version, err := r.Client.Get(KeyPrefix + key).Int()
	if err != nil {
		dbv, ok := QueryDBVersion(key)
		if !ok {
			return 0, errors.New("both redis and db have not key[" + key + "]")
		}
		err = r.SetKey(key, dbv)
		if err != nil {
			return 0, err
		}
		version = dbv
	}
	return version, nil
}

func (r *RedisClient) ScanKeys() map[string]int {
	keys := r.Client.Keys("").Val()
	resp := make(map[string]int, len(keys))

	for _, key := range keys {
		version, err := r.Client.Get(key).Int()
		if err != nil {
			log.Println("scan keys failed", err)
			return resp
		}
		resp[key] = version
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
