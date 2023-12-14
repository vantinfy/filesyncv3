package types

import (
	"encoding/gob"
	"github.com/patrickmn/go-cache"
	"os"
	"sync"
	"time"
)

// 对本地文件版本号的存取操作

const SyncCachePath = ".sync_cache"

var (
	Mutex            sync.Mutex
	FileVersionCache = cache.New(cache.NoExpiration, cache.NoExpiration)
)

func UpdateCacheVersion(filePath string, newVersion int) {
	Mutex.Lock()
	defer Mutex.Unlock()

	FileVersionCache.Set(filePath, newVersion, cache.DefaultExpiration)
}

func GetCacheVersion(filePath string) (int, bool) {
	Mutex.Lock()
	defer Mutex.Unlock()

	// 从缓存中获取文件版本信息
	if version, found := FileVersionCache.Get(filePath); found {
		return version.(int), true
	}
	return 0, false
}

func ShowCache() map[string]cache.Item {
	return FileVersionCache.Items()
}

func SaveCacheToFile(filename string) error {
	Mutex.Lock()
	defer Mutex.Unlock()

	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := gob.NewEncoder(file)
	for key, value := range FileVersionCache.Items() {
		err := encoder.Encode(cacheItem{Key: key, Value: value.Object, Expiration: time.Unix(0, value.Expiration)})
		if err != nil {
			return err
		}
	}

	return nil
}

type cacheItem struct {
	Key        string
	Value      any
	Expiration time.Time
}

func LoadCacheFromFile(filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	decoder := gob.NewDecoder(file)
	var item cacheItem
	for {
		if err = decoder.Decode(&item); err != nil {
			break
		}
		FileVersionCache.Set(item.Key, item.Value, item.Expiration.Sub(time.Now()))
	}

	return nil
}
