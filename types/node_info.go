package types

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"os"
)

type NodeInfo struct {
	*ecdsa.PrivateKey
	UniqueId string
}

const KeyPath = ".key"

func GenerateKeyPair() (*ecdsa.PrivateKey, error) {
	// 生成密钥对
	return ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
}

func GenerateUniqueID(privateKey *ecdsa.PrivateKey) string {
	// 将公钥序列化
	publicKeyBytes := elliptic.Marshal(privateKey.Curve, privateKey.X, privateKey.Y)
	// 计算公钥的 SHA256 哈希值
	hash := sha256.Sum256(publicKeyBytes)

	// 将哈希值转为十六进制字符串作为唯一标识符
	return hex.EncodeToString(hash[:])
}

func (ni *NodeInfo) SavePrivateKey() error {
	keyBytes, err := x509.MarshalECPrivateKey(ni.PrivateKey)
	if err != nil {
		return err
	}

	return os.WriteFile(KeyPath, keyBytes, 0644)
}

func (ni *NodeInfo) LoadPrivateKey() error {
	keyBytes, err := os.ReadFile(KeyPath)
	if errors.Is(err, os.ErrNotExist) {
		// 第一次运行 文件不存在
		ni.PrivateKey, err = GenerateKeyPair()
		if err != nil {
			return err
		}
	} else if err != nil {
		return err
	} else { // err == nil
		ni.PrivateKey, err = x509.ParseECPrivateKey(keyBytes)
		if err != nil {
			return err
		}
	}

	ni.UniqueId = GenerateUniqueID(ni.PrivateKey)
	return nil
}
