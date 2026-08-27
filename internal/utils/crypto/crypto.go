package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"io"
)

// Encryptor 加密器接口
type Encryptor interface {
	Encrypt(data []byte) ([]byte, error)
	Decrypt(data []byte) ([]byte, error)
}

// AESEncryptor AES加密器
type AESEncryptor struct {
	key []byte
	gcm cipher.AEAD
}

// NewAESEncryptor 创建AES加密器
func NewAESEncryptor(key string) (*AESEncryptor, error) {
	// 使用SHA256生成32字节密钥
	hash := sha256.Sum256([]byte(key))

	block, err := aes.NewCipher(hash[:])
	if err != nil {
		return nil, fmt.Errorf("failed to create AES cipher: %v", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCM: %v", err)
	}

	return &AESEncryptor{
		key: hash[:],
		gcm: gcm,
	}, nil
}

// Encrypt 加密数据
func (e *AESEncryptor) Encrypt(data []byte) ([]byte, error) {
	nonce := make([]byte, e.gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("failed to generate nonce: %v", err)
	}

	ciphertext := e.gcm.Seal(nonce, nonce, data, nil)
	return ciphertext, nil
}

// Decrypt 解密数据
func (e *AESEncryptor) Decrypt(data []byte) ([]byte, error) {
	if len(data) < e.gcm.NonceSize() {
		return nil, fmt.Errorf("ciphertext too short")
	}

	nonce, ciphertext := data[:e.gcm.NonceSize()], data[e.gcm.NonceSize():]

	plaintext, err := e.gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt: %v", err)
	}

	return plaintext, nil
}

// NoOpEncryptor 空加密器（不执行任何加密）
type NoOpEncryptor struct{}

func (e *NoOpEncryptor) Encrypt(data []byte) ([]byte, error) {
	return data, nil
}

func (e *NoOpEncryptor) Decrypt(data []byte) ([]byte, error) {
	return data, nil
}

// NewEncryptor 根据配置创建加密器
func NewEncryptor(enabled bool, key string) (Encryptor, error) {
	if !enabled || key == "" {
		return &NoOpEncryptor{}, nil
	}

	return NewAESEncryptor(key)
}
