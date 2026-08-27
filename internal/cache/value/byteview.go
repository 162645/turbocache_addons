package value

import (
	"bytes"
	"errors"
	"unsafe"
)

// ByteView 只读字节视图，避免数据拷贝
type ByteView struct {
	b []byte // 如果是字符串，这里存储字符串数据
	s string // 如果是字节数组，这里为空
}

// NewByteView 从字节数组创建ByteView
func NewByteView(b []byte) ByteView {
	return ByteView{b: cloneBytes(b)}
}

// NewByteViewFromString 从字符串创建ByteView
func NewByteViewFromString(s string) ByteView {
	return ByteView{s: s}
}

// Len 返回数据长度
func (v ByteView) Len() int {
	if v.b != nil {
		return len(v.b)
	}
	return len(v.s)
}

// ByteSlice 返回字节切片的副本
func (v ByteView) ByteSlice() []byte {
	if v.b != nil {
		return cloneBytes(v.b)
	}
	return []byte(v.s)
}

// String 返回字符串表示
func (v ByteView) String() string {
	if v.b != nil {
		return string(v.b)
	}
	return v.s
}

// At 返回指定位置的字节
func (v ByteView) At(i int) byte {
	if v.b != nil {
		return v.b[i]
	}
	return v.s[i]
}

// Slice 返回子切片
func (v ByteView) Slice(from, to int) ByteView {
	if v.b != nil {
		return ByteView{b: v.b[from:to]}
	}
	return ByteView{s: v.s[from:to]}
}

// SliceFrom 从指定位置开始的子切片
func (v ByteView) SliceFrom(from int) ByteView {
	if v.b != nil {
		return ByteView{b: v.b[from:]}
	}
	return ByteView{s: v.s[from:]}
}

// Equal 比较两个ByteView是否相等
func (v ByteView) Equal(other ByteView) bool {
	if v.Len() != other.Len() {
		return false
	}

	if v.b != nil && other.b != nil {
		return bytes.Equal(v.b, other.b)
	}

	if v.s != "" && other.s != "" {
		return v.s == other.s
	}

	// 混合比较
	return v.String() == other.String()
}

// WriteTo 将数据写入到目标切片
func (v ByteView) WriteTo(dst []byte) (int, error) {
	if len(dst) < v.Len() {
		return 0, errors.New("destination slice too small")
	}

	if v.b != nil {
		return copy(dst, v.b), nil
	}
	return copy(dst, v.s), nil
}

// ReadFrom 从源数据读取
func (v *ByteView) ReadFrom(src []byte) {
	v.b = cloneBytes(src)
	v.s = ""
}

// ReadFromString 从字符串读取
func (v *ByteView) ReadFromString(src string) {
	v.s = src
	v.b = nil
}

// IsEmpty 检查是否为空
func (v ByteView) IsEmpty() bool {
	return v.Len() == 0
}

// Clone 创建副本
func (v ByteView) Clone() ByteView {
	if v.b != nil {
		return ByteView{b: cloneBytes(v.b)}
	}
	return ByteView{s: v.s}
}

// UnsafeString 返回字符串表示，不进行数据拷贝（危险操作）
func (v ByteView) UnsafeString() string {
	if v.b != nil {
		return *(*string)(unsafe.Pointer(&v.b))
	}
	return v.s
}

// UnsafeBytes 返回字节切片，不进行数据拷贝（危险操作）
func (v ByteView) UnsafeBytes() []byte {
	if v.b != nil {
		return v.b
	}
	return *(*[]byte)(unsafe.Pointer(&struct {
		string
		int
	}{v.s, len(v.s)}))
}

// Hash 计算哈希值
func (v ByteView) Hash() uint64 {
	var h uint64 = 14695981039346656037 // FNV offset basis
	const prime = 1099511628211         // FNV prime

	for i := 0; i < v.Len(); i++ {
		h ^= uint64(v.At(i))
		h *= prime
	}
	return h
}

// Compare 比较两个ByteView的大小关系
func (v ByteView) Compare(other ByteView) int {
	minLen := v.Len()
	if other.Len() < minLen {
		minLen = other.Len()
	}

	for i := 0; i < minLen; i++ {
		a, b := v.At(i), other.At(i)
		if a < b {
			return -1
		}
		if a > b {
			return 1
		}
	}

	if v.Len() < other.Len() {
		return -1
	}
	if v.Len() > other.Len() {
		return 1
	}
	return 0
}

// Contains 检查是否包含子序列
func (v ByteView) Contains(sub ByteView) bool {
	return v.Index(sub) >= 0
}

// Index 查找子序列的位置
func (v ByteView) Index(sub ByteView) int {
	if sub.Len() == 0 {
		return 0
	}
	if sub.Len() > v.Len() {
		return -1
	}

	for i := 0; i <= v.Len()-sub.Len(); i++ {
		match := true
		for j := 0; j < sub.Len(); j++ {
			if v.At(i+j) != sub.At(j) {
				match = false
				break
			}
		}
		if match {
			return i
		}
	}
	return -1
}

// HasPrefix 检查是否以指定前缀开头
func (v ByteView) HasPrefix(prefix ByteView) bool {
	if prefix.Len() > v.Len() {
		return false
	}
	for i := 0; i < prefix.Len(); i++ {
		if v.At(i) != prefix.At(i) {
			return false
		}
	}
	return true
}

// HasSuffix 检查是否以指定后缀结尾
func (v ByteView) HasSuffix(suffix ByteView) bool {
	if suffix.Len() > v.Len() {
		return false
	}
	offset := v.Len() - suffix.Len()
	for i := 0; i < suffix.Len(); i++ {
		if v.At(offset+i) != suffix.At(i) {
			return false
		}
	}
	return true
}

// Split 按分隔符分割
func (v ByteView) Split(sep byte) []ByteView {
	var result []ByteView
	start := 0

	for i := 0; i < v.Len(); i++ {
		if v.At(i) == sep {
			if i > start {
				result = append(result, v.Slice(start, i))
			}
			start = i + 1
		}
	}

	if start < v.Len() {
		result = append(result, v.SliceFrom(start))
	}

	return result
}

// TrimSpace 去除首尾空白字符
func (v ByteView) TrimSpace() ByteView {
	start := 0
	end := v.Len()

	// 查找开头非空白字符
	for start < end && isSpace(v.At(start)) {
		start++
	}

	// 查找结尾非空白字符
	for end > start && isSpace(v.At(end-1)) {
		end--
	}

	return v.Slice(start, end)
}

// ToUpper 转换为大写
func (v ByteView) ToUpper() ByteView {
	result := make([]byte, v.Len())
	for i := 0; i < v.Len(); i++ {
		b := v.At(i)
		if b >= 'a' && b <= 'z' {
			result[i] = b - 32
		} else {
			result[i] = b
		}
	}
	return ByteView{b: result}
}

// ToLower 转换为小写
func (v ByteView) ToLower() ByteView {
	result := make([]byte, v.Len())
	for i := 0; i < v.Len(); i++ {
		b := v.At(i)
		if b >= 'A' && b <= 'Z' {
			result[i] = b + 32
		} else {
			result[i] = b
		}
	}
	return ByteView{b: result}
}

// Reverse 反转字节序列
func (v ByteView) Reverse() ByteView {
	result := make([]byte, v.Len())
	for i := 0; i < v.Len(); i++ {
		result[i] = v.At(v.Len() - 1 - i)
	}
	return ByteView{b: result}
}

// 辅助函数

// cloneBytes 克隆字节切片
func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	c := make([]byte, len(b))
	copy(c, b)
	return c
}

// isSpace 检查是否为空白字符
func isSpace(b byte) bool {
	return b == ' ' || b == '\t' || b == '\n' || b == '\r' || b == '\f' || b == '\v'
}

// ByteViewPool ByteView对象池，减少内存分配
type ByteViewPool struct {
	pool chan ByteView
}

// NewByteViewPool 创建ByteView对象池
func NewByteViewPool(size int) *ByteViewPool {
	return &ByteViewPool{
		pool: make(chan ByteView, size),
	}
}

// Get 从池中获取ByteView
func (p *ByteViewPool) Get() ByteView {
	select {
	case bv := <-p.pool:
		return bv
	default:
		return ByteView{}
	}
}

// Put 将ByteView放回池中
func (p *ByteViewPool) Put(bv ByteView) {
	// 重置ByteView
	bv.b = nil
	bv.s = ""

	select {
	case p.pool <- bv:
	default:
		// 池已满，丢弃
	}
}

//🧩 byteview 的作用：数据视图层
//通常用于封装底层数据（如 []byte 或 string）的只读视图。
//
//提供统一的数据访问接口，避免直接暴露原始数据。
//
//典型功能包括：
//
//数据克隆（防止外部修改）
//
//字节与字符串互转
//
//哈希计算、比较、切片、大小写转换等
//
//在缓存系统中，ByteView 常用于表示缓存值的只读封装。
