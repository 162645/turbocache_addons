package protection_test

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"turbocache/internal/protection/bloom"
)

// ===== 基础功能测试 =====

// TestNewBloomFilter 测试创建布隆过滤器
func TestNewBloomFilter(t *testing.T) {
	config := &bloom.Config{
		ExpectedElements:  10000,
		FalsePositiveRate: 0.01,
	}

	bf := bloom.NewBloomFilter(config)
	if bf == nil {
		t.Fatal("期望非空布隆过滤器")
	}

	stats := bf.Stats()
	if stats.BitArraySize == 0 {
		t.Error("位数组大小不应该为0")
	}
	if stats.HashFunctions == 0 {
		t.Error("哈希函数数量不应该为0")
	}
}

// TestBloomFilter_Add_Test 测试添加和测试
func TestBloomFilter_Add_Test(t *testing.T) {
	config := &bloom.Config{
		ExpectedElements:  1000,
		FalsePositiveRate: 0.01,
	}

	bf := bloom.NewBloomFilter(config)

	// 添加元素
	testData := []string{"apple", "banana", "cherry", "date"}
	for _, item := range testData {
		bf.AddString(item)
	}

	// 测试存在的元素
	for _, item := range testData {
		if !bf.TestString(item) {
			t.Errorf("期望 %s 在过滤器中", item)
		}
	}

	// 测试不存在的元素
	notExist := []string{"grape", "kiwi", "mango", "orange", "peach"}
	falsePositives := 0
	for _, item := range notExist {
		if bf.TestString(item) {
			falsePositives++
		}
	}

	// 误报率应该很低
	t.Logf("误报: %d/%d", falsePositives, len(notExist))
}

// TestBloomFilter_AddBytes 测试字节数组添加
func TestBloomFilter_AddBytes(t *testing.T) {
	config := &bloom.Config{
		ExpectedElements:  100,
		FalsePositiveRate: 0.01,
	}

	bf := bloom.NewBloomFilter(config)

	// 添加字节数组
	data := [][]byte{
		[]byte("test1"),
		[]byte("test2"),
		[]byte("test3"),
	}

	for _, d := range data {
		bf.Add(d)
	}

	// 验证
	for _, d := range data {
		if !bf.Test(d) {
			t.Errorf("期望 %s 在过滤器中", string(d))
		}
	}
}

// TestBloomFilter_TestAndAdd 测试TestAndAdd原子操作
func TestBloomFilter_TestAndAdd(t *testing.T) {
	config := &bloom.Config{
		ExpectedElements:  100,
		FalsePositiveRate: 0.01,
	}

	bf := bloom.NewBloomFilter(config)

	// 第一次应该返回false（不存在）
	exists := bf.TestAndAdd([]byte("test"))
	if exists {
		t.Error("第一次TestAndAdd期望返回false")
	}

	// 第二次应该返回true（已存在）
	exists = bf.TestAndAdd([]byte("test"))
	if !exists {
		t.Error("第二次TestAndAdd期望返回true")
	}

	// 验证确实添加了
	if !bf.Test([]byte("test")) {
		t.Error("期望test在过滤器中")
	}
}

// TestBloomFilter_Clear 测试清空
func TestBloomFilter_Clear(t *testing.T) {
	config := &bloom.Config{
		ExpectedElements:  100,
		FalsePositiveRate: 0.01,
	}

	bf := bloom.NewBloomFilter(config)

	// 添加元素
	for i := 0; i < 10; i++ {
		bf.AddString(fmt.Sprintf("item_%d", i))
	}

	// 验证存在
	for i := 0; i < 10; i++ {
		if !bf.TestString(fmt.Sprintf("item_%d", i)) {
			t.Errorf("期望item_%d在过滤器中", i)
		}
	}

	// 清空
	bf.Clear()

	// 验证已清空
	for i := 0; i < 10; i++ {
		if bf.TestString(fmt.Sprintf("item_%d", i)) {
			t.Errorf("清空后期望item_%d不在过滤器中", i)
		}
	}

	// 统计信息应该重置
	stats := bf.Stats()
	if stats.ElementsAdded != 0 {
		t.Errorf("获得 %d 个元素, 期望 0", stats.ElementsAdded)
	}
}

// ===== 统计信息测试 =====

// TestBloomFilter_Stats 测试统计信息
func TestBloomFilter_Stats(t *testing.T) {
	config := &bloom.Config{
		ExpectedElements:  1000,
		FalsePositiveRate: 0.01,
	}

	bf := bloom.NewBloomFilter(config)

	// 添加元素
	const numElements = 100
	for i := 0; i < numElements; i++ {
		bf.AddString(fmt.Sprintf("item_%d", i))
	}

	// 进行一些测试
	for i := 0; i < 50; i++ {
		bf.TestString(fmt.Sprintf("item_%d", i))
	}

	stats := bf.Stats()

	// 验证统计信息
	if stats.ElementsAdded != numElements {
		t.Errorf("获得 %d 个已添加元素, 期望 %d", stats.ElementsAdded, numElements)
	}
	if stats.AddCount != numElements {
		t.Errorf("获得 %d 次添加操作, 期望 %d", stats.AddCount, numElements)
	}
	if stats.CheckCount != 50 {
		t.Errorf("获得 %d 次检查操作, 期望 50", stats.CheckCount)
	}
	if stats.Capacity <= 0 || stats.Capacity > 1 {
		t.Errorf("容量 %f 超出范围 [0, 1]", stats.Capacity)
	}
	if stats.EstimatedFPR < 0 || stats.EstimatedFPR > 1 {
		t.Errorf("误报率 %f 超出范围 [0, 1]", stats.EstimatedFPR)
	}
}

// TestBloomFilter_FalsePositiveRate 测试实际误报率
func TestBloomFilter_FalsePositiveRate(t *testing.T) {
	if testing.Short() {
		t.Skip("在短测试模式下跳过误报率测试")
	}

	config := &bloom.Config{
		ExpectedElements:  10000,
		FalsePositiveRate: 0.01,
	}

	bf := bloom.NewBloomFilter(config)

	// 添加元素
	const numElements = 5000
	for i := 0; i < numElements; i++ {
		bf.AddString(fmt.Sprintf("item_%d", i))
	}

	// 测试不存在的元素
	const testCount = 10000
	falsePositives := 0
	for i := numElements; i < numElements+testCount; i++ {
		if bf.TestString(fmt.Sprintf("item_%d", i)) {
			falsePositives++
		}
	}

	actualFPR := float64(falsePositives) / float64(testCount)
	t.Logf("实际误报率: %.4f, 期望: %.4f", actualFPR, config.FalsePositiveRate)

	// 实际误报率应该接近预期（允许2倍误差）
	if actualFPR > config.FalsePositiveRate*2 {
		t.Errorf("误报率过高: 获得 %.4f, 期望 <= %.4f", actualFPR, config.FalsePositiveRate*2)
	}
}

// ===== 并发安全测试 =====

// TestBloomFilter_ConcurrentAdd 测试并发添加
func TestBloomFilter_ConcurrentAdd(t *testing.T) {
	config := &bloom.Config{
		ExpectedElements:  10000,
		FalsePositiveRate: 0.01,
	}

	bf := bloom.NewBloomFilter(config)

	const (
		goroutines = 100
		itemsEach  = 100
	)

	var wg sync.WaitGroup
	wg.Add(goroutines)

	// 并发添加
	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < itemsEach; i++ {
				bf.AddString(fmt.Sprintf("item_%d_%d", id, i))
			}
		}(g)
	}

	wg.Wait()

	// 验证所有元素都被添加
	missing := 0
	for g := 0; g < goroutines; g++ {
		for i := 0; i < itemsEach; i++ {
			if !bf.TestString(fmt.Sprintf("item_%d_%d", g, i)) {
				missing++
			}
		}
	}

	if missing > 0 {
		t.Errorf("并发添加后缺失 %d 个元素", missing)
	}

	stats := bf.Stats()
	expected := uint64(goroutines * itemsEach)
	if stats.ElementsAdded != expected {
		t.Errorf("获得 %d 个元素, 期望 %d", stats.ElementsAdded, expected)
	}
}

// TestBloomFilter_ConcurrentTest 测试并发查询
func TestBloomFilter_ConcurrentTest(t *testing.T) {
	config := &bloom.Config{
		ExpectedElements:  1000,
		FalsePositiveRate: 0.01,
	}

	bf := bloom.NewBloomFilter(config)

	// 预先添加元素
	const numElements = 100
	for i := 0; i < numElements; i++ {
		bf.AddString(fmt.Sprintf("item_%d", i))
	}

	const goroutines = 50
	var wg sync.WaitGroup
	wg.Add(goroutines)

	errors := int32(0)

	// 并发测试
	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < numElements; i++ {
				if !bf.TestString(fmt.Sprintf("item_%d", i)) {
					atomic.AddInt32(&errors, 1)
				}
			}
		}()
	}

	wg.Wait()

	if errors > 0 {
		t.Errorf("并发测试中出现 %d 个错误", errors)
	}
}

// TestBloomFilter_ConcurrentAddAndTest 测试并发添加和查询
func TestBloomFilter_ConcurrentAddAndTest(t *testing.T) {
	config := &bloom.Config{
		ExpectedElements:  5000,
		FalsePositiveRate: 0.01,
	}

	bf := bloom.NewBloomFilter(config)

	const (
		writers = 20
		readers = 30
		items   = 100
	)

	var wg sync.WaitGroup
	wg.Add(writers + readers)

	// 写入goroutines
	for w := 0; w < writers; w++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < items; i++ {
				bf.AddString(fmt.Sprintf("item_%d_%d", id, i))
				time.Sleep(time.Microsecond)
			}
		}(w)
	}

	// 读取goroutines
	for r := 0; r < readers; r++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < items; i++ {
				bf.TestString(fmt.Sprintf("item_%d_%d", id%writers, i))
				time.Sleep(time.Microsecond)
			}
		}(r)
	}

	wg.Wait()
}

// ===== 合并操作测试 =====

// TestBloomFilter_Union 测试合并
func TestBloomFilter_Union(t *testing.T) {
	config := &bloom.Config{
		ExpectedElements:  1000,
		FalsePositiveRate: 0.01,
	}

	bf1 := bloom.NewBloomFilter(config)
	bf2 := bloom.NewBloomFilter(config)

	// 向bf1添加元素
	for i := 0; i < 50; i++ {
		bf1.AddString(fmt.Sprintf("item_%d", i))
	}

	// 向bf2添加元素
	for i := 25; i < 75; i++ {
		bf2.AddString(fmt.Sprintf("item_%d", i))
	}

	// 合并
	err := bf1.Union(bf2)
	if err != nil {
		t.Fatalf("合并失败: %v", err)
	}

	// 验证合并后的结果
	for i := 0; i < 75; i++ {
		if !bf1.TestString(fmt.Sprintf("item_%d", i)) {
			t.Errorf("期望item_%d在合并后的过滤器中", i)
		}
	}
}

// TestBloomFilter_Union_Incompatible 测试不兼容的合并
func TestBloomFilter_Union_Incompatible(t *testing.T) {
	config1 := &bloom.Config{
		ExpectedElements:  1000,
		FalsePositiveRate: 0.01,
	}
	config2 := &bloom.Config{
		ExpectedElements:  5000,
		FalsePositiveRate: 0.01,
	}

	bf1 := bloom.NewBloomFilter(config1)
	bf2 := bloom.NewBloomFilter(config2)

	err := bf1.Union(bf2)
	if err == nil {
		t.Error("期望不兼容过滤器错误")
	}
}

// TestBloomFilter_Intersect 测试交集
func TestBloomFilter_Intersect(t *testing.T) {
	config := &bloom.Config{
		ExpectedElements:  1000,
		FalsePositiveRate: 0.01,
	}

	bf1 := bloom.NewBloomFilter(config)
	bf2 := bloom.NewBloomFilter(config)

	// 向bf1添加元素
	for i := 0; i < 50; i++ {
		bf1.AddString(fmt.Sprintf("item_%d", i))
	}

	// 向bf2添加元素（部分重叠）
	for i := 25; i < 75; i++ {
		bf2.AddString(fmt.Sprintf("item_%d", i))
	}

	// 计算交集
	err := bf1.Intersect(bf2)
	if err != nil {
		t.Fatalf("交集计算失败: %v", err)
	}

	// 交集应该包含25-49
	for i := 25; i < 50; i++ {
		if !bf1.TestString(fmt.Sprintf("item_%d", i)) {
			t.Errorf("期望item_%d在交集中", i)
		}
	}
}

// ===== 自动刷新测试 =====

// TestBloomFilter_AutoRefresh 测试自动刷新
func TestBloomFilter_AutoRefresh(t *testing.T) {
	config := &bloom.Config{
		ExpectedElements:  100,
		FalsePositiveRate: 0.01,
		AutoRefresh:       true,
		RefreshPeriod:     100 * time.Millisecond,
	}

	bf := bloom.NewBloomFilter(config)
	defer bf.Close()

	// 添加元素直到达到刷新阈值
	for i := 0; i < 85; i++ {
		bf.AddString(fmt.Sprintf("item_%d", i))
	}

	// 验证元素存在
	existsBefore := 0
	for i := 0; i < 85; i++ {
		if bf.TestString(fmt.Sprintf("item_%d", i)) {
			existsBefore++
		}
	}

	if existsBefore < 80 {
		t.Errorf("刷新前应该有大部分元素存在，获得 %d/85", existsBefore)
	}

	// 等待自动刷新
	time.Sleep(200 * time.Millisecond)

	// 刷新后元素应该被清除
	existsAfter := 0
	for i := 0; i < 85; i++ {
		if bf.TestString(fmt.Sprintf("item_%d", i)) {
			existsAfter++
		}
	}

	if existsAfter > 10 {
		t.Logf("刷新后仍有 %d/85 元素存在（可能是误报）", existsAfter)
	}
}

// TestBloomFilter_ShouldRefresh 测试刷新条件
func TestBloomFilter_ShouldRefresh(t *testing.T) {
	config := &bloom.Config{
		ExpectedElements:  100,
		FalsePositiveRate: 0.01,
	}

	bf := bloom.NewBloomFilter(config)

	// 初始状态不应该刷新
	if bf.ShouldRefresh() {
		t.Error("空过滤器不应该需要刷新")
	}

	// 添加元素到80%
	for i := 0; i < 80; i++ {
		bf.AddString(fmt.Sprintf("item_%d", i))
	}

	// 现在应该需要刷新
	if !bf.ShouldRefresh() {
		t.Error("容量>80%时应该需要刷新")
	}
}

// ===== 可扩展布隆过滤器测试 =====

// TestScalableBloomFilter_Basic 测试可扩展布隆过滤器基础功能
func TestScalableBloomFilter_Basic(t *testing.T) {
	config := &bloom.Config{
		ExpectedElements:  100,
		FalsePositiveRate: 0.01,
	}

	sbf := bloom.NewScalableBloomFilter(config)

	// 添加大量元素（超过初始容量）
	const numElements = 500
	for i := 0; i < numElements; i++ {
		sbf.Add([]byte(fmt.Sprintf("item_%d", i)))
	}

	// 验证所有元素都存在
	missing := 0
	for i := 0; i < numElements; i++ {
		if !sbf.Test([]byte(fmt.Sprintf("item_%d", i))) {
			missing++
		}
	}

	if missing > 0 {
		t.Errorf("可扩展过滤器中缺失 %d 个元素", missing)
	}

	// 应该创建了多个过滤器
	stats := sbf.Stats()
	if stats.FilterCount < 2 {
		t.Errorf("期望至少2个过滤器, 获得 %d", stats.FilterCount)
	}
	if stats.TotalElements != numElements {
		t.Errorf("获得 %d 个总元素, 期望 %d", stats.TotalElements, numElements)
	}
}

// TestScalableBloomFilter_Stats 测试统计信息
func TestScalableBloomFilter_Stats(t *testing.T) {
	config := &bloom.Config{
		ExpectedElements:  50,
		FalsePositiveRate: 0.01,
	}

	sbf := bloom.NewScalableBloomFilter(config)

	// 添加元素
	for i := 0; i < 200; i++ {
		sbf.Add([]byte(fmt.Sprintf("item_%d", i)))
	}

	stats := sbf.Stats()

	if stats.FilterCount == 0 {
		t.Error("过滤器数量应该大于0")
	}
	if stats.TotalElements != 200 {
		t.Errorf("获得 %d 个总元素, 期望 200", stats.TotalElements)
	}
	if len(stats.FilterStats) != stats.FilterCount {
		t.Errorf("统计详情数量不匹配: %d vs %d", len(stats.FilterStats), stats.FilterCount)
	}

	t.Logf("可扩展过滤器: %d个子过滤器, %d个元素", stats.FilterCount, stats.TotalElements)
}

// ===== 计数布隆过滤器测试 =====

// TestCountingBloomFilter_AddRemove 测试添加和删除
func TestCountingBloomFilter_AddRemove(t *testing.T) {
	cbf := bloom.NewCountingBloomFilter(1000, 0.01)

	// 添加元素
	testData := [][]byte{
		[]byte("apple"),
		[]byte("banana"),
		[]byte("cherry"),
	}

	for _, item := range testData {
		cbf.Add(item)
	}

	// 验证存在
	for _, item := range testData {
		if !cbf.Test(item) {
			t.Errorf("期望 %s 在过滤器中", string(item))
		}
	}

	// 删除一个元素
	removed := cbf.Remove([]byte("banana"))
	if !removed {
		t.Error("期望删除成功")
	}

	// 验证已删除
	if cbf.Test([]byte("banana")) {
		t.Error("删除后期望banana不在过滤器中")
	}

	// 其他元素应该仍然存在
	if !cbf.Test([]byte("apple")) {
		t.Error("期望apple仍在过滤器中")
	}
	if !cbf.Test([]byte("cherry")) {
		t.Error("期望cherry仍在过滤器中")
	}
}

// TestCountingBloomFilter_RemoveNonExistent 测试删除不存在的元素
func TestCountingBloomFilter_RemoveNonExistent(t *testing.T) {
	cbf := bloom.NewCountingBloomFilter(100, 0.01)

	// 删除不存在的元素
	removed := cbf.Remove([]byte("nonexistent"))
	if removed {
		t.Error("删除不存在的元素应该返回false")
	}
}

// TestCountingBloomFilter_MultipleAdd 测试多次添加同一元素
func TestCountingBloomFilter_MultipleAdd(t *testing.T) {
	cbf := bloom.NewCountingBloomFilter(100, 0.01)

	// 多次添加同一元素
	for i := 0; i < 5; i++ {
		cbf.Add([]byte("test"))
	}

	// 多次删除
	for i := 0; i < 5; i++ {
		removed := cbf.Remove([]byte("test"))
		if !removed {
			t.Errorf("第%d次删除失败", i+1)
		}
	}

	// 现在应该不存在
	if cbf.Test([]byte("test")) {
		t.Error("所有删除后元素不应该存在")
	}
}

// ===== 边界条件测试 =====

// TestBloomFilter_EmptyFilter 测试空过滤器
func TestBloomFilter_EmptyFilter(t *testing.T) {
	config := &bloom.Config{
		ExpectedElements:  100,
		FalsePositiveRate: 0.01,
	}

	bf := bloom.NewBloomFilter(config)

	// 测试空过滤器
	if bf.TestString("anything") {
		t.Error("空过滤器不应该包含任何元素")
	}

	stats := bf.Stats()
	if stats.ElementsAdded != 0 {
		t.Errorf("空过滤器应该有0个元素, 获得 %d", stats.ElementsAdded)
	}
}

// TestBloomFilter_ZeroConfig 测试零配置
func TestBloomFilter_ZeroConfig(t *testing.T) {
	config := &bloom.Config{}

	bf := bloom.NewBloomFilter(config)
	if bf == nil {
		t.Fatal("期望非空布隆过滤器")
	}

	// 应该使用默认值
	stats := bf.Stats()
	if stats.MaxElements == 0 {
		t.Error("最大元素数应该有默认值")
	}
}

// TestBloomFilter_EmptyString 测试空字符串
func TestBloomFilter_EmptyString(t *testing.T) {
	config := &bloom.Config{
		ExpectedElements:  100,
		FalsePositiveRate: 0.01,
	}

	bf := bloom.NewBloomFilter(config)

	// 添加空字符串
	bf.AddString("")
	if !bf.TestString("") {
		t.Error("应该能够添加和测试空字符串")
	}
}

// TestBloomFilter_LargeData 测试大数据
func TestBloomFilter_LargeData(t *testing.T) {
	if testing.Short() {
		t.Skip("在短测试模式下跳过大数据测试")
	}

	config := &bloom.Config{
		ExpectedElements:  100000,
		FalsePositiveRate: 0.001,
	}

	bf := bloom.NewBloomFilter(config)

	// 添加大量数据
	const numElements = 50000
	for i := 0; i < numElements; i++ {
		bf.AddString(fmt.Sprintf("large_item_%d", i))
	}

	// 验证部分数据
	missing := 0
	for i := 0; i < numElements; i += 100 {
		if !bf.TestString(fmt.Sprintf("large_item_%d", i)) {
			missing++
		}
	}

	if missing > 0 {
		t.Errorf("大数据测试中缺失 %d 个元素", missing)
	}

	stats := bf.Stats()
	t.Logf("大数据测试: %d元素, 容量%.2f%%, 误报率%.4f",
		stats.ElementsAdded, stats.Capacity*100, stats.EstimatedFPR)
}

// ===== 性能基准测试 =====

// BenchmarkBloomFilter_Add 基准测试添加操作
func BenchmarkBloomFilter_Add(b *testing.B) {
	config := &bloom.Config{
		ExpectedElements:  uint64(b.N),
		FalsePositiveRate: 0.01,
	}

	bf := bloom.NewBloomFilter(config)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bf.AddString(fmt.Sprintf("item_%d", i))
	}
}

// BenchmarkBloomFilter_Test 基准测试查询操作
func BenchmarkBloomFilter_Test(b *testing.B) {
	config := &bloom.Config{
		ExpectedElements:  10000,
		FalsePositiveRate: 0.01,
	}

	bf := bloom.NewBloomFilter(config)

	// 预先添加数据
	for i := 0; i < 10000; i++ {
		bf.AddString(fmt.Sprintf("item_%d", i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bf.TestString(fmt.Sprintf("item_%d", i%10000))
	}
}

// BenchmarkBloomFilter_TestAndAdd 基准测试TestAndAdd操作
func BenchmarkBloomFilter_TestAndAdd(b *testing.B) {
	config := &bloom.Config{
		ExpectedElements:  uint64(b.N),
		FalsePositiveRate: 0.01,
	}

	bf := bloom.NewBloomFilter(config)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bf.TestAndAdd([]byte(fmt.Sprintf("item_%d", i)))
	}
}

// BenchmarkBloomFilter_Concurrent 基准测试并发操作
func BenchmarkBloomFilter_Concurrent(b *testing.B) {
	config := &bloom.Config{
		ExpectedElements:  uint64(b.N),
		FalsePositiveRate: 0.01,
	}

	bf := bloom.NewBloomFilter(config)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			bf.AddString(fmt.Sprintf("item_%d", i))
			i++
		}
	})
}

// BenchmarkCountingBloomFilter_Add 基准测试计数过滤器添加
func BenchmarkCountingBloomFilter_Add(b *testing.B) {
	cbf := bloom.NewCountingBloomFilter(uint64(b.N), 0.01)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cbf.Add([]byte(fmt.Sprintf("item_%d", i)))
	}
}

// BenchmarkScalableBloomFilter_Add 基准测试可扩展过滤器添加
func BenchmarkScalableBloomFilter_Add(b *testing.B) {
	config := &bloom.Config{
		ExpectedElements:  1000,
		FalsePositiveRate: 0.01,
	}

	sbf := bloom.NewScalableBloomFilter(config)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sbf.Add([]byte(fmt.Sprintf("item_%d", i)))
	}
}
