package bloom

import "testing"

func TestReplaceBinaryKeepsCompleteSnapshot(t *testing.T) {
	cfg := &Config{ExpectedElements: 1000, FalsePositiveRate: 0.01}
	source := NewBloomFilter(cfg)
	t.Cleanup(func() { _ = source.Close() })
	source.AddString("alpha")
	source.AddString("beta")

	snapshot, err := source.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	target := NewBloomFilter(cfg)
	t.Cleanup(func() { _ = target.Close() })
	if err := target.ReplaceBinary(snapshot, 2); err != nil {
		t.Fatal(err)
	}
	if !target.TestString("alpha") || !target.TestString("beta") {
		t.Fatal("restored filter lost an inserted key")
	}
	if got := target.Stats().ElementsAdded; got != 2 {
		t.Fatalf("elements=%d, want 2", got)
	}
}

func TestReplaceBinaryRejectsIncompatibleFilter(t *testing.T) {
	small := NewBloomFilter(&Config{ExpectedElements: 10, FalsePositiveRate: 0.01})
	large := NewBloomFilter(&Config{ExpectedElements: 1000, FalsePositiveRate: 0.01})
	t.Cleanup(func() { _ = small.Close(); _ = large.Close() })
	snapshot, err := small.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if err := large.ReplaceBinary(snapshot, 0); err == nil {
		t.Fatal("expected incompatible snapshot error")
	}
}
