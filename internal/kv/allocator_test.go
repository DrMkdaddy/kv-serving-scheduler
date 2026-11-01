package kv

import "testing"

func TestAllocAndFree(t *testing.T) {
	a, err := New(4, 4)
	if err != nil {
		t.Fatal(err)
	}
	if !a.Alloc("a", 5) {
		t.Fatal("expected allocation")
	}
	if got := a.Blocks("a"); got != 2 {
		t.Fatalf("blocks = %d, want 2", got)
	}
	if !a.Grow("a", 9) {
		t.Fatal("expected grow")
	}
	if got := a.Blocks("a"); got != 3 {
		t.Fatalf("blocks after grow = %d, want 3", got)
	}
	if !a.Grow("a", 13) {
		t.Fatal("grow to 13 should fit in the last free block")
	}
	if a.Grow("a", 17) {
		t.Fatal("grow to 17 should fail: needs a fifth block")
	}
	if got := a.Blocks("a"); got != 4 {
		t.Fatalf("failed grow changed blocks to %d", got)
	}
	if got := a.Free("a"); got != 4 {
		t.Fatalf("freed %d, want 4", got)
	}
	if got := a.Stats().UsedBlocks; got != 0 {
		t.Fatalf("used = %d after free", got)
	}
}

func TestAllocAtomic(t *testing.T) {
	a, _ := New(4, 2)
	if a.Alloc("a", 5) != true {
		t.Fatal("first alloc")
	}
	if a.Alloc("b", 5) {
		t.Fatal("second alloc should fail")
	}
	if got := a.Stats().ActiveSequences; got != 1 {
		t.Fatalf("active = %d, want 1", got)
	}
}

func TestStatsWasteAndRuns(t *testing.T) {
	a, _ := New(4, 6)
	if !a.Alloc("a", 3) { // 1 block, 1 token waste
		t.Fatal("alloc a")
	}
	if !a.Alloc("b", 8) { // 2 blocks, no waste
		t.Fatal("alloc b")
	}
	s := a.Stats()
	if s.UsedBlocks != 3 || s.FreeBlocks != 3 {
		t.Fatalf("used/free = %d/%d", s.UsedBlocks, s.FreeBlocks)
	}
	if s.InternalWasteTokens != 1 {
		t.Fatalf("waste = %d, want 1", s.InternalWasteTokens)
	}
	if s.LargestFreeRun != 3 || s.FragmentationFreeRuns != 1 {
		t.Fatalf("free runs = %d largest %d", s.FragmentationFreeRuns, s.LargestFreeRun)
	}
}

func TestDuplicateAndUnknown(t *testing.T) {
	a, _ := New(4, 2)
	if !a.Alloc("a", 1) {
		t.Fatal("alloc")
	}
	if a.Alloc("a", 1) {
		t.Fatal("duplicate should fail")
	}
	if a.Free("missing") != 0 {
		t.Fatal("unknown free should be zero")
	}
	if _, ok := a.Tokens("missing"); ok {
		t.Fatal("unknown tokens should be absent")
	}
}
