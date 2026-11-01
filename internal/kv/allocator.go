// Package kv implements a fixed-pool, block-granular KV cache allocator.
//
// The allocator is deliberately independent of llama.cpp. In the real proxy it
// is the admission model: llama.cpp owns the physical cache, and the proxy uses
// this pool to decide how many sequences may be resident at once. In the
// simulator it is the actual cache model.
package kv

import (
	"fmt"
	"sort"
)

// Stats is a point-in-time view of the pool.
type Stats struct {
	BlockSize             int     `json:"block_size"`
	TotalBlocks           int     `json:"total_blocks"`
	UsedBlocks            int     `json:"used_blocks"`
	FreeBlocks            int     `json:"free_blocks"`
	ActiveSequences       int     `json:"active_sequences"`
	AllocatedTokens       int     `json:"allocated_tokens"`
	UsedTokens            int     `json:"used_tokens"`
	InternalWasteTokens   int     `json:"internal_waste_tokens"`
	Utilization           float64 `json:"utilization"`
	LargestFreeRun        int     `json:"largest_free_run"`
	FragmentationFreeRuns int     `json:"fragmentation_free_runs"`
}

// Sequence is one resident request's KV footprint.
type Sequence struct {
	ID     string
	Blocks []int
	Tokens int
}

// Allocator is not safe for concurrent use; callers hold their own lock.
type Allocator struct {
	blockSize int
	total     int
	free      []int
	seqs      map[string]*Sequence
}

// New creates a pool with total blocks of blockSize tokens each.
func New(blockSize, total int) (*Allocator, error) {
	if blockSize <= 0 {
		return nil, fmt.Errorf("block size must be positive")
	}
	if total <= 0 {
		return nil, fmt.Errorf("total blocks must be positive")
	}
	free := make([]int, total)
	for i := range free {
		free[i] = i
	}
	return &Allocator{
		blockSize: blockSize,
		total:     total,
		free:      free,
		seqs:      make(map[string]*Sequence),
	}, nil
}

func blocksFor(tokens, blockSize int) int {
	if tokens <= 0 {
		return 0
	}
	return (tokens + blockSize - 1) / blockSize
}

// Alloc creates a sequence that can hold tokens. It is atomic: on failure the
// sequence does not exist and no blocks are consumed.
func (a *Allocator) Alloc(id string, tokens int) bool {
	if _, exists := a.seqs[id]; exists {
		return false
	}
	need := blocksFor(tokens, a.blockSize)
	if need > len(a.free) {
		return false
	}
	blocks := make([]int, need)
	copy(blocks, a.free[:need])
	a.free = a.free[need:]
	a.seqs[id] = &Sequence{ID: id, Blocks: blocks, Tokens: tokens}
	return true
}

// Grow extends a sequence to tokens. It is atomic: on failure the sequence
// keeps its previous block set and token count.
func (a *Allocator) Grow(id string, tokens int) bool {
	seq, ok := a.seqs[id]
	if !ok {
		return false
	}
	if tokens <= seq.Tokens {
		seq.Tokens = tokens
		return true
	}
	need := blocksFor(tokens, a.blockSize) - len(seq.Blocks)
	if need <= 0 {
		seq.Tokens = tokens
		return true
	}
	if need > len(a.free) {
		return false
	}
	seq.Blocks = append(seq.Blocks, a.free[:need]...)
	a.free = a.free[need:]
	seq.Tokens = tokens
	return true
}

// Free releases every block owned by id and returns the number freed.
func (a *Allocator) Free(id string) int {
	seq, ok := a.seqs[id]
	if !ok {
		return 0
	}
	n := len(seq.Blocks)
	a.free = append(a.free, seq.Blocks...)
	sort.Ints(a.free)
	delete(a.seqs, id)
	return n
}

// Tokens returns the sequence's current token count.
func (a *Allocator) Tokens(id string) (int, bool) {
	seq, ok := a.seqs[id]
	if !ok {
		return 0, false
	}
	return seq.Tokens, true
}

// Blocks returns the number of blocks owned by id.
func (a *Allocator) Blocks(id string) int {
	seq, ok := a.seqs[id]
	if !ok {
		return 0
	}
	return len(seq.Blocks)
}

// IDs returns the resident sequence IDs in stable order.
func (a *Allocator) IDs() []string {
	ids := make([]string, 0, len(a.seqs))
	for id := range a.seqs {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// CanFit reports whether one more allocation of tokens would fit.
func (a *Allocator) CanFit(tokens int) bool {
	return blocksFor(tokens, a.blockSize) <= len(a.free)
}

// Stats snapshots the pool.
func (a *Allocator) Stats() Stats {
	used := 0
	usedTokens := 0
	waste := 0
	for _, seq := range a.seqs {
		used += len(seq.Blocks)
		usedTokens += seq.Tokens
		waste += len(seq.Blocks)*a.blockSize - seq.Tokens
	}
	free := len(a.free)
	largest := 0
	runs := 0
	if free > 0 {
		sorted := append([]int(nil), a.free...)
		sort.Ints(sorted)
		run := 1
		largest = 1
		for i := 1; i < len(sorted); i++ {
			if sorted[i] == sorted[i-1]+1 {
				run++
				if run > largest {
					largest = run
				}
			} else {
				runs++
				run = 1
			}
		}
		runs++
	}
	util := 0.0
	if a.total > 0 {
		util = float64(used) / float64(a.total)
	}
	return Stats{
		BlockSize:             a.blockSize,
		TotalBlocks:           a.total,
		UsedBlocks:            used,
		FreeBlocks:            free,
		ActiveSequences:       len(a.seqs),
		AllocatedTokens:       used * a.blockSize,
		UsedTokens:            usedTokens,
		InternalWasteTokens:   waste,
		Utilization:           util,
		LargestFreeRun:        largest,
		FragmentationFreeRuns: runs,
	}
}
