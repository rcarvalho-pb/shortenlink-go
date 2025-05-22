package bloom

import (
	"hash/fnv"
	"math"
)

type BloomFilter struct {
	bitset []bool
	size   uint
	k      int
	count  int
	cap    int
}

func NewFixed(n int, p float64) *BloomFilter {
	m := optimalBitSize(n, p)
	k := optimalHashFunctions(m, n)
	return &BloomFilter{
		bitset: make([]bool, m),
		size:   uint(m),
		k:      k,
		cap:    n,
	}
}

func (bf *BloomFilter) hash(item string) []uint {
	hashes := make([]uint, bf.k)
	for i := range bf.k {
		h := fnv.New32a()
		h.Write([]byte{byte(i)})
		h.Write([]byte(item))
		hashes[i] = uint(h.Sum32()) % bf.size
	}
	return hashes
}

func (bf *BloomFilter) Add(item string) {
	for _, h := range bf.hash(item) {
		bf.bitset[h] = true
	}
	bf.count++
}

func (bf *BloomFilter) Check(item string) bool {
	for _, h := range bf.hash(item) {
		if !bf.bitset[h] {
			return false
		}
	}
	return true
}

func optimalBitSize(n int, p float64) int {
	return int(math.Ceil(-float64(n) * math.Log(p) / (math.Ln2 * math.Ln2)))
}

func optimalHashFunctions(m, n int) int {
	return int(math.Ceil((float64(m) / float64(n)) * math.Ln2))
}
