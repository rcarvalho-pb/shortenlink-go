package bloom

import "math"

type ScalableBloomFilter struct {
	filters    []*BloomFilter
	p0         float64
	scale      float64
	initCap    int
	totalCount int
}

func (sbf *ScalableBloomFilter) Add(item string) {
	current := sbf.filters[len(sbf.filters)-1]
	if current.count >= current.cap {
		newCap := int(float64(sbf.initCap) * math.Pow(sbf.scale, float64(len(sbf.filters))))
		newP := sbf.p0 * math.Pow(0.5, float64(len(sbf.filters)))
		sbf.filters = append(sbf.filters, NewFixed(newCap, newP))
		current = sbf.filters[len(sbf.filters)-1]
	}
	current.Add(item)
	sbf.totalCount++
}

func (sbf *ScalableBloomFilter) Check(item string) bool {
	for _, f := range sbf.filters {
		if f.Check(item) {
			return true
		}
	}
	return false
}
