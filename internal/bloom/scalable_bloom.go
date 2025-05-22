package bloom

import (
	"database/sql"
	"math"
)

type ScalableBloomFilter struct {
	filters    []*BloomFilter
	p0         float64
	scale      float64
	initCap    int
	totalCount int
}

func NewScalableFilter() {}

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

func (sbf *ScalableBloomFilter) Save(db *sql.DB) func() error {
	return func() error {
		for _, b := range sbf.filters {
			bytes := b.BoolsToBytes()
			if _, err := db.Exec(`
			INSERT INTO bloom_filters (bitset, size, k, cap, count)
				VALUES
				(?, ?, ?, ?, ?)
				`, bytes, b.size, b.k, b.cap, b.count); err != nil {
				return err
			}
		}
		return nil
	}
}

func (sbf *ScalableBloomFilter) Load(db *sql.DB) func() error {
	return func() error {
		rows, err := db.Query(`
			SELECT (bitset, id, size, k, cap, count) FROM bloom_filters
			`)
		if err != nil {
			return err
		}
		for rows.Next() {
			var filter BloomFilter
			var bytes []byte
			if err := rows.Scan(&bytes, &filter., &filter.size, &filter.k, &filter.cap, &filter.count); err != nil {
				return err
			}
			filter.bitset = BytesToBool(bytes, int(filter.size))
			sbf.filters = append(sbf.filters, &filter)
		}
		return nil
	}
}
