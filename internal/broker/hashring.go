package broker

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"

	"vinr.eu/snapflux/internal/storage"
)

const virtualNodes = 150

type ringEntry struct {
	hash uint32
	node storage.BrokerEntity
}

type HashRing struct {
	ring []ringEntry
}

func newHashRing(brokers []storage.BrokerEntity) *HashRing {
	r := &HashRing{}
	for _, b := range brokers {
		for i := 0; i < virtualNodes; i++ {
			r.ring = append(r.ring, ringEntry{hash: hashKey(fmt.Sprintf("%s:%d", b.ID, i)), node: b})
		}
	}
	sort.Slice(r.ring, func(i, j int) bool { return r.ring[i].hash < r.ring[j].hash })
	return r
}

func (r *HashRing) getNode(key string) (storage.BrokerEntity, error) {
	if len(r.ring) == 0 {
		return storage.BrokerEntity{}, errors.New("hash ring is empty")
	}
	h := hashKey(key)
	idx := sort.Search(len(r.ring), func(i int) bool { return r.ring[i].hash >= h })
	if idx == len(r.ring) {
		idx = 0
	}
	return r.ring[idx].node, nil
}

func (r *HashRing) nodeCount() int {
	seen := make(map[string]struct{})
	for _, e := range r.ring {
		seen[e.node.ID] = struct{}{}
	}
	return len(seen)
}

func hashKey(key string) uint32 {
	sum := sha256.Sum256([]byte(key))
	return binary.BigEndian.Uint32(sum[:4])
}
