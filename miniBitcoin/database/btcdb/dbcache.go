package btcdb

import (
	"miniBitcoin/database/treap"
	"sync"
	"time"

	"github.com/btcsuite/goleveldb/leveldb"
)

type dbCache struct {
	ldb *leveldb.DB

	store *blockStore

	maxSize       uint64
	flushInterval time.Duration
	lastFlush     time.Time

	cacheLock    sync.RWMutex
	cachedKeys   *treap.Immutable
	cachedRemove *treap.Immutable
}

func newDbCache(ldb *leveldb.DB, store *blockStore, maxSize uint64, flushIntervalSecs uint32) *dbCache {
	return &dbCache{
		ldb:           ldb,
		store:         store,
		maxSize:       maxSize,
		flushInterval: time.Second * time.Duration(flushIntervalSecs),
		lastFlush:     time.Now(),
		cachedKeys:    treap.NewImmutable(),
		cachedRemove:  treap.NewImmutable(),
	}
}
