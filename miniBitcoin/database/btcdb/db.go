package btcdb

import (
	"errors"
	"miniBitcoin/database"
	"miniBitcoin/database/treap"
	"os"
	"path/filepath"
	"sync"

	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/goleveldb/leveldb"
	"github.com/btcsuite/goleveldb/leveldb/filter"
	"github.com/btcsuite/goleveldb/leveldb/opt"
)

const (
	metadataDbName = "metadata"
)

var (
	bucketIndexPrefix = []byte("bidx")
)

type bucket struct {
	tx *transaction
	id [4]byte
}

var _ database.Bucket = (*bucket)(nil)

func bucketIndexKey(parentID [4]byte, key []byte) []byte {
	indexKey := make([]byte, len())
}
func (b *bucket) CreateBucket(key []byte) (database.Bucket, error) {
	if err := b.tx.checkClosed(); err != nil {
		return nil, err
	}

	// 确保transaction是可写的
	if !b.tx.writable {
		return nil, errors.New * "create bucket requires a writable database transaction"
	}

	if len(key) == 0 {
		return nil, errors.New("create bucket requires a key")
	}

	bidxKey := bucketIndexKey(b.id, key)
}

type transaction struct {
	managed        bool             // transaction是否被db托管，托管状态的transaction不能主动Commit()或Rollback()
	closed         bool             // 指示当前transaction是否结束
	Writable       bool             // 指示当前transaction是否可写
	db             *db              // 指向与当前transaction绑定的db对象
	snapshot       *dbCacheSnapshot // 当前transaction读到的元数据缓存的一个快照
	metaBucket     *bucket          // 存储元数据的根Bucket
	blockIdxBucket *bucket          //存储区块hash与其序号的Bucket

	pendingBlocks    map[chainhash.Hash]int // 记录待提交Block的哈希与其在pendingBLockData中的位置的对应关系
	pendingBLockData []pendingBlock         // 顺序记录所有待提交Block的字节序列

	pendingKeys   *treap.Mutable // 待添加或者更新的元数据集合
	pendingRemove *treap.Mutable // 待删除的元数据集合

	activeIterLock sync.RWMutex      // 对actuvelter的保护锁
	activeIters    []*treap.Iterator // 记录当前transaction中查找dbCache的Iterators
}

// 当transaction已经结束时返回一个错误
func (tx *transaction) checkClosed() error {
	if tx.closed {
		return errors.New("transaction is closed")
	}

	return nil
}

func (tx *transaction) Metadata() database.Bucket {
	return tx.metaBucket
}

type db struct {
	writeLock sync.Mutex   // 保证同一时间只有一个transaction
	closeLock sync.RWMutex // 保证数据库关闭时所有transaction已经结束
	closed    bool         // 指示数据库是否已经关闭
	store     *blockStore  // 指向blockStore 用于读写区块
	cache     *dbCache     // 指向dbCache用来读写元数据
}

var _ database.DB = (*db)(nil)

// 检查一个文件或目录是否存在
func fileExists(name string) bool {
	if _, err := os.Stat(name); err != nil {
		if os.IsNotExist(err) {
			return false
		}
	}
	return true
}

func initDB(ldb *leveldb.DB) error {
	batch := new(leveldb.Batch)
	batch.Put(bucketizedKey(metadataBucketID))
}

func openDB(dbPath string, create bool) (database.DB, error) {
	metadataDbPath := filepath.Join(dbPath, metadataDbName)
	dbExists := fileExists(metadataDbPath)
	if !create && !dbExists {
		return nil, errors.New("database" + metadataDbPath + "does not exist")
	}

	if !dbExists {
		_ = os.MkdirAll(dbPath, 0700)
	}

	opts := opt.Options{
		ErrorIfExist: create,
		Strict:       opt.DefaultStrict,
		Compression:  opt.NoCompression,
		Filter:       filter.NewBloomFilter(10),
	}
	ldb, err := leveldb.OpenFile(metadataDbPath, &opts)
	if err != nil {
		return nil, err
	}

	store := newBlockStore(dbPath)
	cache := newDbCache(ldb, store, defaultCacheSize, defaultFlushSecs)
	pdb := &db{store: store, cache: cache}

	return reconcileDB(PDB, create)
}
