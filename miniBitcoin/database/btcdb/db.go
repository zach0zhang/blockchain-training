package btcdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"miniBitcoin/database"
	"miniBitcoin/database/treap"
	"os"
	"path/filepath"
	"runtime"
	"sync"

	"github.com/btcsuite/goleveldb/leveldb/comparer"
	"github.com/btcsuite/goleveldb/leveldb/util"

	"github.com/btcsuite/goleveldb/leveldb/iterator"

	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/goleveldb/leveldb"
	"github.com/btcsuite/goleveldb/leveldb/filter"
	"github.com/btcsuite/goleveldb/leveldb/opt"
)

const (
	metadataDbName = "metadata"
)

var (
	metadataBucketID = [4]byte{}

	bucketIndexPrefix = []byte("bidx")

	curBucketIDKeyName = []byte("bidx-cbid")

	blockIdxBucketID = [4]byte{0x00, 0x00, 0x00, 0x01}

	blockIdxBucketName = []byte("btcdb-blockidx")

	writeLocKeyNmae = []byte("btcdb-writeloc")
)

func copySlice(slice []byte) []byte {
	ret := make([]byte, len(slice))
	copy(ret, slice)
	return ret
}

type cursor struct {
	bucket      *bucket
	dbIter      iterator.Iterator
	pendingIter iterator.Iterator
	currentIter iterator.Iterator
}

var _ database.Cursor = (*cursor)(nil)

// Bucket returns the bucket the cursor was created for.
func (c *cursor) Bucket() database.Bucket {
	if err := c.bucket.tx.checkClosed(); err != nil {
		return nil
	}

	return c.bucket
}

// Delete removes the current key/value pair the cursor is at without
// invalidating the cursor.
func (c *cursor) Delete() error {
	if err := c.bucket.tx.checkClosed(); err != nil {
		return err
	}

	if c.currentIter == nil {
		return errors.New("cursor is exhausted")
	}

	key := c.currentIter.Key()
	if bytes.HasPrefix(key, bucketIndexPrefix) {
		return errors.New("buckets may not be deleted from a cursor")
	}

	c.bucket.tx.deleteKey(copySlice(key), true)
	return nil
}

func (c *cursor) skipPendingUpdates(forwards bool) {
	for c.dbIter.Valid() {
		var skip bool
		key := c.dbIter.Key()
		if c.bucket.tx.pendingRemove.Has(key) {
			skip = true
		} else if c.bucket.tx.pendingKeys.Has(key) {
			skip = true
		}
		if !skip {
			break
		}

		if forwards {
			c.dbIter.Next()
		} else {
			c.dbIter.Prev()
		}
	}
}

func (c *cursor) chooseIterator(forwards bool) bool {
	// 跳过正在事物更新处理中的 iterator
	c.skipPendingUpdates(forwards)

	if !c.dbIter.Valid() && !c.pendingIter.Valid() {
		c.currentIter = nil
		return false
	}

	if !c.pendingIter.Valid() {
		c.currentIter = c.dbIter
		return true
	}

	if !c.dbIter.Valid() {
		c.currentIter = c.pendingIter
		return true
	}

	compare := bytes.Compare(c.dbIter.Key(), c.pendingIter.Key())
	if (forwards && compare > 0) || (!forwards && compare < 0) {
		c.currentIter = c.pendingIter
	} else {
		c.currentIter = c.dbIter
	}
	return true
}

// First positions the cursor at the first key/value pair and returns whether or
// not the pair exists.
func (c *cursor) First() bool {
	if err := c.bucket.tx.checkClosed(); err != nil {
		return false
	}

	c.dbIter.First()
	c.pendingIter.First()
	return c.chooseIterator(true)
}

func (c *cursor) Last() bool {
	if err := c.bucket.tx.checkClosed(); err != nil {
		return false
	}

	c.dbIter.Last()
	c.pendingIter.Last()
	return c.chooseIterator(false)
}

func (c *cursor) Next() bool {
	if err := c.bucket.tx.checkClosed(); err != nil {
		return false
	}

	if c.currentIter == nil {
		return false
	}

	c.currentIter.Next()
	return c.chooseIterator(true)
}

func (c *cursor) Prev() bool {
	if err := c.bucket.tx.checkClosed(); err != nil {
		return false
	}

	if c.currentIter == nil {
		return false
	}

	c.currentIter.Prev()
	return c.chooseIterator(false)
}

func (c *cursor) Seek(seek []byte) bool {
	if err := c.bucket.tx.checkClosed(); err != nil {
		return false
	}

	seekKey := bucketizedKey(c.bucket.id, seek)
	c.dbIter.Seek(seekKey)
	c.pendingIter.Seek(seekKey)
	return c.chooseIterator(true)
}

func (c *cursor) rawKey() []byte {
	if c.currentIter == nil {
		return nil
	}

	return copySlice(c.currentIter.Key())
}

func (c *cursor) Key() []byte {
	if err := c.bucket.tx.checkClosed(); err != nil {
		return nil
	}

	if c.currentIter == nil {
		return nil
	}

	key := c.currentIter.Key()
	if bytes.HasPrefix(key, bucketIndexPrefix) {
		key = key[len(bucketIndexPrefix)+4:]
		return copySlice(key)
	}

	key = key[len(c.bucket.id):]
	return copySlice(key)
}

func (c *cursor) rawValue() []byte {
	// Nothing to return if cursor is exhausted.
	if c.currentIter == nil {
		return nil
	}

	return copySlice(c.currentIter.Value())
}

func (c *cursor) Value() []byte {
	if err := c.bucket.tx.checkClosed(); err != nil {
		return nil
	}

	if c.currentIter == nil {
		return nil
	}

	if bytes.HasPrefix(c.currentIter.Key(), bucketIndexPrefix) {
		return nil
	}

	return copySlice(c.currentIter.Value())
}

type cursorType int

const (
	// ctKeys iterates through all of the keys in a given bucket.
	ctKeys cursorType = iota

	// ctBuckets iterates through all directly nested buckets in a given
	// bucket.
	ctBuckets

	// ctFull iterates through both the keys and the directly nested buckets
	// in a given bucket.
	ctFull
)

func cursorFinalizer(c *cursor) {
	c.dbIter.Release()
	c.pendingIter.Release()
}

// newCursor returns a new cursor for the given bucket, bucket ID, and cursor
// type.
func newCursor(b *bucket, bucketID []byte, cursorTyp cursorType) *cursor {
	var dbIter, pendingIter iterator.Iterator
	switch cursorTyp {
	case ctKeys:
		keyRange := util.BytesPrefix(bucketID)
		dbIter = b.tx.snapshot.NewIterator(keyRange)
		pendingKeyIter := newLdbTreapIter(b.tx, keyRange)
		pendingIter = pendingKeyIter
	case ctBuckets:
		// The serialized bucket index key format is:
		//   <bucketindexprefix><parentbucketid><bucketname>
		prefix := make([]byte, len(bucketIndexPrefix)+4)
		copy(prefix, bucketIndexPrefix)
		copy(prefix[len(bucketIndexPrefix):], bucketID)
		bucketRange := util.BytesPrefix(prefix)

		dbIter = b.tx.snapshot.NewIterator(bucketRange)
		pendingBucketIter := newLdbTreapIter(b.tx, bucketRange)
		pendingIter = pendingBucketIter
	case ctFull:
		fallthrough
	default:
		// The serialized bucket index key format is:
		//   <bucketindexprefix><parentbucketid><bucketname>
		prefix := make([]byte, len(bucketIndexPrefix)+4)
		copy(prefix, bucketIndexPrefix)
		copy(prefix[len(bucketIndexPrefix):], bucketID)
		bucketRange := util.BytesPrefix(prefix)
		keyRange := util.BytesPrefix(bucketID)

		// Since both keys and buckets are needed from the database,
		// create an individual iterator for each prefix and then create
		// a merged iterator from them.
		dbKeyIter := b.tx.snapshot.NewIterator(keyRange)
		dbBucketIter := b.tx.snapshot.NewIterator(bucketRange)
		iters := []iterator.Iterator{dbKeyIter, dbBucketIter}
		dbIter = iterator.NewMergedIterator(iters,
			comparer.DefaultComparer, true)

		// Since both keys and buckets are needed from the pending keys,
		// create an individual iterator for each prefix and then create
		// a merged iterator from them.
		pendingKeyIter := newLdbTreapIter(b.tx, keyRange)
		pendingBucketIter := newLdbTreapIter(b.tx, bucketRange)
		iters = []iterator.Iterator{pendingKeyIter, pendingBucketIter}
		pendingIter = iterator.NewMergedIterator(iters,
			comparer.DefaultComparer, true)
	}
	return &cursor{bucket: b, dbIter: dbIter, pendingIter: pendingIter}
}

type bucket struct {
	tx *transaction
	id [4]byte
}

var _ database.Bucket = (*bucket)(nil)

// 返回一个实际的key
// 通过key的格式来标记bucket
// <bucketindexprefix><parentbucketid><bucketname>
func bucketIndexKey(parentID [4]byte, key []byte) []byte {
	indexKey := make([]byte, len(bucketIndexPrefix)+4+len(key))
	copy(indexKey, bucketIndexPrefix)
	copy(indexKey[len(bucketIndexPrefix):], parentID[:])
	copy(indexKey[len(bucketIndexPrefix)+4:], key)
	return indexKey
}

// 返回一个实际的key
// block index key format : <bucketid><key>
func bucketizedKey(bucketID [4]byte, key []byte) []byte {
	bKey := make([]byte, 4+len(key))
	copy(bKey, bucketID[:])
	copy(bKey[4:], key)
	return bKey
}

func (b *bucket) Bucket(key []byte) database.Bucket {
	if err := b.tx.checkClosed(); err != nil {
		return nil
	}

	childID := b.tx.fetchKey(bucketIndexKey(b.id, key))
	if childID == nil {
		return nil
	}

	childBucket := &bucket{tx: b.tx}
	copy(childBucket.id[:], childID)
	return childBucket
}

// CreateBucket creates and returns a new nested bucket with the given key.
func (b *bucket) CreateBucket(key []byte) (database.Bucket, error) {
	if err := b.tx.checkClosed(); err != nil {
		return nil, err
	}

	// 确保transaction是可写的
	if !b.tx.writable {
		return nil, errors.New("create bucket requires a writable database transaction")
	}

	if len(key) == 0 {
		return nil, errors.New("create bucket requires a key")
	}

	bidxKey := bucketIndexKey(b.id, key)
	if b.tx.hasKey(bidxKey) {
		return nil, errors.New("bucket already exists")
	}

	// 找到一个新的Bucket ID
	var childID [4]byte
	if b.id == metadataBucketID && bytes.Equal(key, blockIdxBucketName) {
		childID = blockIdxBucketID
	} else {
		childID, err := b.tx.nextBucketID()
		if err != nil {
			return nil, err
		}
	}

	// 添加新bucket
	if err := b.tx.putKey(bidxKey, childID[:]); err != nil {
		str := fmt.Sprintf("failed to create bucket with key %q", key)
		return nil, errors.New(str)
	}
	return &bucket{tx: b.tx, id: childID}, nil
}

// CreateBucketIfNotExists creates and returns a new nested bucket with the
// given key if it does not already exist.
func (b *bucket) CreateBucketIfNotExists(key []byte) (database.Bucket, error) {
	if err := b.tx.checkClosed(); err != nil {
		return nil, err
	}

	if !b.tx.writable {
		return nil, errors.New("create bucket requires a writable database transaction")
	}

	if bucket := b.Bucket(key); bucket != nil {
		return bucket, nil
	}

	return b.CreateBucket(key)
}

// DeleteBucket removes a nested bucket with the given key.
func (b *bucket) DeleteBucket(key []byte) error {
	if err := b.tx.checkClosed(); err != nil {
		return err
	}

	if !b.tx.writable {
		return errors.New("delete bucket requires a writable database transaction")
	}

	bidxKey := bucketIndexKey(b.id, key)
	childID := b.tx.fetchKey(bidxKey)
	if childID == nil {
		return errors.New(fmt.Sprintf("bucket %q does not exist", key))
	}

	childIDs := [][]byte{childID}
	for len(childIDs) > 0 {
		childID = childIDs[len(childIDs)-1]
		childIDs = childIDs[:len(childIDs)-1]

		keyCursor := newCursor(b, childID, ctKeys)
		for ok := keyCursor.First(); ok; ok = keyCursor.Next() {
			b.tx.deleteKey(keyCursor.rawKey(), false)
		}
		cursorFinalizer(keyCursor)

		bucketCursor := newCursor(b, childID, ctBuckets)
		for ok := bucketCursor.First(); ok; ok = bucketCursor.Next() {
			childID := bucketCursor.rawValue()
			childIDs = append(childIDs, childID)

			b.tx.deleteKey(bucketCursor.rawKey(), false)
		}
		cursorFinalizer(bucketCursor)
	}

	b.tx.deleteKey(bidxKey, true)
	return nil
}

// return a new cursor
func (b *bucket) Cursor() database.Cursor {
	if err := b.tx.checkClosed(); err != nil {
		return &cursor{bucket: b}
	}

	c := newCursor(b, b.id[:], ctFull)
	runtime.SetFinalizer(c, cursorFinalizer)
	return c
}

func (b *bucket) ForEach(fn func(k, v []byte) error) error {
	if err := b.tx.checkClosed(); err != nil {
		return err
	}

	c := newCursor(b, b.id[:], ctKeys)
	defer cursorFinalizer(c)
	for ok := c.First(); ok; ok = c.Next() {
		err := fn(c.Key(), c.Value())
		if err != nil {
			return err
		}
	}

	return nil
}

func (b *bucket) ForEachBucket(fn func(k []byte) error) error {
	if err := b.tx.checkClosed(); err != nil {
		return err
	}

	c := newCursor(b, b.id[:], ctBuckets)
	for ok := c.First(); ok; ok = c.Next() {
		err := fn(c.Key())
		if err != nil {
			return err
		}
	}

	return nil
}

func (b *bucket) Writable() bool {
	return b.tx.writable
}

// put k/v 到bucket中
func (b *bucket) Put(key, value []byte) error {
	if err := b.tx.checkClosed(); err != nil {
		return err
	}

	if !b.tx.writable {
		return errors.New("setting a key requires a writable database transaction")
	}

	if len(key) == 0 {
		return errors.New("put requires a key")
	}

	return b.tx.putKey(bucketizedKey(b.id, key), value)
}

func (b *bucket) Get(key []byte) []byte {
	if err := b.tx.checkClosed(); err != nil {
		return nil
	}

	if len(key) == 0 {
		return nil
	}

	return b.tx.fetchKey(bucketizedKey(b.id, key))
}

func (b *bucket) Delete(key []byte) error {
	if err := b.tx.checkClosed(); err != nil {
		return err
	}

	if !b.tx.writable {
		return errors.New("deleting a value requires a writable database transaction")
	}

	if len(key) == 0 {
		return nil
	}

	b.tx.deleteKey(bucketizedKey(b.id, key), true)
	return nil
}

type pendingBlock struct {
	hash  *chainhash.Hash
	bytes []byte
}

type transaction struct {
	managed        bool             // transaction是否被db托管，托管状态的transaction不能主动Commit()或Rollback()
	closed         bool             // 指示当前transaction是否结束
	writable       bool             // 指示当前transaction是否可写
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

var _ database.Tx = (*transaction)(nil)

// 当transaction已经结束时返回一个错误
func (tx *transaction) checkClosed() error {
	if tx.closed {
		return errors.New("transaction is closed")
	}

	return nil
}

func (tx *transaction) notifyActiveIters() {
	tx.activeIterLock.RLock()
	for _, iter := range tx.activeIters {
		iter.ForceReseek()
	}
	tx.activeIterLock.RUnlock()
}

/*
未完成 snapshot
*/

func (tx *transaction) hasKey(key []byte) bool {
	if tx.writable {
		if tx.pendingRemove.Has(key) {
			return false
		}
		if tx.pendingKeys.Has(key) {
			return true
		}
	}

	return tx.snapshot.Has(key)
}

func (tx *transaction) fetchKey(key []byte) []byte {
	if tx.writable {
		if tx.pendingRemove.Has(key) {
			return nil
		}
		if value := tx.pendingKeys.Get(key); value != nil {
			return value
		}
	}

	return tx.snapshot.Get(key)
}

func (tx *transaction) putKey(key, value []byte) error {
	tx.pendingRemove.Delete(key)

	tx.pendingKeys.Put(key, value)
	tx.notifyActiveIters()

	return nil
}

func (tx *transaction) deleteKey(key []byte, notifyIterators bool) {
	tx.pendingKeys.Delete(key)

	tx.pendingRemove.Put(key, nil)

	if notifyIterators {
		tx.notifyActiveIters()
	}
}

func (tx *transaction) nextBucketID() ([4]byte, error) {
	curIDBytes := tx.fetchKey(curBucketIDKeyName)
	curBucketNum := binary.BigEndian.Uint32(curIDBytes)

	var nextBucketID [4]byte
	binary.BigEndian.PutUint32(nextBucketID[:], curBucketNum+1)
	if err := tx.putKey(curBucketIDKeyName, nextBucketID[:]); err != nil {
		return [4]byte{}, err
	}
	return nextBucketID, nil
}

func (tx *transaction) Metadata() database.Bucket {
	return tx.metaBucket
}

func (tx *transaction) close() {
	tx.closed = true

	tx.pendingBlocks = nil
	tx.pendingBLockData = nil

	tx.pendingKeys = nil
	tx.pendingRemove = nil

	if tx.snapshot != nil {
		tx.snapshot.Relese()
		tx.snapshot = nil
	}

	tx.db.closeLock.RUnlock()

	if tx.writable {
		tx.db.writeLock.Unlock()
	}
}

func (tx *transaction) writePendingAndCommit() error {
	wc := tx.db.store.writeCursor
	wc.RLock()
	oldBlkFileNUm := wc.curFileNum
	oldBlkOffset := wc.curOffset
	wc.RUnlock()

	rollback := func() {
		tx.db.store.handleRollback(oldBlkFileNUm, oldBlkOffset)
	}

	for _, blockData := range tx.pendingBLockData {
		log.Debug("Storing block %s", blockData.hash)
		location, err := tx.db.store.writeBlock(blockData.bytes)
		if err != nil {
			rollback()
			return err
		}

		blockRow := serializeBlockLoc(location)
		err = tx.blockIdxBucket.Put(blockData.hash[:], blockRow)
		if err != nil {
			rollback()
			return err
		}
	}

	writeRow := serializeWriteRow(wc.curFIleNum, wc.curOffset)
	if err := tx.metaBucket.Put(writeLocKeyNmae, writeRow); err != nil {
		rollback()
		return errors.New(fmt.Sprintf("failed to store write cursor %s", (err)))
	}

	return tx.db.cache.commitTx(tx)
}

func (tx *transaction) Commit() error {
	if tx.managed {
		tx.close()
		panic("managed transaction commit not allowed")
	}

	if err := tx.checkClosed(); err != nil {
		return err
	}

	defer tx.close()

	if !tx.writable {
		return errors.New("Commit requires a writable database transaction")
	}

	return tx.writePendingAndCommit()
}

func (tx *transaction) Rollback() error {
	if tx.managed {
		tx.close()
		panic("managed transaction rollback not allowed")
	}

	if err := tx.checkClosed(); err != nil {
		return err
	}

	tx.close()
	return nil
}

type db struct {
	writeLock sync.Mutex   // 保证同一时间只有一个可写transaction
	closeLock sync.RWMutex // 保证数据库关闭时所有transaction已经结束
	closed    bool         // 指示数据库是否已经关闭
	store     *blockStore  // 指向blockStore 用于读写区块
	cache     *dbCache     // 指向dbCache用来读写元数据
}

// db作为database.DB 的实例
var _ database.DB = (*db)(nil)

// 返回 database driver type
func (db *db) Type() string {
	return dbType
}

func (db *db) begin(writable bool) (*transaction, error) {
	if writable {
		db.writeLock.Lock()
	}

	db.closeLock.RLock()
	if db.closed {
		db.closeLock.RUnlock()
		if writable {
			db.writeLock.Unlock()
		}
		return nil, errors.New("database is not open")
	}

	snapshot, err := db.cache.Snapshot()
	if err != nil {
		db.closeLock.RUnlock()
		if writable {
			db.writeLock.Unlock()
		}

		return nil, err
	}

	tx := &transaction{
		writable:      writable,
		db:            db,
		snapshot:      snapshot,
		pendingKeys:   treap.NewMutable(),
		pendingRemove: treap.NewMutable(),
	}
	tx.metaBucket = &bucket{tx: tx, id: metadataBucketID}
	tx.blockIdxBucket = &bucket{tx: tx, id: blockIdxBucketID}
	return tx, nil
}

// start a transaction
func (db *db) Begin(writable bool) (database.Tx, error) {
	return db.begin(writable)
}

// 如果Panic则回滚transaction
func rollbackOnPanic(tx *transaction) {
	if err := recover(); err != nil {
		tx.managed = false
		_ = tx.Rollback()
		panic(err)
	}
}

func (db *db) View(fn func(database.Tx) error) error {
	// start a read-only transaction
	tx, err := db.begin(false)
	if err != nil {
		return err
	}

	defer rollbackOnPanic(tx)

	tx.managed = true
	err = fn(tx)
	tx.managed = false

	if err != nil {
		_ = tx.Rollback()
		return err
	}

	return tx.Rollback()
}

func (db *db) Update(fn func(database.Tx) error) error {
	tx, err := db.begin(true)
	if err != nil {
		return err
	}

	defer rollbackOnPanic(tx)

	tx.managed = true
	err = fn(tx)
	tx.managed = false
	if err != nil {
		_ = tx.Rollback()
		return err
	}

	return tx.Commit()
}

func (db *db) Close() error {
	db.closeLock.Lock()
	defer db.closeLock.Unlock()

	if db.closed {
		return errors.New("database is not open")
	}
	db.closed = true

	closeErr := db.cache.Close()

	wc := db.store.writeCursor
	if wc.curFile.file != nil {
		_ = wc.curFile.file.Close()
		wc.curFile.file = nil
	}
	for _, blockFile := range db.store.openBlockFiles {
		_ = blockFile.file.Close()
	}

	db.store.openBlockFiles = nil
	db.store.openBlocksLRU.Init()
	db.store.fileNumToLRUElem = nil

	return closeErr
}

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
	// batch := new(leveldb.Batch)
	// batch.Put(bucketizedKey(metadataBucketID))

	return nil
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

	return reconcileDB(pdb, create)
}
