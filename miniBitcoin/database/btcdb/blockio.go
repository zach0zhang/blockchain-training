package btcdb

import (
	"container/list"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/btcsuite/btcd/chaincfg/chainhash"
)

const (
	// block数量最多为 2^31, 每个block最大32MB
	// 理论最大值为64PB
	// 每个文件最大值为512MB，则需要最大为 134,217,782个文件
	// 使用9位最多可以表示10^9=1,000,000,000个文件 ～ 476.84PB
	blockFilenameTemplate = "%09d.fdb"

	maxOpenFiles = 25

	maxBlockFileSize uint32 = 512 * 1024 * 1024 // 512MB

	// [0:4] Block file (4 bytes)
	// [4:8] File offset (4 bytes)
	// [8:12] Block length
	blockLocSize = 12
)

var (
	castagnoli = crc32.MakeTable(crc32.Castagnoli)
)

type filer interface {
	io.Closer
	io.WriterAt
	io.ReaderAt
	Truncate(size int64) error
	Sync() error
}

type lockableFile struct {
	sync.RWMutex
	file filer
}

type writeCursor struct {
	sync.RWMutex

	curFile *lockableFile

	curFileNum uint32

	curOffset uint32
}

type writerCursor struct {
	sync.RWMutex

	curFile *lockableFile

	curFileNum uint32

	curOffeset uint32
}

type blockStore struct {
	basePath string

	maxBlockFileSize uint32

	obfMutex         sync.RWMutex
	lruMutex         sync.Mutex
	openBlocksLRU    *list.List
	fileNumToLRUElem map[uint32]*list.Element
	openBlockFiles   map[uint32]*lockableFile

	writeCursor *writeCursor

	openFileFunc      func(fileNum uint32) (*lockableFile, error)
	openWriteFileFunc func(fileNum uint32) (filer, error)
	deleteFileFunc    func(fileNum uint32) error
}

type blockLocation struct {
	blockFileNum uint32
	fileOffset   uint32
	blockLen     uint32
}

func deserializeBlockLoc(serializedLoc []byte) blockLocation {
	// The serialized block location format is:
	//
	//  [0:4]  Block file (4 bytes)
	//  [4:8]  File offset (4 bytes)
	//  [8:12] Block length (4 bytes)
	return blockLocation{
		blockFileNum: byteOrder.Uint32(serializedLoc[0:4]),
		fileOffset:   byteOrder.Uint32(serializedLoc[4:8]),
		blockLen:     byteOrder.Uint32(serializedLoc[8:12]),
	}
}

func serializeBlockLoc(loc blockLocation) []byte {
	// The serialized block location format is:
	//
	//  [0:4]  Block file (4 bytes)
	//  [4:8]  File offset (4 bytes)
	//  [8:12] Block length (4 bytes)
	var serializedData [12]byte
	byteOrder.PutUint32(serializedData[0:4], loc.blockFileNum)
	byteOrder.PutUint32(serializedData[4:8], loc.fileOffset)
	byteOrder.PutUint32(serializedData[8:12], loc.blockLen)
	return serializedData[:]
}

func blockFilePath(dbPath string, fileNum uint32) string {
	fileName := fmt.Sprintf(blockFilenameTemplate, fileNum)
	return filepath.Join(dbPath, fileName)
}

// openWriteFile 返回可读可写文件句柄，若文件不存在则创建
func (s *blockStore) openWriteFile(fileNum uint32) (filer, error) {
	filePath := blockFilePath(s.basePath, fileNum)
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	return file, nil
}

// openFile 返回 只读 的文件句柄
func (s *blockStore) openFile(fileNum uint32) (*lockableFile, error) {
	filePath := blockFilePath(s.basePath, fileNum)
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	blockFile := &lockableFile{file: file}

	// 如果文件超出了最大打开的数量，则关闭最近最少使用的文件
	// 操作LRU列表需要加锁
	// 添加刚才打开的文件到LRU列表头(表示刚打开的文件，当然也最后关闭)
	s.lruMutex.Lock()
	lruList := s.openBlocksLRU
	if lruList.Len() >= maxOpenFiles {
		lruFileNum := lruList.Remove(lruList.Back()).(uint32)
		oldBlockFile := s.openBlockFiles[lruFileNum]

		oldBlockFile.Lock()
		_ = oldBlockFile.file.Close()
		oldBlockFile.Unlock()

		delete(s.openBlockFiles, lruFileNum)
		delete(s.fileNumToLRUElem, lruFileNum)
	}
	s.fileNumToLRUElem[fileNum] = lruList.PushFront(fileNum)
	s.lruMutex.Unlock()

	s.openBlockFiles[fileNum] = blockFile

	return blockFile, nil
}

func (s *blockStore) deleteFile(fileNum uint32) error {
	filePath := blockFilePath(s.basePath, fileNum)
	if err := os.Remove(filePath); err != nil {
		return err
	}

	return nil
}

func (s *blockStore) blockFile(fileNum uint32) (*lockableFile, error) {
	// When the requested block file is open for writes, return it.
	wc := s.writeCursor
	wc.RLock()
	if fileNum == wc.curFileNum && wc.curFile.file != nil {
		obf := wc.curFile
		obf.RLock()
		wc.RUnlock()
		return obf, nil
	}
	wc.RUnlock()

	// Try to return an open file under the overall files read lock.
	s.obfMutex.RLock()
	if obf, ok := s.openBlockFiles[fileNum]; ok {
		s.lruMutex.Lock()
		s.openBlocksLRU.MoveToFront(s.fileNumToLRUElem[fileNum])
		s.lruMutex.Unlock()

		obf.RLock()
		s.obfMutex.RUnlock()
		return obf, nil
	}
	s.obfMutex.RUnlock()

	s.obfMutex.Lock()
	if obf, ok := s.openBlockFiles[fileNum]; ok {
		obf.RLock()
		s.obfMutex.Unlock()
		return obf, nil
	}

	// The file isn't open, so open it while potentially closing the least
	// recently used one as needed.
	obf, err := s.openFileFunc(fileNum)
	if err != nil {
		s.obfMutex.Unlock()
		return nil, err
	}
	obf.RLock()
	s.obfMutex.Unlock()
	return obf, nil
}

func (s *blockStore) writeData(data []byte, fieldName string) error {
	wc := s.writeCursor
	n, err := wc.curFile.file.WriteAt(data, int64(wc.curOffset))
	wc.curOffset += uint32(n)
	if err != nil {
		str := fmt.Sprintf("failed to write %s to file %d at "+
			"offset %d: %v", fieldName, wc.curFileNum, wc.curOffset-uint32(n), err)
		return errors.New(str)
	}

	return nil
}

// writeBlock appends the specified raw block bytes to the store's write cursor
// location and increments it accordingly.  When the block would exceed the max
// file size for the current flat file, this function will close the current
// file, create the next file, update the write cursor, and write the block to
// the new file.
//
// The write cursor will also be advanced the number of bytes actually written
// in the event of failure.
//
// Format: <block length><serialized block><checksum>
func (s *blockStore) writeBlock(rawBlock []byte) (blockLocation, error) {
	blockLen := uint32(len(rawBlock))
	fullLen := blockLen + 8

	wc := s.writeCursor
	finalOffset := wc.curOffset + fullLen
	if finalOffset < wc.curOffset || finalOffset > s.maxBlockFileSize {
		wc.Lock()
		wc.curFile.Lock()
		if wc.curFile.file != nil {
			_ = wc.curFile.file.Close()
			wc.curFile.file = nil
		}
		wc.curFile.Unlock()

		wc.curFileNum++
		wc.curOffset = 0
		wc.Unlock()
	}

	wc.curFile.Lock()
	defer wc.curFile.Unlock()

	if wc.curFile.file == nil {
		file, err := s.openWriteFileFunc(wc.curFileNum)
		if err != nil {
			return blockLocation{}, err
		}
		wc.curFile.file = file
	}

	origOffset := wc.curOffset
	hasher := crc32.New(castagnoli)
	var scratch [4]byte

	// Block length
	byteOrder.PutUint32(scratch[:], blockLen)
	if err := s.writeData(scratch[:], "block length"); err != nil {
		return blockLocation{}, err
	}
	_, _ = hasher.Write(scratch[:])

	// Serialized block
	if err := s.writeData(rawBlock[:], "block"); err != nil {
		return blockLocation{}, err
	}
	_, _ = hasher.Write(rawBlock)

	// checksum
	if err := s.writeData(hasher.Sum(nil), "checksum"); err != nil {
		return blockLocation{}, err
	}

	loc := blockLocation{
		blockFileNum: wc.curFileNum,
		fileOffset:   origOffset,
		blockLen:     fullLen,
	}
	return loc, nil
}

// readBlock reads the specified block record and returns the serialized block.
// It ensures the integrity of the block data by checking that the serialized
// network matches the current network associated with the block store and
// comparing the calculated checksum against the one stored in the flat file.
// This function also automatically handles all file management such as opening
// and closing files as necessary to stay within the maximum allowed open files
// limit.
//
// Returns ErrDriverSpecific if the data fails to read for any reason and
// ErrCorruption if the checksum of the read data doesn't match the checksum
// read from the file.
//
// Format: <block length><serialized block><checksum>
func (s *blockStore) readBlock(hash *chainhash.Hash, loc blockLocation) ([]byte, error) {
	blockFile, err := s.blockFile(loc.blockFileNum)
	if err != nil {
		return nil, err
	}

	serializedData := make([]byte, loc.blockLen)
	n, err := blockFile.file.ReadAt(serializedData, int64(loc.fileOffset))
	blockFile.RUnlock()
	if err != nil {
		str := fmt.Sprintf("failed to read block %s from file %d, "+
			"offset %d: %v", hash, loc.blockFileNum, loc.fileOffset, err)
		return nil, errors.New(str)
	}

	serializedChecksum := binary.BigEndian.Uint32(serializedData[n-4:])
	calculatedChecksum := crc32.Checksum(serializedData[:n-4], castagnoli)
	if serializedChecksum != calculatedChecksum {
		str := fmt.Sprintf("block data for block %s checksum "+
			"does not match - got %x, want %x", hash, calculatedChecksum, serializedChecksum)
		return nil, errors.New(str)
	}

	return serializedData[4 : n-4], nil
}

func (s *blockStore) readBlockRegion(loc blockLocation, offset, numBytes uint32) ([]byte, error) {
	blockFile, err := s.blockFile(loc.blockFileNum)
	if err != nil {
		return nil, err
	}

	readOffset := loc.fileOffset + 4 + offset
	serializedData := make([]byte, numBytes)
	_, err = blockFile.file.ReadAt(serializedData, int64(readOffset))
	blockFile.RUnlock()
	if err != nil {
		str := fmt.Sprintf("failed to read region from block file %d, "+
			"offset %d, len %d: %v", loc.blockFileNum, readOffset, numBytes, err)
		return nil, errors.New(str)
	}

	return serializedData, nil
}

func (s *blockStore) syncBlocks() error {
	wc := s.writeCursor
	wc.RLock()
	defer wc.RUnlock()

	wc.curFile.RLock()
	defer wc.curFile.RUnlock()
	if wc.curFile.file == nil {
		return nil
	}

	if err := wc.curFile.file.Sync(); err != nil {
		str := fmt.Sprintf("failed to sync file %d: %v", wc.curFileNum, err)
		return errors.New(str)
	}

	return nil
}

func (s *blockStore) handleRollback(oldBlockFileNum, oldBlockOffset uint32) {
	// Grab the write cursor mutex since it is modified throughout this
	// function.
	wc := s.writeCursor
	wc.Lock()
	defer wc.Unlock()

	// Nothing to do if the rollback point is the same as the current write
	// cursor.
	if wc.curFileNum == oldBlockFileNum && wc.curOffset == oldBlockOffset {
		return
	}

	// Regardless of any failures that happen below, reposition the write
	// cursor to the old block file and offset.
	defer func() {
		wc.curFileNum = oldBlockFileNum
		wc.curOffset = oldBlockOffset
	}()

	log.Debug("ROLLBACK: Rolling back to file %d, offset %d",
		oldBlockFileNum, oldBlockOffset)

	// Close the current write file if it needs to be deleted.  Then delete
	// all files that are newer than the provided rollback file while
	// also moving the write cursor file backwards accordingly.
	if wc.curFileNum > oldBlockFileNum {
		wc.curFile.Lock()
		if wc.curFile.file != nil {
			_ = wc.curFile.file.Close()
			wc.curFile.file = nil
		}
		wc.curFile.Unlock()
	}
	for ; wc.curFileNum > oldBlockFileNum; wc.curFileNum-- {
		if err := s.deleteFileFunc(wc.curFileNum); err != nil {
			log.Warning("ROLLBACK: Failed to delete block file "+
				"number %d: %v", wc.curFileNum, err)
			return
		}
	}

	// Open the file for the current write cursor if needed.
	wc.curFile.Lock()
	if wc.curFile.file == nil {
		obf, err := s.openWriteFileFunc(wc.curFileNum)
		if err != nil {
			wc.curFile.Unlock()
			log.Warning("ROLLBACK: %v", err)
			return
		}
		wc.curFile.file = obf
	}

	// Truncate the to the provided rollback offset.
	if err := wc.curFile.file.Truncate(int64(oldBlockOffset)); err != nil {
		wc.curFile.Unlock()
		log.Warning(fmt.Sprintf("ROLLBACK: Failed to truncate file %d: %v",
			wc.curFileNum, err))
		return
	}

	// Sync the file to disk.
	err := wc.curFile.file.Sync()
	wc.curFile.Unlock()
	if err != nil {
		log.Warning(fmt.Sprintf("ROLLBACK: Failed to sync file %d: %v",
			wc.curFileNum, err))
		return
	}
}

func scanBlockFiles(dbPath string) (int, uint32) {
	lastFile := -1
	fileLen := uint32(0)
	for i := 0; ; i++ {
		filePath := blockFilePath(dbPath, uint32(i))
		st, err := os.Stat(filePath)
		if err != nil {
			break
		}
		lastFile = i
		fileLen = uint32(st.Size())
	}
	log.Debug("Scan found latest block file", lastFile, "with length", fileLen)
	return lastFile, fileLen
}

func newBlockStore(basePath string) *blockStore {
	fileNum, fileOff := scanBlockFiles(basePath)
	if fileNum == -1 {
		fileNum = 0
		fileOff = 0
	}

	store := &blockStore{
		basePath:         basePath,
		maxBlockFileSize: maxBlockFileSize,
		openBlockFiles:   make(map[uint32]*lockableFile),
		openBlocksLRU:    list.New(),
		fileNumToLRUElem: make(map[uint32]*list.Element),

		writeCursor: &writeCursor{
			curFile:    &lockableFile{},
			curFileNum: uint32(fileNum),
			curOffset:  fileOff,
		},
	}

	store.openFileFunc = store.openFile
	store.openWriteFileFunc = store.openWriteFile
	store.deleteFileFunc = store.deleteFile
	return store
}
