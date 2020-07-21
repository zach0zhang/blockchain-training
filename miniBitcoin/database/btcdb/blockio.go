package btcdb

import (
	"container/list"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
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

func blockFilePath(dbPath string, fileNum uint32) string {
	fileName := fmt.Sprintf(blockFilenameTemplate, fileNum)
	return filepath.Join(dbPath, fileName)
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
	log.Debug("Scan found latest block file #%d with length %d", lastFile, fileLen)
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
	store.openWriteFileFunc = store.openWriteFileFunc
	store.deleteFileFunc = store.deleteFile
	return store
}
