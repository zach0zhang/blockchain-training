package btcdb

import (
	"errors"
	"fmt"
	"hash/crc32"
	"miniBitcoin/database"
)

// The serialized write cursor location format is:
//
//  [0:4]  Block file (4 bytes)
//  [4:8]  File offset (4 bytes)
//  [8:12] Castagnoli CRC-32 checksum (4 bytes)

// serializeWriteRow serialize the current block file and offset where new
// will be written into a format suitable for storage into the metadata.
func serializeWriteRow(curBlockFileNum, curFileOffset uint32) []byte {
	var serializedRow [12]byte
	byteOrder.PutUint32(serializedRow[0:4], curBlockFileNum)
	byteOrder.PutUint32(serializedRow[4:8], curFileOffset)
	checksum := crc32.Checksum(serializedRow[:8], castagnoli)
	byteOrder.PutUint32(serializedRow[8:12], checksum)
	return serializedRow[:]
}

// deserializeWriteRow deserializes the write cursor location stored in the
// metadata.  Returns ErrCorruption if the checksum of the entry doesn't match.
func deserializeWriteRow(writeRow []byte) (uint32, uint32, error) {
	// Ensure the checksum matches.  The checksum is at the end.
	gotChecksum := crc32.Checksum(writeRow[:8], castagnoli)
	wantChecksumBytes := writeRow[8:12]
	wantChecksum := byteOrder.Uint32(wantChecksumBytes)
	if gotChecksum != wantChecksum {
		str := fmt.Sprintf("metadata for write cursor does not match "+
			"the expected checksum - got %d, want %d", gotChecksum,
			wantChecksum)
		return 0, 0, errors.New(str)
	}

	fileNum := byteOrder.Uint32(writeRow[0:4])
	fileOffset := byteOrder.Uint32(writeRow[4:8])
	return fileNum, fileOffset, nil
}

func reconcileDB(pdb *db, create bool) (database.DB, error) {
	if create {
		if err := initDB(pdb.cache.ldb); err != nil {
			return nil, err
		}
	}

	var curFileNum, curOffset uint32
	err := pdb.View(func(tx database.Tx) error {
		writeRow := tx.Metadata().Get(writeLocKeyNmae)
		if writeRow == nil {
			errors.New("write cursor does not exist")
		}

		var err error
		curFileNum, curOffset, err = deserializeWriteRow(writeRow)
		return err
	})
	if err != nil {
		return nil, err
	}

	wc := pdb.store.writeCursor
	if wc.curFileNum > curFileNum || (wc.curFileNum == curFileNum && wc.curOffset > curOffset) {
		log.Info("Detected unclean shutdown - Repairing...")
		log.Debug(fmt.Sprintf("Metadata claims file %d, offset %d. Block data is at file %d, offset %d", curFileNum,
			curOffset, wc.curFileNum, wc.curOffset))
		pdb.store.handleRollback(curFileNum, curOffset)
		log.Info("Database sync complete")
	}

	if wc.curFileNum < curFileNum || (wc.curFileNum == curFileNum && wc.curOffset < curOffset) {
		str := fmt.Sprintf("metadata claim file %d, offset %d, but "+
			"block data is at file %d, offset %d", curFileNum,
			curOffset, wc.curFileNum, wc.curOffset)
		log.Warning(fmt.Sprintf("***Database corruption detected***: %v", str))
		return nil, errors.New(str)
	}

	return pdb, nil
}
