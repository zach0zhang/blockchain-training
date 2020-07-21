package btcdb

import "miniBitcoin/database"

func reconcileDB(pdb *db, create bool) (database.DB, error) {
	if create {
		if err := initDB(pdb.cache.ldb); err != nil {
			return nil, err
		}
	}
}
