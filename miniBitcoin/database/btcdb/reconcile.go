package btcdb

func reconcileDB(pdb *db, create bool) (database.DB, error) {
	if create {
		if err := initDB(pdb.cache.ldb)
	}
}