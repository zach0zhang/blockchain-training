package database

type Cursor interface {
	Bucket() Bucket

	Delete() error

	Fitst() bool

	Last() bool

	Next() bool

	Seek(seek []byte) bool

	Key() []byte

	Value() []byte
}

type Bucket interface {
	Bucket(key []byte) Bucket

	CreateBucket(key []byte) (Bucket, error)

	CreateBucketIfNotExists(key []byte) (Bucket, error)

	DeleteBucket(key []byte) error

	ForEach(func(k, v []byte) error) error

	ForEachBucket(func(k []byte) error) error

	Cursor() Cursor

	Writable() bool

	Put(key, value []byte) error

	Get(key []byte) []byte

	Delete(key []byte) error
}

type Tx interface {
	Metadata() Bucket
}

type DB interface {
	Type() string

	Begin(writable bool) (Tx, error)

	View(fn func(tx Tx) error) error

	Update(fn func(tx Tx) error) error

	Close() error
}
