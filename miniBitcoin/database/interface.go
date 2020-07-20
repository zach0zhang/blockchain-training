package database

type Bucket interface {
	Bucket(key []byte) Bucket
}

type Tx interface {
	Metadata() Bucket
}

type DB interface {
	Type() string
}
