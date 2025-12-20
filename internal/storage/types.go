package storage

type Entry struct {
	Value               []byte
	Version             uint64
	TimestampUnixMillis int64
	Tombstone           bool
}
