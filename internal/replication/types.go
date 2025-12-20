package replication

type WriteConsistency int

const (
	WriteConsistencyUnspecified WriteConsistency = iota
	WriteConsistencyFast
	WriteConsistencyStrong
)
