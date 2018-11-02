package ydb

import "hash/fnv"

// For consistent hashing
func StoreHash(key string) uint32 {
	hasher := fnv.New32()
	hasher.Write([]byte(key))
	return hasher.Sum32()
}
