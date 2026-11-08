package soroi

import (
	"encoding/binary"
	"hash/maphash"
	"math"
	"reflect"
)

// hashBucketKey retains Go equality for non-string keys without unsafe or private runtime hashing.
// Invalid keys may route to any bucket; admission rejects them before map access.
func hashBucketKey(seed maphash.Seed, key any) uint64 {
	var hash maphash.Hash
	hash.SetSeed(seed)
	hashBucketValue(&hash, reflect.ValueOf(key))
	return hash.Sum64()
}

// hashBucketValue encodes equality-relevant values; the underlying map resolves hash collisions.
func hashBucketValue(hash *maphash.Hash, value reflect.Value) {
	switch value.Kind() {
	case reflect.Interface:
		if !value.IsNil() {
			hashBucketValue(hash, value.Elem())
		}
	case reflect.String:
		_, _ = hash.WriteString(value.String())
	case reflect.Bool:
		if value.Bool() {
			writeBucketWord(hash, 1)
		} else {
			writeBucketWord(hash, 0)
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		// The signed representation preserves equality, including negative values.
		writeBucketWord(hash, uint64(value.Int())) //nolint:gosec
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		writeBucketWord(hash, value.Uint())
	case reflect.Pointer, reflect.Chan, reflect.UnsafePointer:
		writeBucketWord(hash, uint64(value.Pointer()))
	case reflect.Float32, reflect.Float64:
		writeBucketFloat(hash, value.Float())
	case reflect.Complex64, reflect.Complex128:
		number := value.Complex()
		writeBucketFloat(hash, real(number))
		writeBucketFloat(hash, imag(number))
	case reflect.Array:
		for i := 0; i < value.Len(); i++ {
			hashBucketValue(hash, value.Index(i))
		}
	case reflect.Struct:
		for i := 0; i < value.NumField(); i++ {
			// Go equality ignores blank fields regardless of their stored bits.
			if value.Type().Field(i).Name != "_" {
				hashBucketValue(hash, value.Field(i))
			}
		}
	}
}

// writeBucketWord adds scalar bits without allocating a per-key buffer.
func writeBucketWord(hash *maphash.Hash, value uint64) {
	var bytes [8]byte
	binary.LittleEndian.PutUint64(bytes[:], value)
	_, _ = hash.Write(bytes[:])
}

// writeBucketFloat canonicalizes signed zero because Go compares both representations as equal.
func writeBucketFloat(hash *maphash.Hash, value float64) {
	if value == 0 {
		value = 0
	}
	writeBucketWord(hash, math.Float64bits(value))
}
