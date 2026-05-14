// Package attribute is a minimal stub of go.opentelemetry.io/otel/attribute
// for analysistest. The attrkey analyzer matches the import path, so the
// only requirement here is that the path resolves and the constructors have
// the expected names + signatures. Real OTel ships a much richer API.
package attribute

type Key string

type Value struct{ s string }

type KeyValue struct {
	Key   Key
	Value Value
}

func String(k, v string) KeyValue  { return KeyValue{Key: Key(k), Value: Value{s: v}} }
func Int(k string, v int) KeyValue { return KeyValue{Key: Key(k)} }
