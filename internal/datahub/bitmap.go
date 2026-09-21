package datahub

// PartBitmap records which parts of one Upload have been registered. Bit n-1
// stands for part n, so a 10 000-part upload costs 1 250 bytes.
type PartBitmap []byte

// NewPartBitmap returns an empty bitmap sized for totalParts.
func NewPartBitmap(totalParts int) PartBitmap {
	size := (totalParts + 7) / 8
	if size < 1 {
		size = 1
	}
	return make(PartBitmap, size)
}

// Set marks partNumber as registered. Part numbers start at 1.
func (b PartBitmap) Set(partNumber int) {
	if partNumber < 1 || b == nil {
		return
	}
	index := partNumber - 1
	byteIndex, bitIndex := index/8, index%8
	if byteIndex < len(b) {
		b[byteIndex] |= 1 << bitIndex
	}
}

// IsSet reports whether partNumber has been registered.
func (b PartBitmap) IsSet(partNumber int) bool {
	if partNumber < 1 || b == nil {
		return false
	}
	index := partNumber - 1
	byteIndex, bitIndex := index/8, index%8
	if byteIndex >= len(b) {
		return false
	}
	return b[byteIndex]&(1<<bitIndex) != 0
}

// Count returns how many parts have been registered.
func (b PartBitmap) Count() int {
	count := 0
	for _, value := range b {
		for value != 0 { // Brian Kernighan: one iteration per set bit.
			value &= value - 1
			count++
		}
	}
	return count
}

// Bytes returns a copy suitable for storing.
func (b PartBitmap) Bytes() []byte {
	if b == nil {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}

// Clone returns an independent copy.
func (b PartBitmap) Clone() PartBitmap {
	return PartBitmapFromBytes(b)
}

// PartBitmapFromBytes rebuilds a bitmap from its stored bytes.
func PartBitmapFromBytes(data []byte) PartBitmap {
	if data == nil {
		return nil
	}
	out := make(PartBitmap, len(data))
	copy(out, data)
	return out
}
