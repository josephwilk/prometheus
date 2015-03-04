// Copyright 2014 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package local

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"sort"

	clientmodel "github.com/prometheus/client_golang/model"

	"github.com/prometheus/prometheus/storage/metric"
)

// The 37-byte header of a delta-encoded chunk looks like:
//
// - time double-delta bytes:  1 bytes
// - value double-delta bytes: 1 bytes
// - is integer:               1 byte
// - base time:                8 bytes
// - base value:               8 bytes
// - base time delta:          8 bytes
// - base value delta:         8 bytes
// - used buf bytes:           2 bytes
const (
	doubleDeltaHeaderBytes = 37

	doubleDeltaHeaderTimeBytesOffset      = 0
	doubleDeltaHeaderValueBytesOffset     = 1
	doubleDeltaHeaderIsIntOffset          = 2
	doubleDeltaHeaderBaseTimeOffset       = 3
	doubleDeltaHeaderBaseValueOffset      = 11
	doubleDeltaHeaderBaseTimeDeltaOffset  = 19
	doubleDeltaHeaderBaseValueDeltaOffset = 27
	doubleDeltaHeaderBufLenOffset         = 35
)

// A doubleDeltaEncodedChunk adaptively stores sample timestamps and values with
// a double-delta encoding of various types (int, float) and bit widths. A base
// value and timestamp and a base delta for each is saved in the header. The
// payload consists of double-deltas, i.e. deviations from the values and
// timestamps calculated by applying the base value and time and the base deltas.
// However, once 8 bytes would be needed to encode a double-delta value, a
// fall-back to the absolute numbers happens (so that timestamps are saved
// directly as int64 and values as float64).
// doubleDeltaEncodedChunk implements the chunk interface.
type doubleDeltaEncodedChunk struct {
	buf []byte
}

// newDoubleDeltaEncodedChunk returns a newly allocated doubleDeltaEncodedChunk.
func newDoubleDeltaEncodedChunk(tb, vb deltaBytes, isInt bool) *doubleDeltaEncodedChunk {
	buf := make([]byte, doubleDeltaHeaderIsIntOffset+1, 1024)

	buf[doubleDeltaHeaderTimeBytesOffset] = byte(tb)
	buf[doubleDeltaHeaderValueBytesOffset] = byte(vb)
	if vb < d8 && isInt { // Only use int for fewer than 8 value double-delta bytes.
		buf[deltaHeaderIsIntOffset] = 1
	} else {
		buf[deltaHeaderIsIntOffset] = 0
	}

	return &doubleDeltaEncodedChunk{
		buf: buf,
	}
}

func (c *doubleDeltaEncodedChunk) newFollowupChunk() chunk {
	return newDeltaEncodedChunk(d1, d0, true)
}

// clone implements chunk.
func (c *doubleDeltaEncodedChunk) clone() chunk {
	buf := make([]byte, len(c.buf), 1024)
	copy(buf, c.buf)
	return &doubleDeltaEncodedChunk{
		buf: buf,
	}
}

func (c *doubleDeltaEncodedChunk) timeBytes() deltaBytes {
	return deltaBytes(c.buf[doubleDeltaHeaderTimeBytesOffset])
}

func (c *doubleDeltaEncodedChunk) valueBytes() deltaBytes {
	return deltaBytes(c.buf[doubleDeltaHeaderValueBytesOffset])
}

func (c *doubleDeltaEncodedChunk) isInt() bool {
	return c.buf[doubleDeltaHeaderIsIntOffset] == 1
}

func (c *doubleDeltaEncodedChunk) baseTime() clientmodel.Timestamp {
	return clientmodel.Timestamp(binary.LittleEndian.Uint64(c.buf[doubleDeltaHeaderBaseTimeOffset:]))
}

func (c *doubleDeltaEncodedChunk) baseValue() clientmodel.SampleValue {
	return clientmodel.SampleValue(math.Float64frombits(binary.LittleEndian.Uint64(c.buf[doubleDeltaHeaderBaseValueOffset:])))
}

// add implements chunk.
func (c *doubleDeltaEncodedChunk) add(s *metric.SamplePair) []chunk {
	if len(c.buf) <= doubleDeltaHeaderIsIntOffset+1 {
		c.buf = c.buf[:doubleDeltaHeaderBaseValueOffset+8]
		binary.LittleEndian.PutUint64(c.buf[deltaHeaderBaseTimeOffset:], uint64(s.Timestamp))
		binary.LittleEndian.PutUint64(c.buf[deltaHeaderBaseValueOffset:], math.Float64bits(float64(s.Value)))
	}

	remainingBytes := cap(c.buf) - len(c.buf)
	sampleSize := c.sampleSize()

	// Do we generally have space for another sample in this chunk? If not,
	// overflow into a new one.
	if remainingBytes < sampleSize {
		overflowChunks := c.newFollowupChunk().add(s)
		return []chunk{c, overflowChunks[0]}
	}

	dt := s.Timestamp - c.baseTime()
	dv := s.Value - c.baseValue()
	tb := c.timeBytes()
	vb := c.valueBytes()

	// If the new sample is incompatible with the current encoding, reencode the
	// existing chunk data into new chunk(s).
	//
	// int->float.
	// Note: Using math.Modf is slower than the conversion approach below.
	if c.isInt() && clientmodel.SampleValue(int64(dv)) != dv {
		return transcodeAndAdd(newDeltaEncodedChunk(tb, d4, false), c, s)
	}
	// float32->float64.
	if !c.isInt() && vb == d4 && clientmodel.SampleValue(float32(dv)) != dv {
		return transcodeAndAdd(newDeltaEncodedChunk(tb, d8, false), c, s)
	}
	if tb < d8 || vb < d8 {
		// Maybe more bytes per sample.
		if ntb, nvb := neededDeltaBytes(dt, dv, c.isInt()); ntb > tb || nvb > vb {
			ntb = max(ntb, tb)
			nvb = max(nvb, vb)
			return transcodeAndAdd(newDeltaEncodedChunk(ntb, nvb, c.isInt()), c, s)
		}
	}
	offset := len(c.buf)
	c.buf = c.buf[:offset+sampleSize]

	switch tb {
	case d1:
		c.buf[offset] = byte(dt)
	case d2:
		binary.LittleEndian.PutUint16(c.buf[offset:], uint16(dt))
	case d4:
		binary.LittleEndian.PutUint32(c.buf[offset:], uint32(dt))
	case d8:
		// Store the absolute value (no delta) in case of d8.
		binary.LittleEndian.PutUint64(c.buf[offset:], uint64(s.Timestamp))
	default:
		panic("invalid number of bytes for time delta")
	}

	offset += int(tb)

	if c.isInt() {
		switch vb {
		case d0:
			// No-op. Constant value is stored as base value.
		case d1:
			c.buf[offset] = byte(dv)
		case d2:
			binary.LittleEndian.PutUint16(c.buf[offset:], uint16(dv))
		case d4:
			binary.LittleEndian.PutUint32(c.buf[offset:], uint32(dv))
		// d8 must not happen. Those samples are encoded as float64.
		default:
			panic("invalid number of bytes for integer delta")
		}
	} else {
		switch vb {
		case d4:
			binary.LittleEndian.PutUint32(c.buf[offset:], math.Float32bits(float32(dv)))
		case d8:
			// Store the absolute value (no delta) in case of d8.
			binary.LittleEndian.PutUint64(c.buf[offset:], math.Float64bits(float64(s.Value)))
		default:
			panic("invalid number of bytes for floating point delta")
		}
	}
	return []chunk{c}
}

func (c *doubleDeltaEncodedChunk) sampleSize() int {
	return int(c.timeBytes() + c.valueBytes())
}

func (c *doubleDeltaEncodedChunk) len() int {
	if len(c.buf) < deltaHeaderBytes {
		return 0
	}
	return (len(c.buf) - deltaHeaderBytes) / c.sampleSize()
}

// values implements chunk.
func (c *doubleDeltaEncodedChunk) values() <-chan *metric.SamplePair {
	n := c.len()
	valuesChan := make(chan *metric.SamplePair)
	go func() {
		for i := 0; i < n; i++ {
			valuesChan <- c.valueAtIndex(i)
		}
		close(valuesChan)
	}()
	return valuesChan
}

func (c *doubleDeltaEncodedChunk) valueAtIndex(idx int) *metric.SamplePair {
	offset := deltaHeaderBytes + idx*c.sampleSize()

	var ts clientmodel.Timestamp
	switch c.timeBytes() {
	case d1:
		ts = c.baseTime() + clientmodel.Timestamp(uint8(c.buf[offset]))
	case d2:
		ts = c.baseTime() + clientmodel.Timestamp(binary.LittleEndian.Uint16(c.buf[offset:]))
	case d4:
		ts = c.baseTime() + clientmodel.Timestamp(binary.LittleEndian.Uint32(c.buf[offset:]))
	case d8:
		// Take absolute value for d8.
		ts = clientmodel.Timestamp(binary.LittleEndian.Uint64(c.buf[offset:]))
	default:
		panic("Invalid number of bytes for time delta")
	}

	offset += int(c.timeBytes())

	var v clientmodel.SampleValue
	if c.isInt() {
		switch c.valueBytes() {
		case d0:
			v = c.baseValue()
		case d1:
			v = c.baseValue() + clientmodel.SampleValue(int8(c.buf[offset]))
		case d2:
			v = c.baseValue() + clientmodel.SampleValue(int16(binary.LittleEndian.Uint16(c.buf[offset:])))
		case d4:
			v = c.baseValue() + clientmodel.SampleValue(int32(binary.LittleEndian.Uint32(c.buf[offset:])))
		// No d8 for ints.
		default:
			panic("Invalid number of bytes for integer delta")
		}
	} else {
		switch c.valueBytes() {
		case d4:
			v = c.baseValue() + clientmodel.SampleValue(math.Float32frombits(binary.LittleEndian.Uint32(c.buf[offset:])))
		case d8:
			// Take absolute value for d8.
			v = clientmodel.SampleValue(math.Float64frombits(binary.LittleEndian.Uint64(c.buf[offset:])))
		default:
			panic("Invalid number of bytes for floating point delta")
		}
	}
	return &metric.SamplePair{
		Timestamp: ts,
		Value:     v,
	}
}

// firstTime implements chunk.
func (c *doubleDeltaEncodedChunk) firstTime() clientmodel.Timestamp {
	return c.valueAtIndex(0).Timestamp
}

// lastTime implements chunk.
func (c *doubleDeltaEncodedChunk) lastTime() clientmodel.Timestamp {
	return c.valueAtIndex(c.len() - 1).Timestamp
}

// marshal implements chunk.
func (c *doubleDeltaEncodedChunk) marshal(w io.Writer) error {
	if len(c.buf) > math.MaxUint16 {
		panic("chunk buffer length would overflow a 16 bit uint.")
	}
	binary.LittleEndian.PutUint16(c.buf[deltaHeaderBufLenOffset:], uint16(len(c.buf)))

	n, err := w.Write(c.buf[:cap(c.buf)])
	if err != nil {
		return err
	}
	if n != cap(c.buf) {
		return fmt.Errorf("wanted to write %d bytes, wrote %d", len(c.buf), n)
	}
	return nil
}

// unmarshal implements chunk.
func (c *doubleDeltaEncodedChunk) unmarshal(r io.Reader) error {
	c.buf = c.buf[:cap(c.buf)]
	readBytes := 0
	for readBytes < len(c.buf) {
		n, err := r.Read(c.buf[readBytes:])
		if err != nil {
			return err
		}
		readBytes += n
	}
	c.buf = c.buf[:binary.LittleEndian.Uint16(c.buf[deltaHeaderBufLenOffset:])]
	return nil
}

// doubleDeltaEncodedChunkIterator implements chunkIterator.
type doubleDeltaEncodedChunkIterator struct {
	chunk *doubleDeltaEncodedChunk
	// TODO: add more fields here to keep track of last position.
}

// newIterator implements chunk.
func (c *doubleDeltaEncodedChunk) newIterator() chunkIterator {
	return &doubleDeltaEncodedChunkIterator{
		chunk: c,
	}
}

// getValueAtTime implements chunkIterator.
func (it *doubleDeltaEncodedChunkIterator) getValueAtTime(t clientmodel.Timestamp) metric.Values {
	i := sort.Search(it.chunk.len(), func(i int) bool {
		return !it.chunk.valueAtIndex(i).Timestamp.Before(t)
	})

	switch i {
	case 0:
		return metric.Values{*it.chunk.valueAtIndex(0)}
	case it.chunk.len():
		return metric.Values{*it.chunk.valueAtIndex(it.chunk.len() - 1)}
	default:
		v := it.chunk.valueAtIndex(i)
		if v.Timestamp.Equal(t) {
			return metric.Values{*v}
		}
		return metric.Values{*it.chunk.valueAtIndex(i - 1), *v}
	}
}

// getRangeValues implements chunkIterator.
func (it *doubleDeltaEncodedChunkIterator) getRangeValues(in metric.Interval) metric.Values {
	oldest := sort.Search(it.chunk.len(), func(i int) bool {
		return !it.chunk.valueAtIndex(i).Timestamp.Before(in.OldestInclusive)
	})

	newest := sort.Search(it.chunk.len(), func(i int) bool {
		return it.chunk.valueAtIndex(i).Timestamp.After(in.NewestInclusive)
	})

	if oldest == it.chunk.len() {
		return nil
	}

	result := make(metric.Values, 0, newest-oldest)
	for i := oldest; i < newest; i++ {
		result = append(result, *it.chunk.valueAtIndex(i))
	}
	return result
}

// contains implements chunkIterator.
func (it *doubleDeltaEncodedChunkIterator) contains(t clientmodel.Timestamp) bool {
	return !t.Before(it.chunk.firstTime()) && !t.After(it.chunk.lastTime())
}
