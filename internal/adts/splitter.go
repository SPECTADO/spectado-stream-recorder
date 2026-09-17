package adts

import "github.com/spectado/stream-recorder/internal/id3"

// Header describes one ADTS frame header.
type Header struct {
	SampleRate int // Hz
	Blocks     int // number_of_raw_data_blocks + 1
	Length     int // frame_length, header included
}

// Duration returns the playback time of the frame in seconds (1024 samples per
// raw data block at the header's sampling rate).
func (h Header) Duration() float64 {
	if h.SampleRate == 0 {
		return 0
	}
	return float64(1024*h.Blocks) / float64(h.SampleRate)
}

// ParseHeader validates an ADTS header at the start of b (len(b) >= 7) and
// returns the decoded Header. ok is false when b does not begin with a valid
// header.
func ParseHeader(b []byte) (Header, bool) {
	if len(b) < minHeader {
		return Header{}, false
	}
	frameLen, ok := parseHeader(b)
	if !ok {
		return Header{}, false
	}
	return Header{
		SampleRate: sampleRates[(b[2]>>2)&0x0F],
		Blocks:     int(b[6]&0x03) + 1,
		Length:     frameLen,
	}, true
}

// Splitter cuts a byte stream into complete ADTS frames. It is the streaming
// counterpart of scan: bytes are fed in arbitrary chunks and every complete
// frame is handed to the frame callback, while byte runs that cannot be ADTS are
// handed to the junk callback so the file stays a faithful copy of ffmpeg's
// output. A partial frame at the end of the current input stays buffered until
// more bytes arrive or Flush is called.
//
// Trust rule (identical to scan): a frame is only emitted once the buffer is
// synced. Sync is established at a position whose valid header is followed by
// another valid header (or, at EOF, by nothing). Well-formed ID3v2 tags in the
// input are passed through as junk (ffmpeg does not emit them, but the writer
// must never lose bytes).
type Splitter struct {
	buf    []byte
	synced bool
}

// Feed appends p and emits every complete frame and junk run now available.
func (s *Splitter) Feed(p []byte, frame func(b []byte, h Header), junk func(b []byte)) {
	s.buf = append(s.buf, p...)
	s.process(false, frame, junk)
	// Reclaim the backing array once fully drained so a long recording does not
	// keep growing it.
	if len(s.buf) == 0 {
		s.buf = nil
	}
}

// Flush hands out whatever is buffered: the last complete frames plus any
// trailing partial frame (as junk). After Flush the splitter is empty.
func (s *Splitter) Flush(junk func(b []byte)) {
	s.process(true, func(b []byte, _ Header) { junk(b) }, junk)
	s.buf = nil
	s.synced = false
}

func (s *Splitter) consume(n int) { s.buf = s.buf[n:] }

// skipJunk emits at least one leading byte as junk. When the leading byte cannot
// start a frame (0xFF) or a tag ('I'), it extends the run up to the next such
// candidate so a long junk region is emitted in one call rather than byte by
// byte.
func (s *Splitter) skipJunk(junk func(b []byte)) {
	k := 1
	if s.buf[0] != 0xFF && s.buf[0] != 'I' {
		for k < len(s.buf) && s.buf[k] != 0xFF && s.buf[k] != 'I' {
			k++
		}
	}
	junk(s.buf[:k])
	s.consume(k)
}

func (s *Splitter) process(eof bool, frame func(b []byte, h Header), junk func(b []byte)) {
	for len(s.buf) > 0 {
		if s.synced {
			if len(s.buf) < minHeader {
				if eof {
					junk(s.buf)
					s.buf = nil
				}
				return
			}
			h, ok := ParseHeader(s.buf)
			if !ok {
				s.synced = false
				continue
			}
			if len(s.buf) < h.Length {
				if eof {
					junk(s.buf) // trailing partial frame
					s.buf = nil
				}
				return
			}
			frame(s.buf[:h.Length], h)
			s.consume(h.Length)
			continue
		}

		// Unsynced: look for a tag, then a confirmable frame.
		if s.buf[0] == 'I' {
			if len(s.buf) < 10 {
				if eof {
					junk(s.buf)
					s.buf = nil
				}
				return // wait for the full 10-byte tag header
			}
			if n, ok := id3.TagLen(s.buf); ok {
				if len(s.buf) < n {
					if eof {
						junk(s.buf) // truncated tag at EOF
						s.buf = nil
					}
					return
				}
				junk(s.buf[:n])
				s.consume(n)
				continue
			}
			// "ID3"-like prefix that is not a valid tag: junk.
			s.skipJunk(junk)
			continue
		}
		if len(s.buf) < minHeader {
			if headerPrefixValid(s.buf) {
				if eof {
					junk(s.buf)
					s.buf = nil
				}
				return // could be the start of a frame; wait
			}
			s.skipJunk(junk)
			continue
		}
		h, ok := ParseHeader(s.buf)
		if !ok {
			s.skipJunk(junk)
			continue
		}
		if len(s.buf) >= h.Length+minHeader {
			if _, ok := parseHeader(s.buf[h.Length:]); ok {
				s.synced = true
				continue
			}
			// A valid-looking header not followed by another: a false positive.
			s.skipJunk(junk)
			continue
		}
		// Not enough bytes to confirm the following header.
		if !eof {
			return // wait for more
		}
		switch {
		case len(s.buf) == h.Length:
			frame(s.buf[:h.Length], h) // lone trailing frame, clean EOF
			s.consume(h.Length)
		case len(s.buf) < h.Length:
			junk(s.buf) // partial trailing frame
			s.buf = nil
		default:
			// A complete frame followed by a few bytes that cannot be a header:
			// cannot be trusted as a boundary, skip a byte and re-examine.
			s.skipJunk(junk)
		}
	}
}
