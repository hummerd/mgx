package mgx

import "io"

type token uint

const (
	tKey token = iota + 1
)

type pos struct {
	l int
	c int
}

type scanner struct {
	src io.Reader
	pos pos
	buf []byte
	tok token
	lit string
}

func (s *scanner) next() {
	for i := 0; i < len(s.buf); i++ {
		c := s.buf[i]
		switch {
		case isKey(c):
			s.tok = tKey
			s.lit = s.readKey()
		case c == '\n':
			s.pos.l++
			s.pos.c = 0
		}
	}
}

func isKey(s byte) bool {
	return false
}

func (s *scanner) readKey() string {
	return ""
}
