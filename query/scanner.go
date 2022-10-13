package query

import (
	"bytes"
	"errors"
	"io"
)

type Token uint

const (
	TKey Token = iota + 1
	TNumber
	TString
	TOp
	TParentheses
	TRegex
)

type pos struct {
	l int
	c int
}

func NewScanner(r io.Reader) *Scanner {
	return &Scanner{
		src: r,
		buf: make([]byte, 12),
	}
}

type Scanner struct {
	src    io.Reader
	pos    pos
	buf    []byte
	bufPos int
	bufLen int
	tok    Token
	lit    []byte
	match  func(byte) bool
}

func (s *Scanner) Token() (Token, []byte) {
	return s.tok, s.lit
}

func (s *Scanner) Position() (int, int) {
	return s.pos.l + 1, s.pos.c + 1
}

func (s *Scanner) Next() error {
	s.lit = s.lit[:0]
	s.tok = 0

	for {
		if s.bufLen-s.bufPos == 0 {
			err := s.advance()
			if err != nil {
				return err
			}
		}

		for ; s.bufPos < s.bufLen; s.bufPos++ {
			c := s.buf[s.bufPos]
			switch {
			case isKey(c):
				s.match = isKey
				s.tok = TKey
				return s.read()
			case isOp(c):
				s.match = isOp
				s.tok = TOp
				return s.read()
			case isNumber(c):
				s.match = isNumber
				s.tok = TNumber
				return s.read()
			case isString(c):
				s.match = isString
				s.tok = TString
				return s.readString()
			case isParentheses(c):
				s.match = isParentheses
				s.tok = TParentheses
				return s.read()
			case c == '\n':
				s.pos.l++
				s.pos.c = 0
			default:
				s.pos.c++
			}
		}
	}
}

func (s *Scanner) read() error {
	for {
		start := s.bufPos

		for ; s.bufPos < s.bufLen; s.bufPos++ {
			if !s.match(s.buf[s.bufPos]) {
				break
			}

			s.pos.c++
		}

		s.lit = append(s.lit, s.buf[start:s.bufPos]...)

		if s.bufPos < s.bufLen {
			break
		}

		read := s.bufPos > start

		err := s.advance()
		if err != nil {
			// ignore EOF if we read some token
			// we will return EOF on next call
			if read && errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
	}

	return nil
}

func (s *Scanner) readString() error {
	s.lit = append(s.lit, '"')
	s.bufPos++

	for {
		if s.bufPos == s.bufLen {
			err := s.advance()
			if err != nil {
				return err
			}
		}

		closePos := bytes.IndexByte(s.buf[s.bufPos:s.bufLen], '"')
		if closePos == -1 {
			s.lit = append(s.lit, s.buf[s.bufPos:s.bufLen]...)
			s.pos.c += (s.bufLen - s.bufPos)
			s.bufPos = s.bufLen
			continue
		}

		closeBuffPos := closePos + s.bufPos

		// check for escaped ", example: "\\\""
		sc := s.countSlashBack(closeBuffPos - 1)
		if sc != 0 && sc%2 != 0 {
			s.bufPos = closeBuffPos + 1
			continue
		}

		s.lit = append(s.lit, s.buf[s.bufPos:closeBuffPos+1]...)
		s.pos.c += closeBuffPos + 1 - s.bufPos
		s.bufPos = closeBuffPos + 1
		return nil
	}
}

func (s *Scanner) countSlashBack(p int) int {
	c := 0

	i := p
	for ; i >= 0; i-- {
		if s.buf[i] != '\\' {
			break
		}
		c++
	}

	if i == -1 {
		for j := len(s.lit) - 1; j >= 0; j-- {
			if s.lit[j] != '\\' {
				break
			}
			c++
		}
	}

	return c
}

func (s *Scanner) advance() error {
	n, err := io.ReadFull(s.src, s.buf)

	s.bufPos = 0
	s.bufLen = n

	if errors.Is(err, io.ErrUnexpectedEOF) {
		if n == 0 {
			return io.EOF
		}

		return nil
	}

	return err
}

func isKey(s byte) bool {
	return (s >= 'a' && s <= 'z') ||
		(s >= 'A' && s <= 'Z') ||
		s == '_' ||
		s == '.' ||
		s == '-'
}

func isOp(s byte) bool {
	return bytes.IndexByte([]byte("<>=!"), s) >= 0
}

func isNumber(s byte) bool {
	return (s >= '0' && s <= '9')
}

func isString(s byte) bool {
	return s == '"'
}

func isParentheses(s byte) bool {
	return s == '(' || s == ')'
}
