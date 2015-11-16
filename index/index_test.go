package index

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"reflect"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"testing/quick"
	"time"
	"unicode"
)

type testPost struct {
	id    uint64
	words []string
}

func splitToWords(postText string) []string {
	words := []string{}
	var word []rune = make([]rune, 0)
	for _, runeValue := range postText {
		if !unicode.IsLetter(runeValue) {
			if len(word) > 0 {
				words = append(words, strings.ToLower(string(word)))
				word = word[0:0]
			}
		} else {
			word = append(word, runeValue)
		}
	}
	if len(word) > 0 {
		words = append(words, strings.ToLower(string(word)))
	}
	return words
}

func loadAllWords() ([]string, error) {
	t, err := ioutil.ReadFile("wonderland.txt")
	if err != nil {
		return nil, err
	}

	words := splitToWords((string)(t))
	sort.Strings(words)
	removeDuplicateWords(&words)
	return words, nil
}

func buildChain() (*Chain, error) {
	t, err := ioutil.ReadFile("wonderland.txt")
	if err != nil {
		return nil, err
	}

	c := NewChain(2)
	pars := strings.Split((string)(t), "\r\n\r\n")
	for i := 0; i < len(pars); i++ {
		c.Build(strings.NewReader(pars[i]))
	}

	return c, nil
}

func createPosts(chain *Chain, count int) []testPost {
	posts := make([]testPost, count)
	for i := 0; i < count; i++ {
		text := chain.Generate(20)
		posts[i] = testPost{id: (uint64)(rand.Int63()), words: splitToWords(text)}
	}

	return posts
}

var aliceChain *Chain

type AliceText string

func (a AliceText) Generate(rand *rand.Rand, size int) reflect.Value {
	at := (AliceText)(aliceChain.Generate(size))
	return reflect.ValueOf(at)
}

func TestMain(m *testing.M) {
	flag.Parse()
	var err error
	if aliceChain, err = buildChain(); err != nil {
		fmt.Printf("Unable to build chain for testing: %s", err.Error())
		os.Exit(-1)
	}

	os.Exit(m.Run())
}

func TestAddAndQuery(t *testing.T) {
	f := func(text AliceText, id uint64) bool {
		w := splitToWords((string)(text))

		idx := &PostIndex{}
		idx.AddPost(id, w)

		q := fmt.Sprintf("\"%s\"", strings.ToLower(w[0]))
		r, err := idx.QueryPosts(q, 100)
		return err == nil && len(r) == 1 && r[0] == id
	}
	if err := quick.Check(f, nil); err != nil {
		t.Error(err)
	}
}

func BenchmarkAddPost(b *testing.B) {
	rand.Seed(time.Now().UnixNano()) // Seed the random number generator.
	idx := &PostIndex{}
	posts := createPosts(aliceChain, b.N)

	var index int32 = -1 // Count up to N but atomically

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := atomic.AddInt32(&index, 1)
			idx.AddPost(posts[i].id, posts[i].words)
		}
	})
}

// Markov stuff, taken from https://golang.org/doc/codewalk/markov/

// Copyright 2011 The Go Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

/*
Generating random text: a Markov chain algorithm

Based on the program presented in the "Design and Implementation" chapter
of The Practice of Programming (Kernighan and Pike, Addison-Wesley 1999).
See also Computer Recreations, Scientific American 260, 122 - 125 (1989).

A Markov chain algorithm generates text by creating a statistical model of
potential textual suffixes for a given prefix. Consider this text:

	I am not a number! I am a free man!

Our Markov chain algorithm would arrange this text into this set of prefixes
and suffixes, or "chain": (This table assumes a prefix length of two words.)

	Prefix       Suffix

	"" ""        I
	"" I         am
	I am         a
	I am         not
	a free       man!
	am a         free
	am not       a
	a number!    I
	number! I    am
	not a        number!

To generate text using this table we select an initial prefix ("I am", for
example), choose one of the suffixes associated with that prefix at random
with probability determined by the input statistics ("a"),
and then create a new prefix by removing the first word from the prefix
and appending the suffix (making the new prefix is "am a"). Repeat this process
until we can't find any suffixes for the current prefix or we exceed the word
limit. (The word limit is necessary as the chain table may contain cycles.)
*/

// Prefix is a Markov chain prefix of one or more words.
type Prefix []string

// String returns the Prefix as a string (for use as a map key).
func (p Prefix) String() string {
	return strings.Join(p, " ")
}

// Shift removes the first word from the Prefix and appends the given word.
func (p Prefix) Shift(word string) {
	copy(p, p[1:])
	p[len(p)-1] = word
}

// Chain contains a map ("chain") of prefixes to a list of suffixes.
// A prefix is a string of prefixLen words joined with spaces.
// A suffix is a single word. A prefix can have multiple suffixes.
type Chain struct {
	chain     map[string][]string
	prefixLen int
}

// NewChain returns a new Chain with prefixes of prefixLen words.
func NewChain(prefixLen int) *Chain {
	return &Chain{make(map[string][]string), prefixLen}
}

// Build reads text from the provided Reader and
// parses it into prefixes and suffixes that are stored in Chain.
func (c *Chain) Build(r io.Reader) {
	br := bufio.NewReader(r)
	p := make(Prefix, c.prefixLen)
	for {
		var s string
		if _, err := fmt.Fscan(br, &s); err != nil {
			break
		}
		key := p.String()
		c.chain[key] = append(c.chain[key], s)
		p.Shift(s)
	}
}

// Generate returns a string of at most n words generated from Chain.
func (c *Chain) Generate(n int) string {
	p := make(Prefix, c.prefixLen)
	var words []string
	for i := 0; i < n; i++ {
		choices := c.chain[p.String()]
		if len(choices) == 0 {
			break
		}
		next := choices[rand.Intn(len(choices))]
		words = append(words, next)
		p.Shift(next)
	}
	return strings.Join(words, " ")
}
