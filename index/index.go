package index

import (
	"sort"
	"sync"
	"sync/atomic"
)

const ChunkSize int32 = 4096

type PostChunk struct {
	IDs    [ChunkSize]uint32
	Length int32
	Next   *PostChunk
}

type PostSet struct {
	firstChunk atomic.Value
	SyncRoot   sync.Mutex
}

func NewPostSet() *PostSet {
	r := &PostSet{}
	r.firstChunk.Store(&PostChunk{})
	return r
}

func (set *PostSet) AddPost(id uint32) {
	firstChunk := set.FirstChunk()
	if firstChunk.Length == ChunkSize {
		target := &PostChunk{[ChunkSize]uint32{0: id}, 1, firstChunk}
		set.firstChunk.Store(target)
	} else {
		firstChunk.IDs[firstChunk.Length] = id
		atomic.AddInt32(&firstChunk.Length, 1)
	}
}

func (set *PostSet) FirstChunk() *PostChunk {
	return set.firstChunk.Load().(*PostChunk)
}

type PostIndex struct {
	nextLocalId int32
	setsLock    sync.RWMutex
	sets        map[string]*PostSet
	idMapLock   sync.RWMutex
	idMap       map[uint32]uint64
}

func (index *PostIndex) findOrCreateSets(sortedWords []string) []*PostSet {
	sets := make([]*PostSet, len(sortedWords), cap(sortedWords))

	// Make a pass through the table with a read-lock, first, to see if we
	// can get all the values we care about without taking a write
	// lock. (Write locks interrupt queries and so are expensive.)
	needWrite := false
	index.setsLock.RLock()
	if index.sets == nil {
		needWrite = true
	} else {
		for i := 0; i < len(sortedWords); i++ {
			present := true
			sets[i], present = index.sets[sortedWords[i]]
			needWrite = needWrite || !present
		}
	}
	index.setsLock.RUnlock()

	// If we missed any of the sets then take a write lock and go back
	// through, updating the set table and creating sets for words that
	// missed on the first pass.
	if needWrite {
		index.setsLock.Lock()
		if index.sets == nil {
			index.sets = make(map[string]*PostSet)
		}
		for i := 0; i < len(sortedWords); i++ {
			if sets[i] == nil {
				present := true
				if sets[i], present = index.sets[sortedWords[i]]; !present {
					sets[i] = NewPostSet()
					index.sets[sortedWords[i]] = sets[i]
				}
			}
		}
		index.setsLock.Unlock()
	}

	return sets
}

func (index *PostIndex) addIdMapping(globalId uint64, localId uint32) {
	index.idMapLock.Lock()
	defer index.idMapLock.Unlock()

	if index.idMap == nil {
		index.idMap = make(map[uint32]uint64)
	}
	index.idMap[localId] = globalId
}

func (index *PostIndex) AddPost(id uint64, words []string) {
	sortedWords := make([]string, len(words), cap(words))
	copy(sortedWords, words)
	sort.Strings(sortedWords)

	sets := index.findOrCreateSets(sortedWords)
	for _, v := range sets {
		v.SyncRoot.Lock()
		defer v.SyncRoot.Unlock()
	}

	localId := (uint32)(atomic.AddInt32(&index.nextLocalId, 1))
	index.addIdMapping(id, localId)

	for _, v := range sets {
		v.AddPost(localId)
	}
}
