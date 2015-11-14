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

func (index *PostIndex) findSetChunkForQuery(word string) *PostChunk {
	index.setsLock.RLock()
	defer index.setsLock.RUnlock()

	if index.sets == nil {
		return nil
	}

	set, present := index.sets[word]
	if present {
		return set.FirstChunk()
	} else {
		return nil
	}
}

func (index *PostIndex) findOrCreateSets(sortedWords []string) []*PostSet {
	// This function takes two passes, in order to minimize the amount of
	// locking and unlocking that we have to do. (And therefore allowing for
	// the maximum parallelism during the read phase.)
	//
	sets := make([]*PostSet, len(sortedWords), cap(sortedWords))

	// Make a pass through the table with a read-lock, first, to see if we
	// can get all the values we care about without taking a write
	// lock. (Write locks interrupt queries and so are expensive.)
	//
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
	//
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
	// NOTE: Access to the ID map is currently protected by a mutex; this is
	// not great. Perhaps we need something fancier? Like channels and
	// goroutines and the like? That would let us at least add
	// asynchronously, but it still serializes the readers, which is where we
	// need the most performance...
	//
	index.idMapLock.Lock()
	defer index.idMapLock.Unlock()

	if index.idMap == nil {
		index.idMap = make(map[uint32]uint64)
	}
	index.idMap[localId] = globalId
}

func (index *PostIndex) AddPost(id uint64, words []string) {
	// The order of operations here is pretty critical. We need to ensure is
	// that each of the sets contains integers in ascending order. IDs must
	// be added in ascending order or the set operations in the query won't
	// work. In order to do this, we serialize calls to AddPost, but only
	// relative to other calls that include the same words. Calls to AddPost
	// that have disjoint words can proceed in parallel without blocking each
	// other. To accomplish this feat, we:
	//
	//  - Sort the words, then take the locks for the sets in the order of
	//    the words. (Sorting the words provides a global lock order,
	//    ensuring that we won't deadlock.)
	//  - Generate a new local ID under the locks. Holding the locks before
	//    we generate the new local ID ensures that no larger ID can be
	//    inserted into the sets before we're done.
	//  - Add the mapping from local ID to global ID. Doing this before
	//    inserting the local ID into the set ensures that any query that
	//    sees the local ID will be able to reverse it to a local ID.
	//  - Add the local ID to the relevant sets.
	//
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
