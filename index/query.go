package index

const beforeStartedLength int = 0
const doneLength = -1
const queryBufferSize int = 128

type QueryOperator interface {
	NextChunk(buffer [queryBufferSize]uint32) int
}

type QueryNode struct {
	buffer [queryBufferSize]uint32
	length int
	cursor int
	op     QueryOperator
}

func (q *QueryNode) Current() uint32 {
	return q.buffer[q.cursor]
}

func (q *QueryNode) Started() bool {
	return q.length == beforeStartedLength
}

func (q *QueryNode) Done() bool {
	return q.length == doneLength
}

func (q *QueryNode) MoveNext() bool {
	if !q.Done() {
		q.cursor++
		if q.cursor >= q.length {
			q.length = q.op.NextChunk(q.buffer)
			if q.length == 0 { // NextChunk returns 0 to signal completion.
				q.length = doneLength
			}
			q.cursor = 0
		}
	}

	return !q.Done()
}

type TerminalOperator struct {
	Current     *PostChunk
	ChunkCursor int32
}

func (op *TerminalOperator) NextChunk(buffer [queryBufferSize]uint32) int {
	var i int = 0
	for i < queryBufferSize && op.Current != nil {
		if op.ChunkCursor < 0 {
			op.Current = op.Current.Next
			op.ChunkCursor = op.Current.Length - 1
		}
		buffer[i] = op.Current.IDs[op.ChunkCursor]
		op.ChunkCursor--
		i++
	}
	return i
}

type AndOperator struct {
	Left  QueryNode
	Right QueryNode
}

func NewAndOperator(left QueryOperator, right QueryOperator) *AndOperator {
	return &AndOperator{QueryNode{op: left}, QueryNode{op: right}}
}

func (op *AndOperator) nextMatch() (uint32, bool) {
	if !(op.Left.MoveNext() && op.Right.MoveNext()) {
		return 0, false
	}

	for op.Left.Current() != op.Right.Current() {
		for op.Left.Current() > op.Right.Current() {
			if !op.Left.MoveNext() {
				return 0, false
			}
		}

		for op.Right.Current() > op.Left.Current() {
			if !op.Right.MoveNext() {
				return 0, false
			}
		}
	}

	return op.Left.Current(), true
}

func (op *AndOperator) NextChunk(buffer [queryBufferSize]uint32) int {
	i := 0
	for i < queryBufferSize {
		if value, success := op.nextMatch(); success {
			buffer[i] = value
			i++
		} else {
			break
		}
	}
	return i
}

type OrOperator struct {
	Left  QueryNode
	Right QueryNode
}

func NewOrOperator(left QueryOperator, right QueryOperator) *OrOperator {
	return &OrOperator{QueryNode{op: left}, QueryNode{op: right}}
}

func (op *OrOperator) NextChunk(buffer [queryBufferSize]uint32) int {
	if !op.Left.Started() {
		op.Left.MoveNext()
		op.Right.MoveNext()
	}

	i := 0
	for i < queryBufferSize && !op.Left.Done() && !op.Right.Done() {
		if op.Left.Current() > op.Right.Current() {
			buffer[i] = op.Left.Current()
			op.Left.MoveNext()
			i++
		} else if op.Right.Current() > op.Left.Current() {
			buffer[i] = op.Right.Current()
			op.Right.MoveNext()
			i++
		} else {
			buffer[i] = op.Left.Current()
			op.Left.MoveNext()
			op.Right.MoveNext()
			i++
		}
	}
	for i < queryBufferSize && !op.Left.Done() {
		buffer[i] = op.Left.Current()
		op.Left.MoveNext()
		i++
	}
	for i < queryBufferSize && !op.Right.Done() {
		buffer[i] = op.Right.Current()
		op.Right.MoveNext()
		i++
	}
	return i
}

type ParseError struct {
	Position int
	Message  string
}

func (e ParseError) Error() string {
	return e.Message // TODO: Better!
}

type opStack []QueryOperator

func (s *opStack) Length() int           { return len(*s) }
func (s *opStack) Push(op QueryOperator) { *s = append(*s, op) }
func (s *opStack) Pop() QueryOperator {
	old := *s
	length := len(old)
	if length < 1 {
		panic("Stack underflow")
	}

	result := old[length-1]
	*s = old[0 : length-1]
	return result
}

func ParseQuery(index *PostIndex, query string) (*QueryNode, error) {
	stack := &opStack{}
	for i := 0; i < len(query); i++ {
		if query[i] == '&' {
			if stack.Length() < 2 {
				return nil, ParseError{i, "Need two operands for &"}
			}
			stack.Push(NewAndOperator(stack.Pop(), stack.Pop()))
		} else if query[i] == '|' {
			if stack.Length() < 2 {
				return nil, ParseError{i, "Need two operands for |"}
			}
			stack.Push(NewOrOperator(stack.Pop(), stack.Pop()))
		} else if query[i] == '"' {
			i++
			start := i
			i++
			for i < len(query) && query[i] != '"' {
				i++
			}
			if i >= len(query) {
				return nil, ParseError{start - 1, "Unterminated string constant"}
			}

			chunk := index.findSetChunkForQuery(query[start:i])
			stack.Push(&TerminalOperator{Current: chunk})
		} else {
			return nil, ParseError{i, "Unexpected character"}
		}
	}
	if stack.Length() != 1 {
		return nil, ParseError{len(query), "Unterminated query"}
	}
	return &QueryNode{op: stack.Pop()}, nil
}
