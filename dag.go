package dag

import (
	"fmt"
	"sync"
)

type DAG interface {
	AddVertex(v *Vertex) error
	BFS(operation func(v *Vertex))
	Contains(*Vertex) bool
	GetChildren(v *Vertex) ([]*Vertex, error)
	GetParents(v *Vertex) ([]*Vertex, error)
	PostorderDFS(operation func(v *Vertex))
	PreorderDFS(operation func(v *Vertex))
	SetChildren(parent *Vertex, children ...*Vertex) error
	SetParents(child *Vertex, parents ...*Vertex) error
	Sinks() []*Vertex
	Sources() []*Vertex
}

type threadSafeDAG struct {
	dag *threadUnsafeDAG
	mx  sync.Mutex
}

func NewDAG() DAG {
	return &threadSafeDAG{dag: NewThreadUnsafeDAG().(*threadUnsafeDAG)}
}

func (d *threadSafeDAG) AddVertex(v *Vertex) error {
	d.mx.Lock()
	defer d.mx.Unlock()
	return d.dag.AddVertex(v)
}

func (d *threadSafeDAG) BFS(operation func(v *Vertex)) {
	d.mx.Lock()
	defer d.mx.Unlock()
	d.dag.BFS(operation)
}

func (d *threadSafeDAG) Contains(v *Vertex) bool {
	d.mx.Lock()
	defer d.mx.Unlock()
	return d.dag.Contains(v)
}

func (d *threadSafeDAG) GetChildren(v *Vertex) ([]*Vertex, error) {
	d.mx.Lock()
	defer d.mx.Unlock()
	return d.dag.GetChildren(v)
}

func (d *threadSafeDAG) GetParents(v *Vertex) ([]*Vertex, error) {
	d.mx.Lock()
	defer d.mx.Unlock()
	return d.dag.GetParents(v)
}

func (d *threadSafeDAG) PostorderDFS(operation func(v *Vertex)) {
	d.mx.Lock()
	defer d.mx.Unlock()
	d.dag.PostorderDFS(operation)
}

func (d *threadSafeDAG) PreorderDFS(operation func(v *Vertex)) {
	d.mx.Lock()
	defer d.mx.Unlock()
	d.dag.PreorderDFS(operation)
}

func (d *threadSafeDAG) SetChildren(parent *Vertex, children ...*Vertex) error {
	d.mx.Lock()
	defer d.mx.Unlock()
	return d.dag.SetChildren(parent, children...)
}

func (d *threadSafeDAG) SetParents(child *Vertex, parents ...*Vertex) error {
	d.mx.Lock()
	defer d.mx.Unlock()
	return d.dag.SetParents(child, parents...)
}

func (d *threadSafeDAG) Sinks() []*Vertex {
	d.mx.Lock()
	defer d.mx.Unlock()
	return d.dag.Sinks()
}

func (d *threadSafeDAG) Sources() []*Vertex {
	d.mx.Lock()
	defer d.mx.Unlock()
	return d.dag.Sources()
}

type threadUnsafeDAG struct {
	backwardEdges   [][]bool
	forwardEdges    [][]bool
	sinks           map[*Vertex]bool
	sources         map[*Vertex]bool
	vertexPtrToIdxM map[*Vertex]int
	vertices        []*Vertex
}

func NewThreadUnsafeDAG() DAG {
	return &threadUnsafeDAG{sinks: map[*Vertex]bool{}, sources: map[*Vertex]bool{}, vertexPtrToIdxM: map[*Vertex]int{}}
}

func (d *threadUnsafeDAG) AddVertex(v *Vertex) error {
	if d.Contains(v) {
		return fmt.Errorf("Vertex %p already in DAG %p", v, d)
	}

	d.vertices = append(d.vertices, v)
	d.backwardEdges = append(d.backwardEdges, make([]bool, len(d.vertices)))
	d.forwardEdges = append(d.forwardEdges, make([]bool, len(d.vertices)))
	d.vertexPtrToIdxM[v] = len(d.vertices) - 1
	d.sinks[v] = true
	d.sources[v] = true

	for i := 0; i < len(d.vertices)-1; i++ {
		d.backwardEdges[i] = append(d.backwardEdges[i], false)
		d.forwardEdges[i] = append(d.forwardEdges[i], false)
	}
	return nil
}

func (d *threadUnsafeDAG) BFS(operation func(v *Vertex)) {
	visited := map[*Vertex]bool{}
	queue := make(chan *Vertex, len(d.vertices))
	defer close(queue)
	for s := range d.sources {
		queue <- s
		visited[s] = true
	}
	dequeue := func() (*Vertex, bool) {
		select {
		case v := <-queue:
			return v, true
		default:
			return nil, false
		}
	}
	v, ok := dequeue()
	for ok {
		operation(v)
		children, _ := d.GetChildren(v)
		for _, child := range children {
			if visited[child] {
				continue
			}
			queue <- child
			visited[child] = true
		}
		v, ok = dequeue()
	}
}

func (d *threadUnsafeDAG) Contains(v *Vertex) bool {
	_, contains := d.vertexPtrToIdxM[v]
	return contains
}
func (d *threadUnsafeDAG) GetChildren(v *Vertex) ([]*Vertex, error) {
	if !d.Contains(v) {
		return nil, fmt.Errorf("Vertex %p not in DAG %p", v, d)
	}
	var children []*Vertex
	for cIdx, edgeExists := range d.forwardEdges[d.vertexPtrToIdxM[v]] {
		if !edgeExists {
			continue
		}
		children = append(children, d.vertices[cIdx])
	}
	return children, nil
}

func (d *threadUnsafeDAG) GetParents(v *Vertex) ([]*Vertex, error) {
	if !d.Contains(v) {
		return nil, fmt.Errorf("Vertex %p not in DAG %p", v, d)
	}
	var parents []*Vertex
	for pIdx, edgeExists := range d.backwardEdges[d.vertexPtrToIdxM[v]] {
		if !edgeExists {
			continue
		}
		parents = append(parents, d.vertices[pIdx])
	}
	return parents, nil
}

func (d *threadUnsafeDAG) PostorderDFS(operation func(v *Vertex)) {
	waitedForChildren := map[*Vertex]bool{}
	visited := map[*Vertex]bool{}
	var stack []*Vertex
	for s := range d.sources {
		stack = append(stack, s)
		visited[s] = true
	}

	for len(stack) > 0 {
		n := len(stack) - 1
		v := stack[n]
		if waitedForChildren[v] {
			operation(v)
			stack = stack[:n]
			continue
		}
		children, _ := d.GetChildren(v)
		for _, child := range children {
			if visited[child] {
				continue
			}
			stack = append(stack, child)
			visited[child] = true
		}
		if len(stack) > n+1 {
			waitedForChildren[v] = true
			continue
		}
		operation(v)
		stack = stack[:n]
	}
}

func (d *threadUnsafeDAG) PreorderDFS(operation func(v *Vertex)) {
	visited := map[*Vertex]bool{}
	var stack []*Vertex
	for s := range d.sources {
		stack = append(stack, s)
		visited[s] = true
	}

	for len(stack) > 0 {
		n := len(stack) - 1
		v := stack[n]
		stack = stack[:n]
		operation(v)
		children, _ := d.GetChildren(v)
		for _, child := range children {
			if visited[child] {
				continue
			}
			stack = append(stack, child)
			visited[child] = true
		}
	}
}

func (d *threadUnsafeDAG) SetChildren(parent *Vertex, children ...*Vertex) error {
	pIdx, ok := d.vertexPtrToIdxM[parent]
	if !ok {
		return fmt.Errorf("parent Vertex %p not in DAG %p", parent, d)
	}
	for _, child := range children {
		cIdx, ok := d.vertexPtrToIdxM[child]
		if !ok {
			return fmt.Errorf("child Vertex %p not in DAG %p", child, d)
		}
		d.backwardEdges[cIdx][pIdx] = true
		d.forwardEdges[pIdx][cIdx] = true
	}
	delete(d.sinks, parent)
	for _, child := range children {
		delete(d.sources, child)
	}
	return nil
}

func (d *threadUnsafeDAG) SetParents(child *Vertex, parents ...*Vertex) error {
	cIdx, ok := d.vertexPtrToIdxM[child]
	if !ok {
		return fmt.Errorf("child Vertex %p not in DAG %p", child, d)
	}
	for _, parent := range parents {
		pIdx, ok := d.vertexPtrToIdxM[parent]
		if !ok {
			return fmt.Errorf("parent Vertex %p not in DAG %p", parent, d)
		}
		d.backwardEdges[cIdx][pIdx] = true
		d.forwardEdges[pIdx][cIdx] = true
	}
	delete(d.sources, child)
	for _, parent := range parents {
		delete(d.sinks, parent)
	}
	return nil
}

func (d *threadUnsafeDAG) Sinks() []*Vertex {
	var sinks []*Vertex
	for sink := range d.sinks {
		sinks = append(sinks, sink)
	}
	return sinks
}

func (d *threadUnsafeDAG) Sources() []*Vertex {
	var sources []*Vertex
	for source := range d.sources {
		sources = append(sources, source)
	}
	return sources
}
