package dag

type Vertex struct {
	Value interface{}
}

func (v *Vertex) Children(dag *threadUnsafeDAG) ([]*Vertex, error) {
	return dag.GetChildren(v)
}

func (v *Vertex) Parents(dag *threadUnsafeDAG) ([]*Vertex, error) {
	return dag.GetParents(v)
}
