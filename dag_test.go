package dag

import (
	"testing"
)

func testDAG() DAG {
	// v1 ----> v2 ----> v4
	//     \         /
	//      --> v3 --
	v1 := &Vertex{Value:1}
	v2 := &Vertex{Value:2}
	v3 := &Vertex{Value:3}
	v4 := &Vertex{Value:4}
	d := NewDAG()
	d.AddVertex(v1)
	d.AddVertex(v2)
	d.AddVertex(v3)
	d.AddVertex(v4)
	d.SetChildren(v1, v2, v3)
	d.SetParents(v4, v2, v3)
	return d
}

func Test_BFS(t *testing.T) {
	d := testDAG()

	// Can either be 1,2,3,4 or 1,3,2,4
	var r []int
	d.BFS(func(v *Vertex){
		r = append(r, v.Value.(int))
	})
	if r[0] != 1 {
		t.Errorf("Bad result: %v", r)
		t.Fail()
	}
	if r[3] != 4 {
		t.Errorf("Bad result: %v", r)
		t.Fail()
	}
	if !(r[1] == 3 && r[2] == 2) && !(r[1] == 2 && r[2] == 3) {
		t.Errorf("Bad result: %v", r)
		t.Fail()
	}
}

func Test_PostorderDFS(t *testing.T) {
	d := testDAG()

	// Can either be 4,3,2,1 or 4,2,3,1
	var r []int
	d.PostorderDFS(func(v *Vertex){
		r = append(r, v.Value.(int))
	})
	if r[0] != 4 {
		t.Errorf("Bad result: %v", r)
		t.Fail()
	}
	if r[3] != 1 {
		t.Errorf("Bad result: %v", r)
		t.Fail()
	}
	if !(r[1] == 3 && r[2] == 2) && !(r[1] == 2 && r[2] == 3) {
		t.Errorf("Bad result: %v", r)
		t.Fail()
	}
}

func Test_PreorderDFS(t *testing.T) {
	d := testDAG()

	// Can either be 1,2,4,3 or 1,3,4,2
	var r []int
	d.PreorderDFS(func(v *Vertex){
		r = append(r, v.Value.(int))
	})
	if r[0] != 1 {
		t.Errorf("Bad result: %v", r)
		t.Fail()
	}
	if r[2] != 4 {
		t.Errorf("Bad result: %v", r)
		t.Fail()
	}
	if !(r[1] == 2 && r[3] == 3) && !(r[1] == 3 && r[3] == 2) {
		t.Errorf("Bad result: %v", r)
		t.Fail()
	}
}