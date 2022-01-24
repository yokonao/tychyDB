package storage

import (
	"log"
	"strconv"

	"github.com/goccy/go-graphviz"
	"github.com/goccy/go-graphviz/cgraph"
	"github.com/tychyDB/algorithm"
)

func (st *Storage) Viz(fname string) {
	g := graphviz.New()
	graph, err := g.Graph()
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := graph.Close(); err != nil {
			log.Fatal(err)
		}
		g.Close()
	}()
	pageQueue := algorithm.NewQueue(64)
	nodeMap := make(map[uint32]*cgraph.Node)
	parentMap := make(map[uint32]uint32)

	pageQueue.Push(int(st.rootBlk.BlockNum))

	parentMap[st.rootBlk.BlockNum] = st.rootBlk.BlockNum

	for !pageQueue.IsEmpty() {
		curPageIndex := uint32(pageQueue.Pop())
		curPage := st.ptb.read(NewBlockId(curPageIndex))
		parentIndex := parentMap[curPageIndex]
		str := strconv.Itoa(int(curPageIndex)) + ", key:"
		for _, ptr := range curPage.ptrs {
			str += strconv.Itoa(int(curPage.cells[ptr].getKey())) + ", "
		}
		if !curPage.header.isLeaf {
			str += strconv.Itoa(int(curPage.cells[curPage.header.rightmostPtr].getKey()))
		}
		c, err := graph.CreateNode(str)
		if err != nil {
			log.Fatal(err)
		}
		nodeMap[curPageIndex] = c

		if curPageIndex != parentIndex {
			p := nodeMap[parentIndex]
			e, err := graph.CreateEdge("e", p, c)
			if err != nil {
				log.Fatal(err)
			}
			e.SetLabel("e")
		}

		if curPage.header.isLeaf {
			for _, ptr := range curPage.ptrs {
				keyValue := curPage.cells[ptr]
				key := keyValue.(KeyValueCell).key
				v, err := graph.CreateNode("L:" + strconv.Itoa(int(key)))
				if err != nil {
					log.Fatal(err)
				}
				ve, err := graph.CreateEdge("rec", c, v)
				if err != nil {
					log.Fatal(err)
				}
				ve.SetLabel("rec")
			}
		} else {
			for i := 0; i < int(curPage.header.numOfPtr-1); i++ {
				child := curPage.cells[curPage.ptrs[i]].(KeyCell).pageIndex
				pageQueue.Push(int(child))
				parentMap[child] = curPageIndex
			}
			child := curPage.cells[curPage.header.rightmostPtr].(KeyCell).pageIndex
			pageQueue.Push(int(child))
			parentMap[child] = curPageIndex
		}
	}
	if err := g.RenderFilename(graph, graphviz.PNG, fname+".png"); err != nil {
		log.Fatal(err)
	}
}
