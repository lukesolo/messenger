package main

import (
	"fmt"
	"math/bits"
	"net"
)

type buckets struct {
	id         NodeID
	dict       map[NodeID]bucketPeer
	byDistance [][]bucketPeer

	tasks chan<- func()
}

func newBuckets(id NodeID) buckets {
	tasks := make(chan func(), 16)
	b := buckets{
		id:         id,
		dict:       make(map[NodeID]bucketPeer),
		byDistance: make([][]bucketPeer, 33, 33),
		tasks:      tasks,
	}
	go b.start(tasks)
	return b
}

func (b buckets) Add(id NodeID, addr net.Addr) buckets {
	b.tasks <- func() {
		b.add(id, addr)
	}
	return b
}

func (b buckets) Print() {
	b.tasks <- func() {
		b.print()
	}
}

func (b buckets) Exec(id NodeID, task func(bucketPeer)) {
	b.tasks <- func() {
		peer, ok := b.dict[id]
		if ok {
			go task(peer)
		}
	}
}

func (b buckets) ExecBestNodes(id NodeID, task func([]bucketPeer)) {
	b.tasks <- func() {
		distance := b.calcDistance(id)

		var best []bucketPeer
		for i := distance; i < 33; i++ {
			best = append(best, b.byDistance[i]...)
		}
		take := k
		if len(best) < k {
			take = len(best)
		}

		go task(best[:take])
	}
}

func (b buckets) start(tasks <-chan func()) {
	for task := range tasks {
		task()
	}
}

func (b buckets) add(id NodeID, addr net.Addr) *bucketPeer {
	if id == b.id {
		return nil
	}

	peer, ok := b.dict[id]
	if ok {
		return &peer
	}

	peer = bucketPeer{id, addr}
	b.dict[id] = peer
	distance := b.calcDistance(id)
	b.byDistance[distance] = append(b.byDistance[distance], peer)
	return &peer
}

func (b buckets) print() {
	for distance, bucket := range b.byDistance {
		if len(bucket) > 0 {
			fmt.Print(distance)
			for _, peer := range bucket {
				fmt.Print(" ")
				fmt.Print(peer.addr)
			}
			fmt.Println()
		}
	}
}

func (b buckets) calcDistance(id NodeID) int {
	xor := uint32(b.id ^ id)
	// fmt.Printf("%b\n%b\n%b\n", id1, id2, xor)
	return 32 - bits.LeadingZeros32(xor)
}

type bucketPeer struct {
	id   NodeID
	addr net.Addr
}

type NodeID uint32
