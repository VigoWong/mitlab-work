package raft

import (
	"log"
	"math/rand"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func RandomiseDuration(lower int, upper int) time.Duration {
	return time.Duration(lower+int(float32(upper-lower)*rand.Float32())) * time.Millisecond
}

func IsMajority(voteNum int, total int) bool {
	return voteNum > total/2
}
