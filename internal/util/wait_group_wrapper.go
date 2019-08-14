package util

import (
	"sync"
)

type WaitGroupWrapper struct {
	sync.WaitGroup
}

// 创建goroutine，运行cb函数
func (w *WaitGroupWrapper) Wrap(cb func()) {
	w.Add(1)
	go func() {
		cb()
		w.Done()
	}()
}
