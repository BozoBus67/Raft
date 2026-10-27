package main

import "time"

type Clock interface {
	Now() time.Time
	Since(t time.Time) time.Duration
	Sleep(d time.Duration)
	NewTicker(d time.Duration) Ticker
}

type Ticker interface {
	C() <-chan time.Time
	Stop()
}

type realClock struct{}

func NewRealClock() Clock { return realClock{} }

func (realClock) Now() time.Time                   { return time.Now() }
func (realClock) Since(t time.Time) time.Duration  { return time.Since(t) }
func (realClock) Sleep(d time.Duration)            { time.Sleep(d) }
func (realClock) NewTicker(d time.Duration) Ticker { return &realTicker{t: time.NewTicker(d)} }

type realTicker struct {
	t *time.Ticker
}

func (r *realTicker) C() <-chan time.Time { return r.t.C }
func (r *realTicker) Stop()               { r.t.Stop() }
