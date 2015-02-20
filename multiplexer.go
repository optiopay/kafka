package kafka

import (
	"errors"
	"sync"

	"github.com/husio/kafka/proto"
)

type Fetcher interface {
	Fetch() (*proto.Message, error)
}

// ErrMxClosed is returned as a result of closed multiplexer fetch.
var ErrMxClosed = errors.New("closed")

// Mx is multiplexer combining into single stream number of fetchers.
//
// It is responsibility of the user of the multiplexer and the fetcher
// implementation to handle errors. Multiplexer will not do anything except
// passing them through.
//
// It is important to remember that because fetch from every fetcher is done by
// separate worker, most of the time there is one message consumed by each
// worker that is held in memory while waiting for opportuninty to return it
// once fetch on multiplexer is called.
type Mx struct {
	wg   sync.WaitGroup
	errc chan error
	msgc chan *proto.Message
	stop chan struct{}

	mu     sync.Mutex
	closed bool
}

// Merge is merging fetch result of any number of fetchers into single stream
// and expose them through returned multiplexer.
func Merge(fetchers ...Fetcher) *Mx {
	p := &Mx{
		errc: make(chan error),
		msgc: make(chan *proto.Message),
		stop: make(chan struct{}),
	}

	for _, fetcher := range fetchers {
		p.wg.Add(1)

		go func(f Fetcher) {
			defer p.wg.Done()
			for {
				msg, err := f.Fetch()
				if err != nil {
					select {
					case p.errc <- err:
					case <-p.stop:
						return
					}
				} else {
					select {
					case p.msgc <- msg:
					case <-p.stop:
						return
					}
				}
			}
		}(fetcher)
	}

	return p
}

// Close is closing multiplexer and stopping all underlying workers. Call is
// blocking untill all workers are done. Calling Close from several goroutines
// is safe and will block all of them untill multiplexer is closed. Closing
// closed multiplexer has no effect.
func (p *Mx) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return
	}
	p.closed = true

	close(p.stop)
	p.wg.Wait()
	close(p.errc)
	close(p.msgc)
}

// Fetch returns fetch result from any of the merged fetchers.
func (p *Mx) Fetch() (*proto.Message, error) {
	select {
	case msg, ok := <-p.msgc:
		if ok {
			return msg, nil
		}
		return nil, ErrMxClosed
	case err, ok := <-p.errc:
		if ok {
			return nil, err
		}
		return nil, ErrMxClosed
	}
}
