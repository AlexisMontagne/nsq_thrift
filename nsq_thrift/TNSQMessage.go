package nsq_thrift

import (
	"bytes"
	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/bitly/go-nsq"
	"sync"
)

type TNSQMessage struct {
	message     *nsq.Message
	alreadyRead bool
	mu          sync.Mutex
}

func NewTNSQMessage(message *nsq.Message) (thrift.TTransport, error) {
	return &TNSQMessage{message: message, alreadyRead: false}, nil
}

func (p *TNSQMessage) Open() error {
	return nil
}

func (p *TNSQMessage) IsOpen() bool {
	return true
}

func (p *TNSQMessage) Peek() bool {
	return p.IsOpen()
}

func (p *TNSQMessage) Close() error {
	return nil
}

func (p *TNSQMessage) Read(buf []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.alreadyRead {
		return 0, nil
	} else {
		p.alreadyRead = true
		return bytes.NewBuffer(p.message.Body).Read(buf)
	}
}

func (p *TNSQMessage) Write(buf []byte) (int, error) {
	panic("can't write on this side")
}

func (p *TNSQMessage) Flush() error {
	return nil
}
