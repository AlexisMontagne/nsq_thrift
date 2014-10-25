package nsq_thrift

import (
  "github.com/bitly/go-nsq"
  "git.apache.org/thrift.git/lib/go/thrift"
  "sync"
  "errors"
)

type PeerType uint8

const (
  NSQLookupd PeerType = 0
  NSQD PeerType = 1
)

type TServerNSQ struct {
  consumer    *nsq.Consumer
  addrs       []string
  peerType    PeerType
  mu          sync.RWMutex
  interrupted bool
  messageChan chan *nsq.Message
}

func NewTServerNSQ(topic, channel string, peerType PeerType, addrs ...string) (*TServerNSQ, error) {
  consumer, err := nsq.NewConsumer(topic, channel, nsq.NewConfig())

  if err != nil {
    return nil, err
  }

  return &TServerNSQ{
    consumer: consumer,
    addrs: addrs,
    peerType: peerType,
    messageChan: make(chan *nsq.Message),
  }, nil
}

func (p *TServerNSQ) Listen() error {
  p.consumer.AddHandler(nsq.HandlerFunc(func(m *nsq.Message) error {
    p.messageChan <- m
    return nil
  }))

  if p.peerType == NSQD {
    return p.consumer.ConnectToNSQD(p.addrs[0])
  } else {
    return p.consumer.ConnectToNSQLookupds(p.addrs)
  }
}

func (p *TServerNSQ) Accept() (thrift.TTransport, error) {
  p.mu.RLock()
  interrupted := p.interrupted
  p.mu.RUnlock()

  if interrupted {
    return nil, errors.New("Transport Interrupted")
  }

  message := <-p.messageChan
  trans, _ := NewTNSQMessage(message)
  return trans, nil
}

func (p *TServerNSQ) Close() error {
  p.consumer.Stop()
  return nil
}

func (p *TServerNSQ) Interrupt() error {
  p.mu.Lock()
  p.interrupted = true
  p.mu.Unlock()

  return nil
}
