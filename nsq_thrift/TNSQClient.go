package nsq_thrift

import (
	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/bitly/go-nsq"
)

type TNSQClient struct {
	producer *nsq.Producer
	topic    string
}

func NewTNSQClient(addr, topic string) (thrift.TTransport, error) {
	producer, err := nsq.NewProducer(addr, nsq.NewConfig())

	if err != nil {
		return nil, err
	}

	return &TNSQClient{
		producer: producer,
		topic:    topic,
	}, nil
}

func (p *TNSQClient) Open() error {
	return nil
}

func (p *TNSQClient) IsOpen() bool {
	return true
}

func (p *TNSQClient) Peek() bool {
	return p.IsOpen()
}

func (p *TNSQClient) Close() error {
	p.producer.Stop()
	return nil
}

func (p *TNSQClient) Read(buf []byte) (int, error) {
	panic("Can't handle bi directional messages")
}

func (p *TNSQClient) Write(buf []byte) (int, error) {
	p.producer.Publish(p.topic, buf)
	return len(buf), nil
}

func (p *TNSQClient) Flush() error {
	return nil
}
