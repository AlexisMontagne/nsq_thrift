package main

import (
	"../nsq_thrift"
	"./gen-go/service"
	"git.apache.org/thrift.git/lib/go/thrift"
	"log"
)

func main() {
	trans, err := nsq_thrift.NewTNSQClient("localhost:4150", "foo")

	if err != nil {
		panic("Invalid client")
	}

	defer trans.Close()

	var protocolFactory thrift.TProtocolFactory
	protocolFactory = thrift.NewTJSONProtocolFactory()
	client := service.NewTestClientFactory(trans, protocolFactory)
	log.Println("Message sent: buz")
	client.Foo("buz")
}
