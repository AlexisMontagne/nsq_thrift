all: thrift client server

client:
	GOPATH=`pwd`:$$GOPATH go build client.go

server:
	GOPATH=`pwd`:$$GOPATH go build server.go

thrift:
	thrift --gen go service.thrift

clean:
	rm -rf client server gen-go

