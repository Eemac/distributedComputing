package main 

import (
	"time"
	"net"
	"net/http"
	"net/rpc"
	"log"
	"fmt"
)

type Args struct {}

type TimeServer map[string]int 

func main() {

	longstring = "aliwyegfliajshdfljhabsdflhkbasdlfbsjlhdfbljhasdbfkjasbdflbasdfbajlhsdbfjlhabsdfjkhbasdjbfh"
	mini = longstring[0:15]
	fmt.Println(mini)
	timeserver := new(TimeServer)
	// Register the timeserver object upon which the GiveServerTime 
	// function will be called from the RPC server (from the client)
	rpc.Register(timeserver)
	// Registers an HTTP handler for RPC messages
	rpc.HandleHTTP() // ?
	// Start listening for the requests on port 1234
	listener, err := net.Listen("tcp", "0.0.0.0:1234")
	if err !=nil {
		log.Fatal("Listener error: ", err)
	}
	// Serve accepts incoming HTTP connections on the listener l, creating 
	// a new service goroutine for each. The service goroutines read requests 
	// and then call handler to reply to them
	http.Serve(listener, nil)
}

func (t *TimeServer) GiveServerTime(args *Args, reply *int64) error {
	// Set the value at the pointer got from the client 
	hello := new(map[string]int)
	
	*reply = new(hello)
	return nil
}