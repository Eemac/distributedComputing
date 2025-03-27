package main

import (
	"bufio"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

type RaftNode struct {
	selfID          int
	votes           int
	electionTimeout *time.Timer
	//0 = follower, 1 = candidate, 2 = leader
	state            int
	mutex            sync.Mutex
	heartbeatTimeout *time.Timer
}

type VoteArguments struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

type VoteReply struct {
	Term       int
	ResultVote bool
}

type AppendEntryArgument struct {
	Term     int
	LeaderID int
}

type AppendEntryReply struct {
	Term    int
	Success bool
}

type ServerConnection struct {
	serverID      int
	Address       string
	rpcConnection *rpc.Client
}

var serverNodes []ServerConnection
var currentTerm int
var node RaftNode
var logEntries []int
var votedFor = -1

func (*RaftNode) MonitorNodeTimeout() error {
	for {
		<-node.electionTimeout.C

		fmt.Printf("state %d", node.state)
		if node.state != 2 {
			LeaderElection()
		}

		node.resetElectionTimeout()
	}
	return nil
}

func logCheck(lastLogIndex int, lastLogTerm int) bool {
	if len(logEntries) == 0 {
		return true
	}
	myLastLogIndex := len(logEntries) - 1
	myLastLogTerm := logEntries[myLastLogIndex]
	return lastLogTerm > myLastLogTerm || (lastLogTerm == myLastLogTerm && lastLogIndex >= myLastLogIndex)
}

// The RequestVote RPC as defined in Raft
// Hint 1: Use the description in Figure 2 of the paper
// Hint 2: Only focus on the details related to leader election and majority votes
func (*RaftNode) RequestVote(arguments VoteArguments, reply *VoteReply) error {
	//add mutex
	if arguments.Term < currentTerm {
		reply.Term = currentTerm
		reply.ResultVote = false
		return nil
	}

	if (votedFor == -1 || votedFor == arguments.CandidateID) && logCheck(arguments.LastLogIndex, arguments.LastLogTerm) {
		votedFor = arguments.CandidateID
		currentTerm = arguments.Term
		reply.Term = currentTerm
		reply.ResultVote = true
		return nil
	}
	reply.Term = currentTerm
	reply.ResultVote = false
	return nil
}

// The AppendEntry RPC as defined in Raft
// Hint 1: Use the description in Figure 2 of the paper
// Hint 2: Only focus on the details related to leader election and heartbeats
func (*RaftNode) AppendEntry(arguments AppendEntryArgument, reply *AppendEntryReply) error {
	node.resetElectionTimeout()
	fmt.Println("heartbeat received")

	return nil
}

// You may use this function to help with handling the election time out
// Hint: It may be helpful to call this method every time the node wants to start an election
func LeaderElection() {
	var wg sync.WaitGroup
	currentTerm++
	votedFor = node.selfID
	//Node is a candidate now
	node.state = 1
	node.votes = 1

	//wait group?
	//Create vote arguments
	lastLogIndex := len(logEntries) - 1
	voteArgs := VoteArguments{currentTerm, node.selfID, lastLogIndex, 0}
	if lastLogIndex != -1 {
		voteArgs.LastLogTerm = logEntries[len(logEntries)-1]
	}

	//actually send the vote requests
	for _, peer := range serverNodes {
		wg.Add(1)
		go func(connection ServerConnection, arguments VoteArguments, wg *sync.WaitGroup, node *RaftNode) {
			defer wg.Done()
			var ackReply *VoteReply
			fmt.Println("before")
			err := connection.rpcConnection.Call("RaftNode.RequestVote", &arguments, &ackReply)
			if err != nil {
				fmt.Println(err)
				return
			}
			fmt.Println("after")
			//update votes count
			node.mutex.Lock()
			if ackReply.ResultVote {
				node.votes++
			}
			node.mutex.Unlock()

		}(peer, voteArgs, &wg, &node)
	}

	//Wait until the peers respond
	wg.Wait()

	fmt.Println(node.votes)

	if node.votes > len(serverNodes)/2 {
		node.state = 2
		//I am the leader
		go Heartbeat()
	} else {
		node.state = 0
	}
}

// You may use this function to help with handling the periodic heartbeats
// Hint: Use this only if the node is a leader
func Heartbeat() {
	for node.state == 2 {
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		time.Sleep(time.Duration(r.Intn(37)+37) * time.Millisecond)
		node.resetElectionTimeout()
	}
}

// resetElectionTimeout resets the election timeout to a new random duration.
// This function should be called whenever an event occurs that prevents the need for a new election,
// such as receiving a heartbeat from the leader or granting a vote to a candidate.
func (*RaftNode) resetElectionTimeout() {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	duration := time.Duration(r.Intn(150)+151) * time.Millisecond

	//Init for the first time
	if node.electionTimeout == nil {
		node.electionTimeout = time.NewTimer(duration)
	}

	node.electionTimeout.Stop()          // Use Reset method only on stopped or expired timers
	node.electionTimeout.Reset(duration) // Resets the timer to new random value
}

func main() {
	var wg sync.WaitGroup
	wg.Add(1)
	// The assumption here is that the command line arguments will contain:
	// This server's ID (zero-based), location and name of the cluster configuration file
	arguments := os.Args
	if len(arguments) == 1 {
		fmt.Println("Please provide cluster information.")
		return
	}

	// Read the values sent in the command line

	// Get this sever's ID (same as its index for simplicity)
	myID, err := strconv.Atoi(arguments[1])
	node.selfID = myID
	// Get the information of the cluster configuration file containing information on other servers
	file, err := os.Open(arguments[2])
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	myPort := "localhost"

	// Read the IP:port info from the cluster configuration file
	scanner := bufio.NewScanner(file)
	lines := make([]string, 0)
	index := 0
	for scanner.Scan() {
		// Get server IP:port
		text := scanner.Text()
		log.Printf(text, index)
		if index == myID {
			myPort = text
			index++
			continue
		}
		// Save that information as a string for now
		lines = append(lines, text)
		index++
	}
	// If anything wrong happens with readin the file, simply exit
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	// Following lines are to register the RPCs of this object of type RaftNode
	api := new(RaftNode)
	err = rpc.Register(api)
	if err != nil {
		log.Fatal("error registering the RPCs", err)
	}
	rpc.HandleHTTP()
	go http.ListenAndServe(myPort, nil)
	log.Printf("serving rpc on port" + myPort)

	// This is a workaround to slow things down until all servers are up and running
	// Idea: wait for user input to indicate that all servers are ready for connections
	// Pros: Guaranteed that all other servers are already alive
	// Cons: Non-realistic work around

	reader := bufio.NewReader(os.Stdin)
	fmt.Print("Type anything when ready to connect >> ")
	text, _ := reader.ReadString('\n')
	fmt.Println(text)

	// Idea 2: keep trying to connect to other servers even if failure is encountered
	// For fault tolerance, each node will continuously try to connect to other nodes
	// This loop will stop when all servers are connected
	// Pro: Realistic setup
	// Con: If one server is not set up correctly, the rest of the system will halt

	for index, element := range lines {
		// Attempt to connect to the other server node
		client, err := rpc.DialHTTP("tcp", element)
		// If connection is not established
		for err != nil {
			// Record it in log
			log.Println("Trying again. Connection error: ", err)
			// Try again!
			client, err = rpc.DialHTTP("tcp", element)
		}
		// Once connection is finally established
		// Save that connection information in the servers list
		serverNodes = append(serverNodes, ServerConnection{index, element, client})
		// Record that in log
		fmt.Println("Connected to " + element)
	}

	//Set node to be a follower on init
	node.state = 0
	node.votes = 0
	//Set timeout to be some time in the future
	node.resetElectionTimeout()
	go node.MonitorNodeTimeout()

	wg.Wait()

	// Once all the connections are established, we can start the typical operations within Raft
	// Leader election and heartbeats are concurrent and non-stop in Raft

	// HINT 1: You may need to start a thread here (or more, based on your logic)
	// Hint 2: Main process should never stop
	// Hint 3: After this point, the threads should take over
	// Heads up: they never will be done!
	// Hint 4: wg.Wait() might be helpful here

}
