// Sophie and Ian
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
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
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

type LogEntry struct {
	Index int
	Term  int
	Data  int
}

var lastAppliedIndex = 0
var serverNodes []ServerConnection
var currentTerm int
var node RaftNode
var logEntries []LogEntry
var commitIndex int
var lastIndex int
var votedFor = -1
var started = 0
var states = [3]string{"Follower", "Candidate", "Leader"}

// This function monitors the timeout from heartbeats and restarts an election if need be
func (*RaftNode) MonitorNodeTimeout() error {
	for {
		<-node.electionTimeout.C
		votedFor = -1
		LeaderElection()
		node.resetElectionTimeout()
	}
}

// check if log is as up-to-date/node can be a follower of want-to-be leader
func logCheck(lastLogIndex int, lastLogTerm int) bool {
	if len(logEntries) == 0 {
		return true
	}
	myLastLogIndex := len(logEntries) - 1
	myLastLogTerm := logEntries[myLastLogIndex].Term
	return lastLogTerm > myLastLogTerm || (lastLogTerm == myLastLogTerm && lastLogIndex >= myLastLogIndex)
}

// The RequestVote RPC as defined in Raft
// Hint 1: Use the description in Figure 2 of the paper
// Hint 2: Only focus on the details related to leader election and majority votes
func (*RaftNode) RequestVote(arguments VoteArguments, reply *VoteReply) error {
	if started == 1 {
		node.resetElectionTimeout()
		node.mutex.Lock()
		defer node.mutex.Unlock()

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
	}

	return nil
}

// The AppendEntry RPC as defined in Raft
// Hint 1: Use the description in Figure 2 of the paper
// Hint 2: Only focus on the details related to leader election and heartbeats
func (*RaftNode) AppendEntry(arguments AppendEntryArgument, reply *AppendEntryReply) error {
	if started == 1 {

		// return nil
		node.mutex.Lock()
		defer node.mutex.Unlock()

		if arguments.Term < currentTerm {
			reply.Term = currentTerm
			reply.Success = false
			return nil
		}
		node.resetElectionTimeout()
		currentTerm = arguments.Term
		node.state = 0
		reply.Term = currentTerm

		//If there are entries:
		if len(logEntries) > 0 && len(logEntries)-1 >= arguments.PrevLogIndex {
			if logEntries[arguments.PrevLogIndex].Term != arguments.PrevLogTerm {
				reply.Success = false
				fmt.Println("hehre")
				return nil
			}
		}

		//IF there are logs...
		if len(logEntries) > 0 && arguments.PrevLogIndex >= 0 {
			//delete conflicting logs
			for i, entry := range arguments.Entries {
				//gets the index for the current entry
				for j, nEnt := range logEntries {
					if nEnt.Data == entry.Data && nEnt.Index == entry.Index && nEnt.Term == entry.Term {
						logEntries = logEntries[:j]
						break
					}
				}
				logIndex := arguments.PrevLogIndex + i

				if logIndex+1 < len(logEntries) {
					if logEntries[logIndex].Term > entry.Term {
						logEntries = logEntries[1:logIndex]
						break
					}
				}
			}
		}

		//add any new log entries
		for i, entry := range arguments.Entries {
			logIndex := arguments.PrevLogIndex + i
			if logIndex >= len(logEntries) {
				logEntries = append(logEntries, entry)
			}
		}

		if arguments.LeaderCommit > commitIndex {
			lastNewIndex := arguments.PrevLogIndex + len(arguments.Entries)
			if lastNewIndex < len(logEntries) {
				commitIndex = lastNewIndex
			} else {
				commitIndex = arguments.LeaderCommit
			}
		}

		reply.Success = true
	}
	return nil
}

// You may use this function to help with handling the election time out
// Hint: It may be helpful to call this method every time the node wants to start an election
func LeaderElection() {
	node.mutex.Lock()
	currentTerm++
	var termMax = 0
	votedFor = node.selfID
	//reset election timeout
	node.resetElectionTimeout()

	//Node is a candidate now
	node.state = 1
	node.votes = 1
	node.mutex.Unlock()

	//Create vote arguments
	lastLogIndex := len(logEntries) - 1
	voteArgs := VoteArguments{currentTerm, node.selfID, lastLogIndex, 0}
	if lastLogIndex != -1 {
		voteArgs.LastLogTerm = logEntries[len(logEntries)-1].Term
	}

	//actually send the vote requests
	for _, peer := range serverNodes {
		go func(connection ServerConnection, arguments VoteArguments, node *RaftNode) {
			var ackReply VoteReply
			err := connection.rpcConnection.Call("RaftNode.RequestVote", &arguments, &ackReply)
			if err != nil {
				// fmt.Println(err)
				return
			}
			//update votes count and term max
			node.mutex.Lock()

			termMax = max(termMax, ackReply.Term)

			if ackReply.ResultVote {
				node.votes++
			}

			if node.votes > len(serverNodes)/2 && termMax <= currentTerm {
				node.state = 2
				//I am the leader
				go Heartbeat()
			}
			node.mutex.Unlock()

		}(peer, voteArgs, &node)
	}

	//Wait until the peers respond
	<-node.electionTimeout.C

	node.mutex.Lock()
	if node.votes > len(serverNodes)/2 && termMax < currentTerm {
		node.state = 2
		//I am the leader
		node.mutex.Unlock()
		go Heartbeat()
	} else {
		node.state = 0
		votedFor = -1
		currentTerm = termMax
		node.mutex.Unlock()
	}

	fmt.Print("Process is a: ")
	fmt.Println(states[node.state])
}

// You may use this function to help with handling the periodic heartbeats
// Hint: Use this only if the node is a leader
func Heartbeat() {

	for node.state == 2 {
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		time.Sleep(time.Duration(r.Intn(37)+37) * time.Millisecond)

		lastLogIndex := len(logEntries) - 1
		prevLogIndex := lastLogIndex
		prevLogTerm := 0

		if prevLogIndex >= 0 {
			prevLogTerm = logEntries[prevLogIndex].Term
		}

		entries := []LogEntry{}
		leaderCommit := commitIndex

		var wg sync.WaitGroup

		for _, peer := range serverNodes {
			wg.Add(1)
			go func(connection ServerConnection, wg *sync.WaitGroup, node *RaftNode) {
				defer wg.Done()
				arguments := AppendEntryArgument{
					Term:         currentTerm,
					LeaderID:     node.selfID,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  prevLogTerm,
					Entries:      entries,
					LeaderCommit: leaderCommit,
				}
				var ackReply AppendEntryReply
				err := connection.rpcConnection.Call("RaftNode.AppendEntry", &arguments, &ackReply)
				if err != nil {
					return
				} else {
					node.resetElectionTimeout()
				}

			}(peer, &wg, &node)
		}

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

func ClientAddToLog() {
	var wg sync.WaitGroup
	for {
		time.Sleep(time.Second)
		fmt.Print("Log: ")
		fmt.Println(logEntries)
		// fmt.Print("Term: ")
		// log.Println(currentTerm)

		if node.state == 2 {

			lastLogIndex := len(logEntries) - 1
			prevLogIndex := lastLogIndex
			prevLogTerm := 0

			if prevLogIndex >= 0 {
				prevLogTerm = logEntries[prevLogIndex].Term
			}

			leaderCommit := commitIndex
			var entries = []LogEntry{
				{
					Index: len(logEntries),
					Term:  currentTerm,
					Data:  node.selfID,
				},
			}

			logEntries = append(logEntries, entries[0])

			var numVotes = 0

			// lastAppliedIndex here is an int variable that is needed by a node to store the value of the last index it used in the log
			log.Println("Leader log add idx: " + strconv.Itoa(entries[0].Index))
			// Add rest of logic here

			// HINT 1: using the AppendEntry RPC might happen here
			for _, peer := range serverNodes {
				wg.Add(1)
				go func(connection ServerConnection, wg *sync.WaitGroup, node *RaftNode) {
					defer wg.Done()
					arguments := AppendEntryArgument{
						Term:         currentTerm,
						LeaderID:     node.selfID,
						PrevLogIndex: prevLogIndex,
						PrevLogTerm:  prevLogTerm,
						Entries:      logEntries[commitIndex:],
						LeaderCommit: leaderCommit,
					}
					var ackReply AppendEntryReply
					err := connection.rpcConnection.Call("RaftNode.AppendEntry", &arguments, &ackReply)

					if err != nil {
						return
					} else {
						if ackReply.Success {
							numVotes++
						}
						node.resetElectionTimeout()
					}

				}(peer, &wg, &node)
			}
			if numVotes > len(serverNodes)/2 {
				commitIndex = len(logEntries)
			}
		}
	}
}

func main() {
	var wg sync.WaitGroup
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
	started = 1

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
	wg.Add(2)
	go node.MonitorNodeTimeout()
	//Once every second, add a log if leader
	go ClientAddToLog()

	wg.Wait()

	// Once all the connections are established, we can start the typical operations within Raft
	// Leader election and heartbeats are concurrent and non-stop in Raft

	// HINT 1: You may need to start a thread here (or more, based on your logic)
	// Hint 2: Main process should never stop
	// Hint 3: After this point, the threads should take over
	// Heads up: they never will be done!
	// Hint 4: wg.Wait() might be helpful here

}
