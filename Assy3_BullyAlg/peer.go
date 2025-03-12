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

// RingNode represents a single node in the cluster, providing the
// necessary methods to handle RPCs.
type RingNode struct {
	mutex           sync.Mutex
	selfID          int
	leaderID        int
	myPort          string
	rpcConnection   *rpc.Client
	nominatedSelf   bool
	electionTimeout *time.Timer
	lines           []string
	gotResponse     bool
}

// RingVote contains the candidate's information that is needed for the
// node receiving the RingVote to decide whether to vote for the candidate
// in question.
type RingVote struct {
	CandidateID int
	IsTerminal  bool
}

// -----------------------------------------------------------------------------
// Leader Election
// -----------------------------------------------------------------------------

// RequestVote handles incoming RequestVote RPCs from other servers.
// It checks its own logs to confirm eligibility for leadership
// if the candidateID received is higher that server's own ID, then vote is accepted and passed
// if less than server's own ID, the vote is ignored
// if candidateID is the same as serverID, then election won, and server can confirm its leadership
func (node *RingNode) RequestVote(receivedVote RingVote, acknowledge *string) error {
	node.mutex.Lock()
	defer node.mutex.Unlock()

	fmt.Println("Vote message received for ", receivedVote.CandidateID)

	// Leader has been identified, we're done
	if receivedVote.IsTerminal {
		if node.leaderID != receivedVote.CandidateID {
			node.leaderID = receivedVote.CandidateID
			fmt.Println("Leader has been elected: ", receivedVote.CandidateID)
		}
		return nil
	}

	// Reject vote request if received candidateID is smaller
	if receivedVote.CandidateID < node.selfID {
		fmt.Println("Received a vote from lower Id of ", receivedVote.CandidateID)

		theNextVote := RingVote{
			CandidateID: node.selfID,
			IsTerminal:  false,
		}
		addr := node.lines[receivedVote.CandidateID]

		go func(addr string, arguments RingVote) {
			ackReply := "nil"
			client, dial_err := rpc.DialHTTP("tcp", addr)
			if dial_err != nil {
				// fmt.Println("Dialing error", addr, dial_err)
			} else {
				err := client.Call("RingNode.RequestVote", &arguments, &ackReply)
				if err != nil {
					// fmt.Println(err)
					return
				}
			}
		}(addr, theNextVote)

		//Start leader election now
		node.LeaderElection()

		return nil
	}

	if receivedVote.CandidateID > node.selfID {
		node.gotResponse = true
	}

	return nil
}

func (node *RingNode) LeaderElection() {
	// This is an option to limit who can start the leader election
	// Recommended if you want only a specific process to start the election
	// if node.selfID != 2 {
	// 	return
	// }

	// for {
	// Wait for election timeout
	// Uncomment this if you decide to do periodic leader election

	// <-node.electionTimeout.C
	node.gotResponse = false

	// Check if node is already leader so loop does not continue
	// if node.leaderID == node.selfID {
	// 	fmt.Println("Ending leader election because I am now leader")
	// 	//broadcast
	// 	return
	// }

	// Initialize election by incrementing term and voting for self

	//Initiate election
	for i := node.selfID + 1; i < len(node.lines); i++ {
		//Sending nomination message
		arguments := RingVote{
			CandidateID: node.selfID,
			IsTerminal:  false,
		}

		// nextNode := ServerConnection{i, lines[i], node.client}
		fmt.Println("Sending vote request to ", i)
		addr := node.lines[i]

		go func(addr string, arguments RingVote) {
			ackReply := "nil"
			client, dial_err := rpc.DialHTTP("tcp", addr)
			// fmt.Println(dial_err)
			if dial_err != nil {
				// fmt.Println("Dialing error", addr, dial_err)
			} else {
				err := client.Call("RingNode.RequestVote", &arguments, &ackReply)
				if err != nil {
					// fmt.Println(err)
					return
				}
			}
		}(addr, arguments)
	}

	time.Sleep(1 * time.Second)
	fmt.Println("response:", node.gotResponse)

	//NO ONE RESPONDS
	if node.gotResponse == false {
		node.leaderID = node.selfID
		fmt.Println("Leader has been elected: ", node.leaderID)

		for i := 0; i < node.selfID; i++ {

			// nextNode := ServerConnection{i, lines[i], node.client}
			fmt.Println("Sending election message to ", i)
			addr := node.lines[i]

			//Sending nomination message
			client, dial_err := rpc.DialHTTP("tcp", node.lines[i])
			if dial_err != nil {
				// fmt.Println("Dial error", dial_err)
				// fmt.Println("unavailable")
			} else {
				// nextNode := ServerConnection{i, lines[i], node.client}

				go func(addr string) {
					ackReply := "nil"

					finalVote := RingVote{
						CandidateID: node.selfID,
						IsTerminal:  true,
					}

					err := client.Call("RingNode.RequestVote", &finalVote, &ackReply)
					if err != nil {
						return
					}
				}(addr)
			}
		}
	}

	// If you want leader election to be restarted periodically,
	// Uncomment the next line
	// I do not recommend when debugging

	// node.resetElectionTimeout()
	// }
}

// resetElectionTimeout resets the election timeout to a new random duration.
// This function should be called whenever an event occurs that prevents the need for a new election,
// such as receiving a heartbeat from the leader or granting a vote to a candidate.
func (node *RingNode) resetElectionTimeout() {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	duration := time.Duration(r.Intn(150)+151) * time.Millisecond
	node.electionTimeout.Stop()          // Use Reset method only on stopped or expired timers
	node.electionTimeout.Reset(duration) // Resets the timer to new random value
}

// -----------------------------------------------------------------------------
func main() {
	// The assumption here is that the command line arguments will contain:
	// This server's ID (zero-based), location and name of the cluster configuration file
	arguments := os.Args
	if len(arguments) == 1 {
		fmt.Println("Please provide cluster information.")
		return
	}

	// --- Read the values sent in the command line
	// Get this server's ID (same as its index for simplicity)
	myID, _ := strconv.Atoi(arguments[1])

	// Get the information of the cluster configuration file containing information on other servers
	file, err := os.Open(arguments[2])
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	node := &RingNode{
		selfID:        myID,
		leaderID:      -1,
		nominatedSelf: false,
		mutex:         sync.Mutex{},
	}

	// --- Read the IP:port info from the cluster configuration file
	scanner := bufio.NewScanner(file)
	lines := make([]string, 0)
	index := 0
	for scanner.Scan() {
		// Get server IP:port
		text := scanner.Text()
		// log.Printf(text, index)
		if index == myID {
			node.myPort = text
			index++
			//continue
		}
		// Save that information as a string for now
		lines = append(lines, text)
		index++
	}
	node.lines = lines
	// If anything wrong happens with reading the file, simply exit
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	// --- Register the RPCs of this object of type RaftNode
	err = rpc.Register(node)
	if err != nil {
		log.Fatal("Error registering the RPCs", err)
	}
	rpc.HandleHTTP()
	go http.ListenAndServe(node.myPort, nil)
	log.Printf("serving rpc on port" + node.myPort)

	// fmt.Println("index stopped at ", index)

	// Connect to next node
	// var strNextNode = lines[(myID+1)%(index-1)]
	// // Attempt to connect to the other server node
	// client, err := rpc.DialHTTP("tcp", strNextNode)
	// // If connection is not established
	// for err != nil {
	// 	// Record it in log
	// 	log.Println("Trying again. Connection error: ", err)
	// 	// Try again!
	// 	client, err = rpc.DialHTTP("tcp", strNextNode)
	// }
	// // Once connection is established, save connection information in nextNode
	// node.nextNode = ServerConnection{(myID + 1) % (index - 1), strNextNode, client}
	// // Record that in log
	// fmt.Println("Connected to " + strNextNode)

	// Start the election using a timer
	// Uncomment the next 3 lines, if you want leader election to be initiated periodically
	// I do not recommend it during debugging

	// r := rand.New(rand.NewSource(time.Now().UnixNano()))
	// tRandom := time.Duration(r.Intn(150)+151) * time.Millisecond
	// node.electionTimeout = time.NewTimer(tRandom)

	var wait sync.WaitGroup
	wait.Add(1)
	go node.LeaderElection() // Concurrent leader election, which can be made non-stop with timers
	wait.Wait()              // Waits forever, so main process does not stop
}
