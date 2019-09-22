package node

import (
	"context"
	"log"
	"math"
	pb "raft/internal/raft/node/gen"
	"time"
)

const (
	defaultRPCTimeout time.Duration = 5 * time.Second
)

type ICluster interface {
	GetLeaderIP() string
	BroadcastRequestVoteRPCs(ctx context.Context, request *pb.RequestVoteRequest) <-chan bool
}

type Cluster struct {
	clients map[string]pb.NodeClient
}

func (c *Cluster) BroadcastRequestVoteRPCs(ctx context.Context, request *pb.RequestVoteRequest) <-chan bool {
	successChannel := make(chan bool)
	votesChannel := make(chan struct{})

	// We are not closing votesChannel explicitly, but we expect it to be GC,
	// because we send all requests with a timeout context
	ctx, cancel := context.WithTimeout(ctx, defaultRPCTimeout)
	for key, client := range c.clients {
		go func() {
			resp, err := client.RequestVote(ctx, request)
			if err != nil {
				log.Printf("unable to RPC RequestVote to client %+v because: %+v", key, err)
				return
			}

			if resp.GetVoteGranted() {
				votesChannel <- struct{}{}
			}
		}()
	}

	go func() {
		defer func() {
			close(successChannel)
		}()

		clusterMajority := int(math.Round(float64(len(c.clients)) * 0.5))
		votes := 0

		for {
			select {
			case <-votesChannel:
				votes++
				if votes >= clusterMajority {
					successChannel <- true
				}

				cancel()
				return
			case <-ctx.Done():
				return
			}
		}
	}()

	return successChannel
}
