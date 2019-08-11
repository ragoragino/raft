package proxy

import (
	"fmt"
	"sync"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/metadata"
)

// IRouter will be used for testing Raft network partitions
// 1. It provides functionality to add a firewall rule and remove it
// 2. It also provides method to check whether the traffic is allowed
// to the desired endpoint and then returns the address to the endpoint.
// It parses grpc metadata headers, src and dest, to make routing decisions

type IRouter interface {
	AddFirewalRule(endpoint string) 
	RemoveFirewalRule(endpoint string) 
	Route(md metadata.MD) (string, error) 
}

type Router struct {
	firewalMutex sync.RWMutex
	firewallRule map[string]struct{}
}

func NewRouter(endpoints []string) *Router {
	return &Router{
		firewallRule: make(map[string]struct{}, 0),
	}
}

func (rm *Router) AddFirewalRule(endpoint string) {
	log.Debugf("adding firewall rule for %s", endpoint)

	rm.firewalMutex.Lock()
	rm.firewallRule[endpoint] = struct{}{}
	rm.firewalMutex.Unlock()
}

func (rm *Router) RemoveFirewalRule(endpoint string) {
	log.Debugf("removing firewall rule for %s", endpoint)

	rm.firewalMutex.Lock()
	delete(rm.firewallRule, endpoint)
	rm.firewalMutex.Unlock()
}

func (rm *Router) isEndpointUnderFirewall(endpoint string) bool {
	rm.firewalMutex.RLock()
	_, ok := rm.firewallRule[endpoint]
	rm.firewalMutex.RUnlock()

	return ok
}

func (rm *Router) Route(md metadata.MD) (string, error) {
	destHeader, ok := md["dest"]
	if !ok {
		err := fmt.Errorf("no dest header present while routing")
		log.Errorf("%+v", err)
		return "", err
	} else if len(destHeader) != 1 {
		err := fmt.Errorf("dest header has incorrect length: %d", len(destHeader))
		log.Errorf("%+v", err)
		return "", err
	}

	srcHeader, ok := md["src"]
	if !ok {
		err := fmt.Errorf("no src header present while routing")
		log.Errorf("%+v", err)
		return "", err
	} else if len(srcHeader) != 1 {
		err := fmt.Errorf("src header has incorrect length: %d", len(srcHeader))
		log.Errorf("%+v", err)
		return "", err
	}

	if ok := rm.isEndpointUnderFirewall(destHeader[0]); ok {
		log.Debugf("destination %s under firewall", destHeader[0])
		return "", fmt.Errorf("service is unavailable")
	}

	if ok := rm.isEndpointUnderFirewall(srcHeader[0]); ok {
		log.Debugf("source %s under firewall", srcHeader[0])
		return "", fmt.Errorf("service is unavailable")
	}

	return destHeader[0], nil
}