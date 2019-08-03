package proxy

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"time"

	"net/http/httputil"

	"go.uber.org/atomic"

	log "github.com/sirupsen/logrus"
)

// Proxy will be used for testing Raft network partitions
// By providing n endpoints (on which Raft nodes will be listening)
// it creates n proxy servers that are mapped 1:1 to the Raft endpoints.
// One can then block traffic from/to certain nodes

// Usage:
/*
// Nodes will be listening on ports 9001 and 9002
endpoints := []string{"localhost:9001", "localhost:9002"}

// Proxy will be forwarding 8001->9001 and 8002->9002
mapOfEndpoints, proxy, err := StartTestProxy(endpoints)
if err != nil {
	panic(err)
}
defer fp.Shutdown()

someChannelThatDoesAllTheWork := make(chan error)
go func (errChanel chan<- error){
	// do some work, use mapOfEndpoints to signal addresses of other nodes
	// i.e. tell other node 1 that other node listens on 8002,
	// and tell node 2 that other nodes listens on 8001
	errChanel <- nil
}(someChannelThatDoesAllTheWork)

select {
	case <-proxy.Done():
	case <-someChannelThatDoesAllTheWork:
}
*/

type RulesManager struct {
	routingRule map[string]string

	firewalMutex sync.RWMutex
	firewallRule map[string]struct{}
}

func NewRulesManager(rules map[string]string) *RulesManager {
	return &RulesManager{
		routingRule:  rules,
		firewallRule: make(map[string]struct{}, 0),
	}
}

func (rm *RulesManager) AddFirewalRule(endpoint string) {
	log.Infof("adding firewall rule for %s", endpoint)

	rm.firewalMutex.Lock()
	rm.firewallRule[endpoint] = struct{}{}
	rm.firewalMutex.Unlock()
}

func (rm *RulesManager) RemoveFirewalRule(endpoint string) {
	log.Infof("removing firewall rule for %s", endpoint)

	rm.firewalMutex.Lock()
	delete(rm.firewallRule, endpoint)
	rm.firewalMutex.Unlock()
}

func (rm *RulesManager) GetRoute(src string) string {
	return rm.routingRule[src]
}

func (rm *RulesManager) IsEndpointUnderFirewall(endpoint string) bool {
	rm.firewalMutex.RLock()
	_, ok := rm.firewallRule[endpoint]
	rm.firewalMutex.RUnlock()

	return ok
}

type FrontendProxy struct {
	rulesManager     *RulesManager
	backendProxy     *BackendProxy
	servers          []*http.Server
	terminateChannel chan context.Context
	doneChannel      chan error
}

func (fp *FrontendProxy) AddFirewalRule(endpoint string) {
	fp.rulesManager.AddFirewalRule(endpoint)
}

func (fp *FrontendProxy) RemoveFirewalRule(endpoint string) {
	fp.rulesManager.RemoveFirewalRule(endpoint)
}

func (fp *FrontendProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	destination := r.Header.Get("Authority")
	if destination == "" {
		log.Errorf("no Authority header present while routing")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	sourceRoute := r.Header.Get("Origin")
	if sourceRoute == "" {
		log.Errorf("no Origin header present while routing")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	destinationRoute := fp.rulesManager.GetRoute(destination)
	if destinationRoute == "" {
		log.Errorf("no destination to route to")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if ok := fp.rulesManager.IsEndpointUnderFirewall(destinationRoute); ok {
		log.Debugf("destination %s under firewall", destinationRoute)
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	if ok := fp.rulesManager.IsEndpointUnderFirewall(sourceRoute); ok {
		log.Debugf("source %s under firewall", sourceRoute)
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	fp.backendProxy.ServeHTTP(w, r)
}

func (fp *FrontendProxy) ListenAndServe() {
	errChan := make(chan error)
	errChannelClosed := atomic.NewBool(false)

	// Launch servers
	for _, server := range fp.servers {
		go func(server *http.Server, ch chan<- error) {
			err := server.ListenAndServe()
			if err != nil && err != http.ErrServerClosed {
				if errChannelClosed.CAS(false, true) {
					ch <- err
					close(errChan)
				}
			}
		}(server, errChan)
	}

	// Wait for the termination of at least one server, or signal of the shutdown
	var err error
	go func() {
		// Shutdown the server
		select {
		case ctx := <-fp.terminateChannel:
			for _, server := range fp.servers {
				server.Shutdown(ctx)
			}
		case err = <-errChan:
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			// Closed server should allow calling shutdown
			for _, server := range fp.servers {
				server.Shutdown(ctx)
			}
		}

		fp.doneChannel <- err
		close(fp.doneChannel)
	}()
}

func (fp *FrontendProxy) Shutdown(ctx context.Context) error {
	var err error

	fp.terminateChannel <- ctx

	select {
	case <-ctx.Done():
		log.Errorf("context timed out while waiting for server shutdown")
	case doneError := <-fp.doneChannel:
		err = doneError
		log.Errorf("error occured while shutting down server: %+v", err)
	}

	close(fp.terminateChannel)

	return err
}

func (fp *FrontendProxy) Done() <-chan error {
	return fp.doneChannel
}

type BackendProxy struct {
	backends map[string]*httputil.ReverseProxy
}

func (bp *BackendProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.Debugf("serving http request to: %s", r.URL.Host)
	bp.backends[r.URL.Host].ServeHTTP(w, r)
}

func NewFrontendProxy(endpoints map[string]string,
	backendProxy *BackendProxy,
	rulesManager *RulesManager) *FrontendProxy {

	fp := &FrontendProxy{
		terminateChannel: make(chan context.Context),
		doneChannel:      make(chan error),
	}

	servers := make([]*http.Server, len(endpoints))

	count := 0
	for src, _ := range endpoints {
		servers[count] = &http.Server{
			Handler:      fp,
			Addr:         src,
			WriteTimeout: 15 * time.Second,
			ReadTimeout:  15 * time.Second,
		}
		count++
	}

	fp.servers = servers
	fp.backendProxy = backendProxy
	fp.rulesManager = rulesManager

	return fp
}

func NewBackendProxy(endpoints map[string]string) (*BackendProxy, error) {
	backends := make(map[string]*httputil.ReverseProxy, 0)

	for src, dest := range endpoints {
		urlEndpoint, err := url.Parse(dest)
		if err != nil {
			return nil, fmt.Errorf("unable to parse URL: %s", dest)
		}

		backends[src] = httputil.NewSingleHostReverseProxy(urlEndpoint)
	}

	return &BackendProxy{
		backends: backends,
	}, nil
}

func StartTestProxy(endpoints []string) (map[string]string, *FrontendProxy, error) {
	// Set server endpoints map
	mapOfEndpoints := make(map[string]string, len(endpoints))
	startEndpoint := 8001
	for i := 0; i != len(endpoints); i++ {
		src := fmt.Sprintf("localhost:%d", startEndpoint+i)
		mapOfEndpoints[src] = endpoints[i]
	}

	// Create new RulesManager and its associated server
	rulesManager := NewRulesManager(mapOfEndpoints)

	// Create new backend proxy
	backendProxy, err := NewBackendProxy(mapOfEndpoints)
	if err != nil {
		return mapOfEndpoints, nil, err
	}

	// Create new frontend proxy
	frontendProxy := NewFrontendProxy(mapOfEndpoints, backendProxy, rulesManager)

	// Start frontend proxy
	frontendProxy.ListenAndServe()

	return mapOfEndpoints, frontendProxy, nil
}
