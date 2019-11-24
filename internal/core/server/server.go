package server

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	logrus "github.com/sirupsen/logrus"
	"net/http"
	"time"
)

// Structs are separate for client and cluster because one day
// we might add some optional arguments to either one

type ClientCreateRequest struct {
	Key   string
	Value []byte
}

type ClientGetRequest struct {
	Key string
}

type ClientGetResponse struct {
	Value []byte
}

type ClusterStatusCode int

const (
	Ok             ClusterStatusCode = iota
	FailedNotFound ClusterStatusCode = iota
	FailedInternal ClusterStatusCode = iota
	Redirect       ClusterStatusCode = iota
)

type ClusterMessage struct {
	Error   error
	Address string
}

type ClusterRequest interface{}

type ClusterCreateRequest struct {
	Key   string
	Value []byte
}

type ClusterGetRequest struct {
	Key string
}

type ClusterRequestWrapper struct {
	Context         context.Context
	Request         ClusterRequest
	ResponseChannel chan<- ClusterResponse
}

type ClusterResponse interface{}

type ClusterCreateResponse struct {
	StatusCode ClusterStatusCode
	Message    ClusterMessage
}

type ClusterGetResponse struct {
	StatusCode ClusterStatusCode
	Message    ClusterMessage
	Value      []byte
}

type IExternalServer interface {
	Run() error
	Shutdown(ctx context.Context) error
	GetRequestChannel() (<-chan ClusterRequestWrapper, error)
}

type ExternalServer struct {
	server                *http.Server
	router                *mux.Router
	requestProcessChannel chan ClusterRequestWrapper
	logger                *logrus.Entry
}

func New(endpoint string, logger *logrus.Entry) *ExternalServer {
	externalServer := &ExternalServer{
		logger: logger,
	}

	r := mux.NewRouter()
	r.HandleFunc("/create", externalServer.handleCreate).Methods("POST")
	r.HandleFunc("/get", externalServer.handleGet).Methods("GET")

	s := &http.Server{
		Handler:      r,
		Addr:         endpoint,
		WriteTimeout: 15 * time.Second,
		ReadTimeout:  15 * time.Second,
	}

	externalServer.router = r
	externalServer.server = s

	return externalServer
}

func (s *ExternalServer) Shutdown(ctx context.Context) error {
	if err := s.server.Shutdown(ctx); err != nil {
		return err
	}

	return nil
}

func (s *ExternalServer) Run() error {
	if err := s.server.ListenAndServe(); err != http.ErrServerClosed {
		return err
	}

	return nil
}

func (s *ExternalServer) GetRequestChannel() (<-chan ClusterRequestWrapper, error) {
	if s.requestProcessChannel != nil {
		return nil, fmt.Errorf("request channel was already taken.")
	}

	s.requestProcessChannel = make(chan ClusterRequestWrapper)
	return s.requestProcessChannel, nil
}

func (s *ExternalServer) handleCreate(w http.ResponseWriter, r *http.Request) {
	clientRequest := &ClientCreateRequest{}
	err := json.NewDecoder(r.Body).Decode(clientRequest)
	if err != nil || !s.validateClientCreateRequest(clientRequest) {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	clusterRequest := ClusterCreateRequest{
		Key:   clientRequest.Key,
		Value: clientRequest.Value,
	}

	responseChannel := make(chan ClusterResponse)
	s.requestProcessChannel <- ClusterRequestWrapper{
		Context:         r.Context(),
		Request:         &clusterRequest,
		ResponseChannel: responseChannel,
	}

	clusterResponse := <-responseChannel

	switch responseTyped := clusterResponse.(type) {
	case ClusterCreateResponse:
		switch responseTyped.StatusCode {
		case Ok:
			w.WriteHeader(http.StatusOK)
			return
		case FailedInternal:
			w.WriteHeader(http.StatusInternalServerError)
			return
		case Redirect:
			http.Redirect(w, r, responseTyped.Message.Address, http.StatusFound)
			return
		}
	default:
		s.logger.Panicf("wrong response type. Received: %+v", responseTyped)
	}
}

func (s *ExternalServer) handleGet(w http.ResponseWriter, r *http.Request) {
	clientRequest := &ClientGetRequest{}
	err := json.NewDecoder(r.Body).Decode(clientRequest)
	if err != nil || !s.validateClientGetRequest(clientRequest) {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	clusterRequest := ClusterGetRequest{
		Key: clientRequest.Key,
	}

	responseChannel := make(chan ClusterResponse)
	s.requestProcessChannel <- ClusterRequestWrapper{
		Context:         r.Context(),
		Request:         &clusterRequest,
		ResponseChannel: responseChannel,
	}

	clusterResponse := <-responseChannel

	switch responseTyped := clusterResponse.(type) {
	case ClusterGetResponse:
		switch responseTyped.StatusCode {
		case Ok:
			clientResponse := ClientGetResponse{
				Value: responseTyped.Value,
			}
			buffer, err := json.Marshal(clientResponse)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}

			w.Write(buffer)
			return
		case FailedInternal:
			w.WriteHeader(http.StatusInternalServerError)
			return
		case FailedNotFound:
			w.WriteHeader(http.StatusNotFound)
			return
		case Redirect:
			http.Redirect(w, r, responseTyped.Message.Address, http.StatusFound)
			return
		}
	default:
		s.logger.Panicf("wrong response type. Received: %+v", responseTyped)
	}
}

func (s *ExternalServer) validateClientCreateRequest(request *ClientCreateRequest) bool {
	return request.Key != ""
}

func (s *ExternalServer) validateClientGetRequest(request *ClientGetRequest) bool {
	return request.Key != ""
}
