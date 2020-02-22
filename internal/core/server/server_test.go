package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	logrus "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"net/http/httptest"
)

func TestHandlersCreate(t *testing.T) {
	logrus.SetLevel(logrus.DebugLevel)

	logger := logrus.StandardLogger()
	logger.SetFormatter(&logrus.JSONFormatter{
		TimestampFormat: time.RFC3339Nano,
	})

	server := New("localhost:80", map[string]string{"Node0": "127.0.0.1:8001"},
		logger.WithFields(logrus.Fields{"component": "server"}))
	ch, err := server.GetRequestsChannels()
	assert.NoError(t, err)

	handler := http.HandlerFunc(server.handleCreate)

	testCases := []struct {
		name                   string
		clientCreateRequest    ClientCreateRequest
		clusterResponse        ClusterCreateResponse
		expectedClusterRequest *ClusterCreateRequest
		expectedStatusCode     int
	}{
		{
			name: "ok",
			clientCreateRequest: ClientCreateRequest{
				Key:   "key",
				Value: []byte("value"),
			},
			clusterResponse: ClusterCreateResponse{
				ClusterResponse: ClusterResponse{StatusCode: Ok},
			},
			expectedClusterRequest: &ClusterCreateRequest{
				Key:   "key",
				Value: []byte("value"),
			},
			expectedStatusCode: http.StatusOK,
		},
		{
			name: "redirect",
			clientCreateRequest: ClientCreateRequest{
				Key:   "key",
				Value: []byte("value"),
			},
			clusterResponse: ClusterCreateResponse{
				ClusterResponse: ClusterResponse{
					StatusCode: Redirect,
					LeaderName: "Node0",
				},
			},
			expectedClusterRequest: &ClusterCreateRequest{
				Key:   "key",
				Value: []byte("value"),
			},
			expectedStatusCode: http.StatusTemporaryRedirect,
		},
		{
			name: "failed",
			clientCreateRequest: ClientCreateRequest{
				Key:   "key",
				Value: []byte("value"),
			},
			clusterResponse: ClusterCreateResponse{
				ClusterResponse: ClusterResponse{StatusCode: FailedInternal},
			},
			expectedClusterRequest: &ClusterCreateRequest{
				Key:   "key",
				Value: []byte("value"),
			},
			expectedStatusCode: http.StatusInternalServerError,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			buffer, err := json.Marshal(testCase.clientCreateRequest)
			assert.NoError(t, err)

			reader := bytes.NewReader(buffer)
			req, err := http.NewRequest("POST", "/create", reader)
			assert.NoError(t, err)

			var clusterCreateRequest ClusterCreateRequest
			go func() {
				clusterCreateRequest = <-ch.ClusterCreateRequestChan
				clusterCreateRequest.ResponseChannel <- testCase.clusterResponse
			}()

			rr := httptest.NewRecorder()
			handler.ServeHTTP(rr, req)

			assert.Equal(t, testCase.expectedStatusCode, rr.Code)
			assert.Equal(t, testCase.expectedClusterRequest.Key, clusterCreateRequest.Key)
			assert.Equal(t, testCase.expectedClusterRequest.Value, clusterCreateRequest.Value)
		})
	}
}

func TestHandlersGet(t *testing.T) {
	logrus.SetLevel(logrus.DebugLevel)

	logger := logrus.StandardLogger()
	logger.SetFormatter(&logrus.JSONFormatter{
		TimestampFormat: time.RFC3339Nano,
	})

	server := New("localhost:80", map[string]string{"Node0": "127.0.0.1:8001"},
		logger.WithFields(logrus.Fields{"component": "server"}))
	ch, err := server.GetRequestsChannels()
	assert.NoError(t, err)

	handler := http.HandlerFunc(server.handleGet)

	testCases := []struct {
		name                   string
		clientGetRequest       ClientGetRequest
		clusterResponse        ClusterGetResponse
		expectedClusterRequest *ClusterGetRequest
		expectedStatusCode     int
	}{
		{
			name: "ok",
			clientGetRequest: ClientGetRequest{
				Key: "key",
			},
			clusterResponse: ClusterGetResponse{
				ClusterResponse: ClusterResponse{StatusCode: Ok},
				Value:           []byte("value"),
			},
			expectedClusterRequest: &ClusterGetRequest{
				Key: "key",
			},
			expectedStatusCode: http.StatusOK,
		},
		{
			name: "redirect",
			clientGetRequest: ClientGetRequest{
				Key: "key",
			},
			clusterResponse: ClusterGetResponse{
				ClusterResponse: ClusterResponse{
					StatusCode: Redirect,
					LeaderName: "Node0",
				},
			},
			expectedClusterRequest: &ClusterGetRequest{
				Key: "key",
			},
			expectedStatusCode: http.StatusTemporaryRedirect,
		},
		{
			name: "failed",
			clientGetRequest: ClientGetRequest{
				Key: "key",
			},
			clusterResponse: ClusterGetResponse{
				ClusterResponse: ClusterResponse{StatusCode: FailedInternal},
			},
			expectedClusterRequest: &ClusterGetRequest{
				Key: "key",
			},
			expectedStatusCode: http.StatusInternalServerError,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			buffer, err := json.Marshal(testCase.clientGetRequest)
			assert.NoError(t, err)

			reader := bytes.NewReader(buffer)
			req, err := http.NewRequest("POST", "/create", reader)
			assert.NoError(t, err)

			var clusterGetRequest ClusterGetRequest
			go func() {
				clusterGetRequest = <-ch.ClusterGetRequestChan
				clusterGetRequest.ResponseChannel <- testCase.clusterResponse
			}()

			rr := httptest.NewRecorder()
			handler.ServeHTTP(rr, req)

			assert.Equal(t, testCase.expectedStatusCode, rr.Code)
			assert.Equal(t, testCase.expectedClusterRequest.Key, clusterGetRequest.Key)

			if testCase.clusterResponse.Value != nil {
				body := rr.Body.String()
				response := &ClientGetResponse{}
				err := json.Unmarshal([]byte(body), response)
				assert.NoError(t, err)

				assert.Equal(t, testCase.clusterResponse.Value, response.Value)
			}
		})
	}
}

func TestHandlersDelete(t *testing.T) {
	logrus.SetLevel(logrus.DebugLevel)

	logger := logrus.StandardLogger()
	logger.SetFormatter(&logrus.JSONFormatter{
		TimestampFormat: time.RFC3339Nano,
	})

	server := New("localhost:80", map[string]string{"Node0": "127.0.0.1:8001"},
		logger.WithFields(logrus.Fields{"component": "server"}))
	ch, err := server.GetRequestsChannels()
	assert.NoError(t, err)

	handler := http.HandlerFunc(server.handleDelete)

	testCases := []struct {
		name                   string
		clientDeleteRequest    ClientDeleteRequest
		clusterResponse        ClusterDeleteResponse
		expectedClusterRequest *ClusterDeleteRequest
		expectedStatusCode     int
	}{
		{
			name: "ok",
			clientDeleteRequest: ClientDeleteRequest{
				Key: "key",
			},
			clusterResponse: ClusterDeleteResponse{
				ClusterResponse: ClusterResponse{StatusCode: Ok},
			},
			expectedClusterRequest: &ClusterDeleteRequest{
				Key: "key",
			},
			expectedStatusCode: http.StatusOK,
		},
		{
			name: "redirect",
			clientDeleteRequest: ClientDeleteRequest{
				Key: "key",
			},
			clusterResponse: ClusterDeleteResponse{
				ClusterResponse: ClusterResponse{
					StatusCode: Redirect,
					LeaderName: "Node0",
				},
			},
			expectedClusterRequest: &ClusterDeleteRequest{
				Key: "key",
			},
			expectedStatusCode: http.StatusTemporaryRedirect,
		},
		{
			name: "failed",
			clientDeleteRequest: ClientDeleteRequest{
				Key: "key",
			},
			clusterResponse: ClusterDeleteResponse{
				ClusterResponse: ClusterResponse{StatusCode: FailedInternal},
			},
			expectedClusterRequest: &ClusterDeleteRequest{
				Key: "key",
			},
			expectedStatusCode: http.StatusInternalServerError,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			buffer, err := json.Marshal(testCase.clientDeleteRequest)
			assert.NoError(t, err)

			reader := bytes.NewReader(buffer)
			req, err := http.NewRequest("POST", "/delete", reader)
			assert.NoError(t, err)

			var clusterDeleteRequest ClusterDeleteRequest
			go func() {
				clusterDeleteRequest = <-ch.ClusterDeleteRequestChan
				clusterDeleteRequest.ResponseChannel <- testCase.clusterResponse
			}()

			rr := httptest.NewRecorder()
			handler.ServeHTTP(rr, req)

			assert.Equal(t, testCase.expectedStatusCode, rr.Code)
			assert.Equal(t, testCase.expectedClusterRequest.Key, clusterDeleteRequest.Key)
		})
	}
}
