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

	server := NewExternalServer("localhost:80", logger.WithFields(logrus.Fields{
		"component": "server",
	}))
	ch, err := server.GetRequestChannel()
	assert.NoError(t, err)

	handler := http.HandlerFunc(server.handleCreate)

	testCases := []struct {
		name                   string
		clientCreateRequest ClientCreateRequest
		clusterResponse        ClusterCreateResponse
		expectedClusterRequest *ClusterCreateRequest
		expectedStatusCode int
	}{
		{
			name: "ok",
			clientCreateRequest: ClientCreateRequest{
				Key:   "key",
				Value: []byte("value"),
			},
			clusterResponse: ClusterCreateResponse{
				StatusCode: Ok,
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
				StatusCode: Redirect,
				Message: ClusterMessage{
					Address: "127.0.0.1",
				},
			},
			expectedClusterRequest: &ClusterCreateRequest{
				Key:   "key",
				Value: []byte("value"),
			},
			expectedStatusCode: http.StatusFound,
		},
		{
			name: "failed",
			clientCreateRequest: ClientCreateRequest{
				Key:   "key",
				Value: []byte("value"),
			},
			clusterResponse: ClusterCreateResponse{
				StatusCode: Failed,
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

			var clusterRequest ClusterRequestWrapper
			go func() {
				clusterRequest = <-ch
				clusterRequest.ResponseChannel <- testCase.clusterResponse
			}()

			rr := httptest.NewRecorder()
			handler.ServeHTTP(rr, req)

			assert.Equal(t, testCase.expectedStatusCode, rr.Code)
			assert.Equal(t, testCase.expectedClusterRequest, clusterRequest.Request)
		})
	}
}

func TestHandlersGet(t *testing.T) {
	logrus.SetLevel(logrus.DebugLevel)

	logger := logrus.StandardLogger()
	logger.SetFormatter(&logrus.JSONFormatter{
		TimestampFormat: time.RFC3339Nano,
	})

	server := NewExternalServer("localhost:80", logger.WithFields(logrus.Fields{
		"component": "server",
	}))
	ch, err := server.GetRequestChannel()
	assert.NoError(t, err)

	handler := http.HandlerFunc(server.handleGet)

	testCases := []struct {
		name                   string
		clientGetRequest ClientGetRequest
		clusterResponse        ClusterGetResponse
		expectedClusterRequest *ClusterGetRequest
		expectedStatusCode int
	}{
		{
			name: "ok",
			clientGetRequest: ClientGetRequest{
				Key:   "key",
			},
			clusterResponse: ClusterGetResponse{
				StatusCode: Ok,
				Value: []byte("value"),
			},
			expectedClusterRequest: &ClusterGetRequest{
				Key:   "key",
			},
			expectedStatusCode: http.StatusOK,
		},
		{
			name: "redirect",
			clientGetRequest: ClientGetRequest{
				Key:   "key",
			},
			clusterResponse: ClusterGetResponse{
				StatusCode: Redirect,
				Message: ClusterMessage{
					Address: "127.0.0.1",
				},
			},
			expectedClusterRequest: &ClusterGetRequest{
				Key:   "key",
			},
			expectedStatusCode: http.StatusFound,
		},
		{
			name: "failed",
			clientGetRequest: ClientGetRequest{
				Key:   "key",
			},
			clusterResponse: ClusterGetResponse{
				StatusCode: Failed,
			},
			expectedClusterRequest: &ClusterGetRequest{
				Key:   "key",
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

			var clusterRequest ClusterRequestWrapper
			go func() {
				clusterRequest = <-ch
				clusterRequest.ResponseChannel <- testCase.clusterResponse
			}()

			rr := httptest.NewRecorder()
			handler.ServeHTTP(rr, req)

			assert.Equal(t, testCase.expectedStatusCode, rr.Code)
			assert.Equal(t, testCase.expectedClusterRequest, clusterRequest.Request)

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
