// Licensed to Elasticsearch B.V. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Elasticsearch B.V. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package http

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/common"
	"github.com/elastic/beats/v7/libbeat/common/transport"
	"github.com/elastic/beats/v7/libbeat/common/transport/tlscommon"
	"github.com/elastic/beats/v7/libbeat/outputs"
	"github.com/elastic/beats/v7/libbeat/outputs/outil"
	"github.com/elastic/beats/v7/libbeat/publisher"
)

// Client struct
type Client struct {
	Connection
	tlsConfig *tlscommon.TLSConfig
	params    map[string]string
	// additional configs
	compressionLevel int
	proxyURL         *url.URL
	batchPublish     bool
	pathPrefix       string
	pathField        string
	pathSuffix       string
	observer         outputs.Observer
	headers          map[string]string
	format           string
}

// ClientSettings struct
type ClientSettings struct {
	URL                string
	PathPrefix         string
	PathField          string
	PathSuffix         string
	Proxy              *url.URL
	TLS                *tlscommon.TLSConfig
	Username, Password string
	Parameters         map[string]string
	Index              outil.Selector
	Pipeline           *outil.Selector
	Timeout            time.Duration
	CompressionLevel   int
	Observer           outputs.Observer
	BatchPublish       bool
	Headers            map[string]string
	ContentType        string
	Format             string
}

// Connection struct
type Connection struct {
	URL         string
	Username    string
	Password    string
	http        *http.Client
	connected   bool
	encoder     bodyEncoder
	ContentType string
}

type eventRaw map[string]json.RawMessage

type event struct {
	Timestamp time.Time     `json:"@timestamp"`
	Fields    common.MapStr `json:"-"`
}

// NewClient instantiate a client.
func NewClient(s ClientSettings) (*Client, error) {
	proxy := http.ProxyFromEnvironment
	if s.Proxy != nil {
		proxy = http.ProxyURL(s.Proxy)
	}
	logger.Info("HTTP URL: %s", s.URL)
	var dialer, tlsDialer transport.Dialer
	var err error

	dialer = transport.NetDialer(s.Timeout)
	tlsDialer, err = transport.TLSDialer(dialer, s.TLS, s.Timeout)

	if err != nil {
		return nil, err
	}

	if st := s.Observer; st != nil {
		dialer = transport.StatsDialer(dialer, st)
		tlsDialer = transport.StatsDialer(tlsDialer, st)
	}
	params := s.Parameters
	var encoder bodyEncoder
	compression := s.CompressionLevel
	if compression == 0 {
		switch s.Format {
		case "json":
			encoder = newJSONEncoder(nil)
		case "json_lines":
			encoder = newJSONLinesEncoder(nil)
		}
	} else {
		switch s.Format {
		case "json":
			encoder, err = newGzipEncoder(compression, nil)
		case "json_lines":
			encoder, err = newGzipLinesEncoder(compression, nil)
		}
		if err != nil {
			return nil, err
		}
	}
	client := &Client{
		Connection: Connection{
			URL:         s.URL,
			Username:    s.Username,
			Password:    s.Password,
			ContentType: s.ContentType,
			http: &http.Client{
				Transport: &http.Transport{
					Dial:    dialer.Dial,
					DialTLS: tlsDialer.Dial,
					Proxy:   proxy,
				},
				Timeout: s.Timeout,
			},
			encoder: encoder,
		},
		params:           params,
		compressionLevel: compression,
		proxyURL:         s.Proxy,
		batchPublish:     s.BatchPublish,
		pathPrefix:       s.PathPrefix,
		pathField:        s.PathField,
		pathSuffix:       s.PathSuffix,
		headers:          s.Headers,
		format:           s.Format,
	}

	return client, nil
}

// Clone clones a client.
func (client *Client) Clone() *Client {
	// when cloning the connection callback and params are not copied. A
	// client's close is for example generated for topology-map support. With params
	// most likely containing the ingest node pipeline and default callback trying to
	// create install a template, we don't want these to be included in the clone.
	c, _ := NewClient(
		ClientSettings{
			URL:              client.URL,
			PathPrefix:       client.pathPrefix,
			PathField:        client.pathField,
			PathSuffix:       client.pathSuffix,
			Proxy:            client.proxyURL,
			TLS:              client.tlsConfig,
			Username:         client.Username,
			Password:         client.Password,
			Parameters:       client.params,
			Timeout:          client.http.Timeout,
			CompressionLevel: client.compressionLevel,
			BatchPublish:     client.batchPublish,
			Headers:          client.headers,
			ContentType:      client.ContentType,
			Format:           client.format,
		},
	)
	return c
}

// Connect establishes a connection to the clients sink.
func (conn *Connection) Connect() error {
	conn.connected = true
	return nil
}

// Close closes a connection.
func (conn *Connection) Close() error {
	conn.connected = false
	return nil
}

func (client *Client) String() string {
	return client.URL
}

// Publish sends events to the clients sink.
func (client *Client) Publish(_ context.Context, batch publisher.Batch) error {
	events := batch.Events()
	rest, err := client.publishEvents(events)
	if len(rest) == 0 {
		batch.ACK()
	} else {
		batch.RetryEvents(rest)
	}
	return err
}

// PublishEvents posts all events to the http endpoint. On error a slice with all
// events not published will be returned.
func (client *Client) publishEvents(data []publisher.Event) ([]publisher.Event, error) {
	begin := time.Now()
	if len(data) == 0 {
		return nil, nil
	}
	if !client.connected {
		return data, ErrNotConnected
	}
	var failedEvents []publisher.Event
	sendErr := error(nil)
	if client.batchPublish {
		// Publish events in bulk
		logger.Debugf("Publishing events in batch.")
		sendErr = client.BatchPublishEvent(data)
		if sendErr != nil {
			return data, sendErr
		}
	} else {
		logger.Debugf("Publishing events one by one.")
		for index, event := range data {
			sendErr = client.PublishEvent(event)
			if sendErr != nil {
				// return the rest of the data with the error
				failedEvents = data[index:]
				break
			}
		}
	}
	logger.Debugf("PublishEvents: %d metrics have been published over HTTP in %v.", len(data), time.Now().Sub(begin))
	if len(failedEvents) > 0 {
		return failedEvents, sendErr
	}
	return nil, nil
}

// BatchPublishEvent publish a single event to output.
func (client *Client) BatchPublishEvent(data []publisher.Event) error {
	if !client.connected {
		return ErrNotConnected
	}
	urlGroups, err := client.groupEventsByURL(data)
	if err != nil {
		return err
	}

	for _, group := range urlGroups {
		events := make([]eventRaw, len(group.events))
		for i, event := range group.events {
			events[i] = client.makeEvent(&event.Content)
		}
		status, _, err := client.requestURL("POST", group.url, client.params, events, client.headers)
		if err != nil {
			logger.Warn("Fail to insert a single event: %s", err)
			if err == ErrJSONEncodeFailed {
				// don't retry unencodable values
				return nil
			}
		}
		switch {
		case status == 500 || status == 400: // server error or bad input, don't retry
			continue
		case status >= 300:
			// retry
			return err
		}
	}
	return nil
}

// PublishEvent publish a single event to output.
func (client *Client) PublishEvent(data publisher.Event) error {
	if !client.connected {
		return ErrNotConnected
	}
	event := data
	logger.Debugf("Publish event: %s", event)
	requestURL, err := client.resolveRequestURL(event)
	if err != nil {
		return err
	}
	status, _, err := client.requestURL("POST", requestURL, client.params, client.makeEvent(&event.Content), client.headers)
	if err != nil {
		logger.Warn("Fail to insert a single event: %s", err)
		if err == ErrJSONEncodeFailed {
			// don't retry unencodable values
			return nil
		}
	}
	switch {
	case status == 500 || status == 400: // server error or bad input, don't retry
		return nil
	case status >= 300:
		// retry
		return err
	}
	if !client.connected {
		return ErrNotConnected
	}
	return nil
}

func (conn *Connection) request(method string, params map[string]string, body interface{}, headers map[string]string) (int, []byte, error) {
	return conn.requestURL(method, conn.URL, params, body, headers)
}

func (conn *Connection) requestURL(method, requestURL string, params map[string]string, body interface{}, headers map[string]string) (int, []byte, error) {
	urlStr := addToURL(requestURL, params)
	logger.Debugf("%s %s %v", method, urlStr, body)

	if body == nil {
		return conn.execRequest(method, urlStr, nil, headers)
	}

	if err := conn.encoder.Marshal(body); err != nil {
		logger.Warn("Failed to json encode body (%v): %#v", err, body)
		return 0, nil, ErrJSONEncodeFailed
	}
	return conn.execRequest(method, urlStr, conn.encoder.Reader(), headers)
}

type urlEventGroup struct {
	url    string
	events []publisher.Event
}

func (client *Client) groupEventsByURL(data []publisher.Event) ([]urlEventGroup, error) {
	groups := make(map[string][]publisher.Event)
	order := make([]string, 0)
	for _, event := range data {
		requestURL, err := client.resolveRequestURL(event)
		if err != nil {
			return nil, err
		}
		if _, exists := groups[requestURL]; !exists {
			order = append(order, requestURL)
		}
		groups[requestURL] = append(groups[requestURL], event)
	}

	result := make([]urlEventGroup, 0, len(order))
	for _, requestURL := range order {
		result = append(result, urlEventGroup{
			url:    requestURL,
			events: groups[requestURL],
		})
	}
	return result, nil
}

func (client *Client) resolveRequestURL(event publisher.Event) (string, error) {
	if client.pathPrefix == "" || client.pathField == "" {
		return client.URL, nil
	}

	value, err := event.Content.Fields.GetValue(client.pathField)
	if err != nil {
		return client.staticOrError()
	}

	stream, ok := value.(string)
	if !ok || strings.TrimSpace(stream) == "" {
		return client.staticOrError()
	}

	parsedURL, err := url.Parse(client.URL)
	if err != nil {
		return "", err
	}
	parsedURL.Path = joinURLPath(client.pathPrefix, stream) + client.pathSuffix
	parsedURL.RawPath = ""
	return parsedURL.String(), nil
}

func (client *Client) staticOrError() (string, error) {
	parsedURL, err := url.Parse(client.URL)
	if err != nil {
		return "", err
	}
	if parsedURL.Path != "" && parsedURL.Path != "/" {
		return client.URL, nil
	}
	return "", fmt.Errorf("missing dynamic path field %q and no static path fallback configured", client.pathField)
}

func joinURLPath(parts ...string) string {
	joined := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.Trim(part, "/")
		if trimmed == "" {
			continue
		}
		joined = append(joined, trimmed)
	}
	if len(joined) == 0 {
		return "/"
	}
	return "/" + strings.Join(joined, "/")
}

func (client *Client) makeEvent(v *beat.Event) eventRaw {
	if client.pathPrefix == "" || client.pathField == "" {
		return makeEvent(v)
	}
	return makeEventWithoutFields(v, map[string]struct{}{
		client.pathField: {},
	})
}

func (conn *Connection) execRequest(method, url string, body io.Reader, headers map[string]string) (int, []byte, error) {
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		logger.Warn("Failed to create request: %v", err)
		return 0, nil, err
	}
	if body != nil {
		conn.encoder.AddHeader(&req.Header, conn.ContentType)
	}
	return conn.execHTTPRequest(req, headers)
}

func (conn *Connection) execHTTPRequest(req *http.Request, headers map[string]string) (int, []byte, error) {
	req.Header.Add("Accept", "application/json")
	for key, value := range headers {
		req.Header.Add(key, value)
	}
	if conn.Username != "" || conn.Password != "" {
		req.SetBasicAuth(conn.Username, conn.Password)
	}
	resp, err := conn.http.Do(req)
	if err != nil {
		conn.connected = false
		return 0, nil, err
	}
	defer closing(resp.Body)

	status := resp.StatusCode
	if status >= 300 {
		conn.connected = false
		return status, nil, fmt.Errorf("%v", resp.Status)
	}
	obj, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		conn.connected = false
		return status, nil, err
	}
	return status, obj, nil
}

func closing(c io.Closer) {
	err := c.Close()
	if err != nil {
		logger.Warn("Close failed with: %v", err)
	}
}

// this should ideally be in enc.go
func makeEvent(v *beat.Event) map[string]json.RawMessage {
	return makeEventWithoutFields(v, nil)
}

func makeEventWithoutFields(v *beat.Event, excludedFields map[string]struct{}) map[string]json.RawMessage {
	// Inline not supported,
	// HT: https://stackoverflow.com/questions/49901287/embed-mapstringstring-in-go-json-marshaling-without-extra-json-property-inlin
	type event0 event // prevent recursion
	e := event{Timestamp: v.Timestamp.UTC(), Fields: v.Fields}
	b, err := json.Marshal(event0(e))
	if err != nil {
		logger.Warn("Error encoding event to JSON: %v", err)
	}

	var eventMap map[string]json.RawMessage
	err = json.Unmarshal(b, &eventMap)
	if err != nil {
		logger.Warn("Error decoding JSON to map: %v", err)
	}
	// Add the individual fields to the map, flatten "Fields"
	for j, k := range e.Fields {
		if _, excluded := excludedFields[j]; excluded {
			continue
		}
		b, err = json.Marshal(k)
		if err != nil {
			logger.Warn("Error encoding map to JSON: %v", err)
		}
		eventMap[j] = b
	}
	return eventMap
}
