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

package elasticsearch

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"

	"github.com/elastic/migrate-management-beats/libbeat/common"
	"github.com/elastic/migrate-management-beats/libbeat/outputs/transport"
)

// Client is an elasticsearch client.
type Client struct {
	Connection
	tlsConfig *transport.TLSConfig

	params    map[string]string
	timeout   time.Duration
	eventType string

	// buffered json response reader
	json jsonReader

	// additional configs
	compressionLevel int
	proxyURL         *url.URL
}

// ClientSettings contains the settings for a client.
type ClientSettings struct {
	URL                string
	Proxy              *url.URL
	TLS                *transport.TLSConfig
	Username, Password string
	EscapeHTML         bool
	Parameters         map[string]string
	Headers            map[string]string
	Timeout            time.Duration
	CompressionLevel   int
}

// Connection manages the connection for a given client.
type Connection struct {
	URL      string
	Username string
	Password string
	Headers  map[string]string

	http              *http.Client
	onConnectCallback func() error

	encoder bodyEncoder
	version common.Version
}

// NewClient instantiates a new client.
func NewClient(s ClientSettings) (*Client, error) {
	proxy := http.ProxyFromEnvironment
	if s.Proxy != nil {
		proxy = http.ProxyURL(s.Proxy)
	}

	u, err := url.Parse(s.URL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse elasticsearch URL: %v", err)
	}
	if u.User != nil {
		s.Username = u.User.Username()
		s.Password, _ = u.User.Password()
		u.User = nil

		// Re-write URL without credentials.
		s.URL = u.String()
	}

	// TODO: add socks5 proxy support
	var dialer, tlsDialer transport.Dialer

	dialer = transport.NetDialer(s.Timeout)
	tlsDialer, err = transport.TLSDialer(dialer, s.TLS, s.Timeout)
	if err != nil {
		return nil, err
	}

	params := s.Parameters
	_, err = newBulkRequest(s.URL, "", "", params, nil)
	if err != nil {
		return nil, err
	}

	var encoder bodyEncoder
	compression := s.CompressionLevel
	if compression == 0 {
		encoder = newJSONEncoder(nil, s.EscapeHTML)
	} else {
		encoder, err = newGzipEncoder(compression, nil, s.EscapeHTML)
		if err != nil {
			return nil, err
		}
	}

	client := &Client{
		Connection: Connection{
			URL:      s.URL,
			Username: s.Username,
			Password: s.Password,
			Headers:  s.Headers,
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
		tlsConfig: s.TLS,
		params:    params,
		timeout:   s.Timeout,
		eventType: "_doc",

		compressionLevel: compression,
		proxyURL:         s.Proxy,
	}

	return client, nil
}

// LoadJSON creates a PUT request based on a JSON document.
func (client *Client) LoadJSON(path string, json map[string]interface{}) ([]byte, error) {
	status, body, err := client.Request("PUT", path, "", nil, json)
	if err != nil {
		return body, fmt.Errorf("couldn't load json. Error: %s", err)
	}
	if status > 300 {
		return body, fmt.Errorf("couldn't load json. Status: %v", status)
	}

	return body, nil
}

// GetVersion returns the elasticsearch version the client is connected to.
func (client *Client) GetVersion() common.Version {
	return client.Connection.version
}

func (client *Client) String() string {
	return "elasticsearch(" + client.Connection.URL + ")"
}

// Connect connects the client. It runs a GET request against the root URL of
// the configured host, updates the known Elasticsearch version and calls
// globally configured handlers.
func (client *Client) Connect() error {
	err := client.Connection.Connect()
	if err != nil {
		return err
	}

	client.eventType = "_doc"
	return nil
}

// Connect connects the client. It runs a GET request against the root URL of
// the configured host, updates the known Elasticsearch version and calls
// globally configured handlers.
func (conn *Connection) Connect() error {
	versionString, err := conn.Ping()
	if err != nil {
		return err
	}

	if version, err := common.NewVersion(versionString); err != nil {
		conn.version = common.Version{}
	} else {
		conn.version = *version
	}

	return nil
}

// Ping sends a GET request to the Elasticsearch.
func (conn *Connection) Ping() (string, error) {

	status, body, err := conn.execRequest("GET", conn.URL, nil)
	if err != nil {
		return "", err
	}

	if status >= 300 {
		return "", fmt.Errorf("Non 2xx response code: %d", status)
	}

	var response struct {
		Version struct {
			Number string
		}
	}

	err = json.Unmarshal(body, &response)
	if err != nil {
		return "", fmt.Errorf("Failed to parse JSON response: %v", err)
	}

	return response.Version.Number, nil
}

// Close closes a connection.
func (conn *Connection) Close() error {
	return nil
}

// Request sends a request via the connection.
func (conn *Connection) Request(
	method, path string,
	pipeline string,
	params map[string]string,
	body interface{},
) (int, []byte, error) {

	url := addToURL(conn.URL, path, pipeline, params)

	return conn.RequestURL(method, url, body)
}

// RequestURL sends a request with the connection object to an alternative url
func (conn *Connection) RequestURL(
	method, url string,
	body interface{},
) (int, []byte, error) {

	if body == nil {
		return conn.execRequest(method, url, nil)
	}

	if err := conn.encoder.Marshal(body); err != nil {
		return 0, nil, ErrJSONEncodeFailed
	}
	return conn.execRequest(method, url, conn.encoder.Reader())
}

func (conn *Connection) execRequest(
	method, url string,
	body io.Reader,
) (int, []byte, error) {
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return 0, nil, err
	}
	if body != nil {
		conn.encoder.AddHeader(&req.Header)
	}
	return conn.execHTTPRequest(req)
}

func (conn *Connection) execHTTPRequest(req *http.Request) (int, []byte, error) {
	req.Header.Add("Accept", "application/json")
	if conn.Username != "" || conn.Password != "" {
		req.SetBasicAuth(conn.Username, conn.Password)
	}

	for name, value := range conn.Headers {
		req.Header.Add(name, value)
	}

	// The stlib will override the value in the header based on the configured `Host`
	// on the request which default to the current machine.
	//
	// We use the normalized key header to retrieve the user configured value and assign it to the host.
	if host := req.Header.Get("Host"); host != "" {
		req.Host = host
	}

	resp, err := conn.http.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer closing(resp.Body)

	status := resp.StatusCode
	obj, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return status, nil, err
	}

	if status >= 300 {
		// add the response body with the error returned by Elasticsearch
		err = fmt.Errorf("%v: %s", resp.Status, obj)
	}

	return status, obj, err
}

// GetVersion returns the elasticsearch version the client is connected to.
// The version is read and updated on 'Connect'.
func (conn *Connection) GetVersion() common.Version {
	return conn.version
}

func closing(c io.Closer) {
	err := c.Close()
	if err != nil {
	}
}
