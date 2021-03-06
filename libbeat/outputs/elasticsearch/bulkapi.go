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
	"bytes"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
)

// MetaBuilder creates meta data for bulk requests
type MetaBuilder func(interface{}) interface{}

type bulkRequest struct {
	requ *http.Request
}

type bulkResult struct {
	raw []byte
}

// Bulk performs many index/delete operations in a single API call.
// Implements: http://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html
func (conn *Connection) Bulk(
	index, docType string,
	params map[string]string, body []interface{},
) (*QueryResult, error) {
	return conn.BulkWith(index, docType, params, nil, body)
}

// BulkWith creates a HTTP request containing a bunch of operations and send
// them to Elasticsearch. The request is retransmitted up to max_retries before
// returning an error.
func (conn *Connection) BulkWith(
	index string,
	docType string,
	params map[string]string,
	metaBuilder MetaBuilder,
	body []interface{},
) (*QueryResult, error) {
	if len(body) == 0 {
		return nil, nil
	}

	enc := conn.encoder
	enc.Reset()
	if err := bulkEncode(enc, metaBuilder, body); err != nil {
		return nil, err
	}

	requ, err := newBulkRequest(conn.URL, index, docType, params, enc)
	if err != nil {
		return nil, err
	}

	_, result, err := conn.sendBulkRequest(requ)
	if err != nil {
		return nil, err
	}
	return readQueryResult(result.raw)
}

func newBulkRequest(
	urlStr string,
	index, docType string,
	params map[string]string,
	body bodyEncoder,
) (*bulkRequest, error) {
	path, err := makePath(index, docType, "_bulk")
	if err != nil {
		return nil, err
	}

	url := addToURL(urlStr, path, "", params)

	var reader io.Reader
	if body != nil {
		reader = body.Reader()
	}

	requ, err := http.NewRequest("POST", url, reader)
	if err != nil {
		return nil, err
	}

	if body != nil {
		body.AddHeader(&requ.Header)
	}

	return &bulkRequest{
		requ: requ,
	}, nil
}

func (r *bulkRequest) Reset(body bodyEncoder) {
	bdy := body.Reader()

	rc, ok := bdy.(io.ReadCloser)
	if !ok && body != nil {
		rc = ioutil.NopCloser(bdy)
	}

	switch v := bdy.(type) {
	case *bytes.Buffer:
		r.requ.ContentLength = int64(v.Len())
	case *bytes.Reader:
		r.requ.ContentLength = int64(v.Len())
	case *strings.Reader:
		r.requ.ContentLength = int64(v.Len())
	}

	r.requ.Header = http.Header{}
	r.requ.Body = rc

	body.AddHeader(&r.requ.Header)
}

func (conn *Connection) sendBulkRequest(requ *bulkRequest) (int, bulkResult, error) {
	status, resp, err := conn.execHTTPRequest(requ.requ)
	if err != nil {
		return status, bulkResult{}, err
	}

	result, err := readBulkResult(resp)
	return status, result, err
}

func readBulkResult(obj []byte) (bulkResult, error) {
	return bulkResult{obj}, nil
}

func bulkEncode(out bulkWriter, metaBuilder MetaBuilder, body []interface{}) error {
	if metaBuilder == nil {
		for _, obj := range body {
			if err := out.AddRaw(obj); err != nil {
				return err
			}
		}
	} else {
		for _, obj := range body {
			meta := metaBuilder(obj)
			if err := out.Add(meta, obj); err != nil {
			}
		}
	}
	return nil
}
