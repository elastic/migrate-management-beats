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
	"strconv"
)

// QueryResult contains the result of a query.
type QueryResult struct {
	Ok           bool            `json:"ok"`
	Index        string          `json:"_index"`
	Type         string          `json:"_type"`
	ID           string          `json:"_id"`
	Source       json.RawMessage `json:"_source"`
	Version      int             `json:"_version"`
	Exists       bool            `json:"exists"`
	Found        bool            `json:"found"`   // Only used prior to ES 6. You must also check for Result == "found".
	Created      bool            `json:"created"` // Only used prior to ES 6. You must also check for Result == "created".
	Result       string          `json:"result"`  // Only used in ES 6+.
	Acknowledged bool            `json:"acknowledged"`
	Matches      []string        `json:"matches"`
}

// IndexResult contains the result of an index request.
type IndexResult struct {
	Took             int  `json:"took"`
	TimedOut         bool `json:"timed_out"`
	Created          int  `json:"created"`
	Updated          int  `json:"updated"`
	Deleted          int  `json:"deleted"`
	Batches          int  `json:"batches"`
	VersionConflicts int  `json:"version_conflicts"`
	Noops            int  `json:"noops"`
	Retries          struct {
		Bulk   int `json:"bulk"`
		Search int `json:"search"`
	} `json:"retries"`
	ThrottledMillis      int      `json:"throttled_millies"`
	RequestsPerSecond    float64  `json:"requests_per_seconds"`
	ThrottledUntilMillis int      `json:"throttled_until_millis"`
	Total                int      `json:"total"`
	Failures             []string `json:"failures"`
}

// SearchResults contains the results of a search.
type SearchResults struct {
	Took   int                        `json:"took"`
	Shards json.RawMessage            `json:"_shards"`
	Hits   Hits                       `json:"hits"`
	Aggs   map[string]json.RawMessage `json:"aggregations"`
}

// Hits contains the hits.
type Hits struct {
	Total Total
	Hits  []json.RawMessage `json:"hits"`
}

// Total contains the number of element fetched and the relation.
type Total struct {
	Value    int    `json:"value"`
	Relation string `json:"relation"`
}

// UnmarshalJSON correctly unmarshal the hits response between ES 6.0 and ES 7.0.
func (t *Total) UnmarshalJSON(b []byte) error {
	value := struct {
		Value    int    `json:"value"`
		Relation string `json:"relation"`
	}{}

	if err := json.Unmarshal(b, &value); err == nil {
		*t = value
		return nil
	}

	// fallback for Elasticsearch < 7
	if i, err := strconv.Atoi(string(b)); err == nil {
		*t = Total{Value: i, Relation: "eq"}
		return nil
	}

	return fmt.Errorf("could not unmarshal JSON value '%s'", string(b))
}

// CountResults contains the count of results.
type CountResults struct {
	Count  int             `json:"count"`
	Shards json.RawMessage `json:"_shards"`
}

func withQueryResult(status int, resp []byte, err error) (int, *QueryResult, error) {
	if err != nil {
		return status, nil, fmt.Errorf("Elasticsearch response: %s; %+v", resp, err)
	}
	result, err := readQueryResult(resp)
	return status, result, err
}

func readQueryResult(obj []byte) (*QueryResult, error) {
	var result QueryResult
	if obj == nil {
		return nil, nil
	}

	err := json.Unmarshal(obj, &result)
	if err != nil {
		return nil, err
	}
	return &result, err
}

func readSearchResult(obj []byte) (*SearchResults, error) {
	var result SearchResults
	if obj == nil {
		return nil, nil
	}

	err := json.Unmarshal(obj, &result)
	if err != nil {
		return nil, err
	}
	return &result, err
}

func readCountResult(obj []byte) (*CountResults, error) {
	if obj == nil {
		return nil, nil
	}

	var result CountResults
	err := json.Unmarshal(obj, &result)
	if err != nil {
		return nil, err
	}
	return &result, err
}

func withIndexResult(status int, resp []byte, err error) (int, *IndexResult, error) {
	if err != nil {
		return status, nil, fmt.Errorf("Elasticsearch response: %s; error: %+v", resp, err)
	}
	result, err := readIndexResult(resp)
	return status, result, err
}

func readIndexResult(obj []byte) (*IndexResult, error) {
	var result IndexResult
	if obj == nil {
		return nil, nil
	}

	err := json.Unmarshal(obj, &result)
	if err != nil {
		return nil, err
	}
	return &result, err
}

// Index adds or updates a typed JSON document in a specified index, making it
// searchable. In case id is empty, a new id is created over a HTTP POST request.
// Otherwise, a HTTP PUT request is issued.
// Implements: http://www.elastic.co/guide/en/elasticsearch/reference/current/docs-index_.html
func (es *Connection) Index(
	index, docType, id string,
	params map[string]string,
	body interface{},
) (int, *QueryResult, error) {
	method := "PUT"
	if id == "" {
		method = "POST"
	}
	return withQueryResult(es.apiCall(method, index, docType, id, "", params, body))
}

// Ingest pushes a pipeline of updates.
func (es *Connection) Ingest(
	index, docType, pipeline, id string,
	params map[string]string,
	body interface{},
) (int, *QueryResult, error) {
	method := "PUT"
	if id == "" {
		method = "POST"
	}
	return withQueryResult(es.apiCall(method, index, docType, id, pipeline, params, body))
}

// Refresh an index. Call this after doing inserts or creating/deleting
// indexes in unit tests.
func (es *Connection) Refresh(index string) (int, *QueryResult, error) {
	return withQueryResult(es.apiCall("POST", index, "", "_refresh", "", nil, nil))
}

// CreateIndex creates a new index, optionally with settings and mappings passed in
// the body.
// Implements: https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-create-index.html
//
func (es *Connection) CreateIndex(index string, body interface{}) (int, *QueryResult, error) {
	return withQueryResult(es.apiCall("PUT", index, "", "", "", nil, body))
}

// IndexExists checks if an index exists.
// Implements: https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-exists.html
func (es *Connection) IndexExists(index string) (int, error) {
	status, _, err := es.apiCall("HEAD", index, "", "", "", nil, nil)
	return status, err
}

// DeleteIndex deletes an existing index from Elasticsearch.
// Implements: https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-delete-index.html#indices-delete-index
func (es *Connection) DeleteIndex(index string) (int, error) {
	status, _, err := es.apiCall("DELETE", index, "", "", "", nil, nil)
	return status, err
}

// Delete deletes a typed JSON document from a specific index based on its id.
// Implements: http://www.elastic.co/guide/en/elasticsearch/reference/current/docs-delete.html
func (es *Connection) Delete(index string, docType string, id string, params map[string]string) (int, *QueryResult, error) {
	return withQueryResult(es.apiCall("DELETE", index, docType, id, "", params, nil))
}

// Reindex copies documents from one index to another.
// Implements: https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-reindex.html#docs-reindex
func (es *Connection) Reindex(oldIndex, newIndex string, options map[string]interface{}) (int, *IndexResult, error) {
	if options == nil {
		options = map[string]interface{}{
			"source": map[string]interface{}{
				"index": oldIndex,
			},
			"dest": map[string]interface{}{
				"index": newIndex,
			},
		}
	}
	params := map[string]string{
		"wait_for_completion": "true",
	}
	return withIndexResult(es.apiCall("POST", "", "", "_reindex", "", params, options))
}

// Alias adds an alias to an existing index.
// Implements: https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-aliases.html#alias-adding
func (es *Connection) Alias(index, alias string) (int, *QueryResult, error) {
	return withQueryResult(es.apiCall("POST", index, "", "_alias/"+alias, "", nil, nil))
}

// DeleteAlias deletes an existing alias from an index.
// Implements: https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-aliases.html#deleting
func (es *Connection) DeleteAlias(index, alias string) (int, *QueryResult, error) {
	return withQueryResult(es.apiCall("DELETE", index, "", "_alias/"+alias, "", nil, nil))
}

// Checks if an alias exists.
// Implements: https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-aliases.html#deleting
func (es *Connection) AliasExists(alias string) (int, error) {
	status, _, err := es.apiCall("HEAD", "/_alias/", alias, "", "", nil, nil)
	return status, err
}

// PipelineExists checks if a pipeline with name id already exists.
// Using: https://www.elastic.co/guide/en/elasticsearch/reference/current/get-pipeline-api.html
func (es *Connection) PipelineExists(id string) (bool, error) {
	status, _, err := es.apiCall("GET", "_ingest", "pipeline", id, "", nil, nil)
	if status == 404 {
		return false, nil
	}
	return status == 200, err
}

// CreatePipeline create a new ingest pipeline with name id.
// Implements: https://www.elastic.co/guide/en/elasticsearch/reference/current/put-pipeline-api.html
func (es *Connection) CreatePipeline(
	id string,
	params map[string]string,
	body interface{},
) (int, *QueryResult, error) {
	return withQueryResult(es.apiCall("PUT", "_ingest", "pipeline", id, "", params, body))
}

// DeletePipeline deletes an ingest pipeline by id.
// Implements: https://www.elastic.co/guide/en/elasticsearch/reference/current/delete-pipeline-api.html
func (es *Connection) DeletePipeline(
	id string,
	params map[string]string,
) (int, *QueryResult, error) {
	return withQueryResult(es.apiCall("DELETE", "_ingest", "pipeline", id, "", params, nil))
}

// SearchURI executes a search request using a URI by providing request parameters.
// Implements: http://www.elastic.co/guide/en/elasticsearch/reference/current/search-uri-request.html
func (es *Connection) SearchURI(index string, docType string, params map[string]string) (int, *SearchResults, error) {
	return es.SearchURIWithBody(index, docType, params, nil)
}

// SearchURIWithBody executes a search request using a URI by providing request
// parameters and a request body.
func (es *Connection) SearchURIWithBody(
	index string,
	docType string,
	params map[string]string,
	body interface{},
) (int, *SearchResults, error) {
	status, resp, err := es.apiCall("GET", index, docType, "_search", "", params, body)
	if err != nil {
		return status, nil, err
	}
	result, err := readSearchResult(resp)
	return status, result, err
}

// CountSearchURI counts the results for a search request.
func (es *Connection) CountSearchURI(
	index string, docType string,
	params map[string]string,
) (int, *CountResults, error) {
	status, resp, err := es.apiCall("GET", index, docType, "_count", "", params, nil)
	if err != nil {
		return status, nil, err
	}
	result, err := readCountResult(resp)
	return status, result, err
}

func (es *Connection) apiCall(
	method, index, docType, id, pipeline string,
	params map[string]string,
	body interface{},
) (int, []byte, error) {
	path, err := makePath(index, docType, id)
	if err != nil {
		return 0, nil, err
	}
	return es.Request(method, path, pipeline, params, body)
}
