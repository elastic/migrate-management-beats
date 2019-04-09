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

package main

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/elastic/migrate-management-beats/libbeat/common"
)

func TestTransformTagsIntoConfigBlocks(t *testing.T) {
	tests := map[string]struct {
		tag                  map[string]interface{}
		expectedConfigBlocks []common.MapStr
		expectedError        string
	}{
		"correct, transformable tag including one configuration snippet": {
			tag: map[string]interface{}{
				"_index": ".management-beats-backup",
				"_type":  " _doc",
				"_id":    "tag:test",
				"_score": 0.2876821,
				"_source": map[string]interface{}{
					"tag": map[string]interface{}{
						"color":        "#DD0A73",
						"id":           "test",
						"last_updated": "2019-01-22T14:38:15.461Z",
						"configuration_blocks": []map[string]interface{}{
							map[string]interface{}{
								"type":        "output",
								"description": "the best output",
								"configs": []map[string]interface{}{
									map[string]interface{}{
										"elasticsearch": map[string]interface{}{
											"hosts":    []string{"http://localhost:9200"},
											"username": "elastic",
											"password": "changeme",
										},
										"output": "elasticsearch",
									},
								},
							},
						},
					},
					"type": "tag",
				},
			},
			expectedConfigBlocks: []common.MapStr{
				common.MapStr{
					"configuration_block": common.MapStr{
						"config":       "{\"elasticsearch\":{\"hosts\":[\"http://localhost:9200\"],\"password\":\"changeme\",\"username\":\"elastic\"},\"output\":\"elasticsearch\"}",
						"description":  "the best output",
						"last_updated": "2019-01-22T14:38:15.461Z",
						"tag":          "test",
						"type":         "output",
					},
					"type": "configuration_block",
				},
			},
			expectedError: "",
		},
		"correct, transformable tag including two configurations: one nested": {
			tag: map[string]interface{}{
				"_index": ".management-beats-backup",
				"_type":  " _doc",
				"_id":    "tag:test",
				"_score": 0.2876821,
				"_source": map[string]interface{}{
					"tag": map[string]interface{}{
						"color":        "#DD0A73",
						"id":           "test",
						"last_updated": "2019-01-22T14:38:15.461Z",
						"configuration_blocks": []map[string]interface{}{
							map[string]interface{}{
								"type":        "filebeat.inputs",
								"description": "my input",
								"configs": []map[string]interface{}{
									map[string]interface{}{
										"json.keys_under_root": "true",
										"paths":                []string{"t.log"}},
								},
							},
							map[string]interface{}{
								"type":        "output",
								"description": "the best output",
								"configs": []map[string]interface{}{
									map[string]interface{}{
										"elasticsearch": map[string]interface{}{
											"hosts":    []string{"http://localhost:9200"},
											"username": "elastic",
											"password": "changeme",
										},
										"output": "elasticsearch",
									},
								},
							},
						},
					},
					"type": "tag",
				},
			},
			expectedConfigBlocks: []common.MapStr{
				common.MapStr{
					"configuration_block": common.MapStr{
						"config":       "{\"json.keys_under_root\":\"true\",\"paths\":[\"t.log\"]}",
						"description":  "my input",
						"last_updated": "2019-01-22T14:38:15.461Z",
						"tag":          "test",
						"type":         "filebeat.inputs",
					},
					"type": "configuration_block",
				},
				common.MapStr{
					"configuration_block": common.MapStr{
						"config":       "{\"elasticsearch\":{\"hosts\":[\"http://localhost:9200\"],\"password\":\"changeme\",\"username\":\"elastic\"},\"output\":\"elasticsearch\"}",
						"description":  "the best output",
						"last_updated": "2019-01-22T14:38:15.461Z",
						"tag":          "test",
						"type":         "output",
					},
					"type": "configuration_block",
				},
			},
			expectedError: "",
		},
		"tag id is missing from the document": {
			tag: map[string]interface{}{
				"_index": ".management-beats-backup",
				"_type":  " _doc",
				"_id":    "tag:test",
				"_score": 0.2876821,
				"_source": map[string]interface{}{
					"tag": map[string]interface{}{
						"color":                "#DD0A73",
						"last_updated":         "2019-01-22T14:38:15.461Z",
						"configuration_blocks": []map[string]interface{}{},
					},
					"type": "tag",
				},
			},
			expectedConfigBlocks: []common.MapStr{},
			expectedError:        "error while extracting tag info: 1 error: key `_source.tag.id` not found",
		},
		"last_updated is missing from the document": {
			tag: map[string]interface{}{
				"_index": ".management-beats-backup",
				"_type":  " _doc",
				"_id":    "tag:test",
				"_score": 0.2876821,
				"_source": map[string]interface{}{
					"tag": map[string]interface{}{
						"color":                "#DD0A73",
						"id":                   "test",
						"configuration_blocks": []map[string]interface{}{},
					},
					"type": "tag",
				},
			},
			expectedConfigBlocks: []common.MapStr{},
			expectedError:        "error while extracting tag info: 1 error: key `_source.tag.last_updated` not found",
		},
		"configuration_blocks is missing from the document": {
			tag: map[string]interface{}{
				"_index": ".management-beats-backup",
				"_type":  " _doc",
				"_id":    "tag:test",
				"_score": 0.2876821,
				"_source": map[string]interface{}{
					"tag": map[string]interface{}{
						"color":                "#DD0A73",
						"id":                   "test",
						"last_updated":         "2019-01-22T14:38:15.461Z",
						"configuration_blocks": []map[string]interface{}{},
					},
					"type": "tag",
				},
			},
			expectedConfigBlocks: []common.MapStr{},
			expectedError:        "",
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			configurationBlocks, err := transformTags(test.tag)

			if test.expectedError != "" {
				assert.EqualError(t, err, test.expectedError)
			} else {
				assert.Nil(t, err)
			}
			assert.Equal(t, len(test.expectedConfigBlocks), len(configurationBlocks))

			for _, configBlock := range configurationBlocks {
				configBlock.Delete("configuration_block.id")
				assert.Contains(t, test.expectedConfigBlocks, configBlock)
			}
		})
	}
}
