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
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/elastic/migrate-management-beats/libbeat/common"
)

func TestTransformTagsIntoConfigBlocks(t *testing.T) {
	tests := map[string]struct {
		tag                  []byte
		expectedConfigBlocks []common.MapStr
		expectedError        string
	}{
		"correct, transformable tag including one elasticsearch output configuration snippet": {
			tag: []byte(`
			{
				"_source":
				{
					"tag": {
						"color":        "#DD0A73",
						"id":           "test",
						"last_updated": "2019-01-22T14:38:15.461Z",
						"configuration_blocks": [
							{
								"type":        "output",
								"description": "the best output",
								"configs": [
									{
										"elasticsearch": {
											"hosts":    ["http://localhost:9200"],
											"username": "elastic",
											"password": "changeme"
										},
										"output": "elasticsearch"
									}
								]
							}
						]
					},
					"type": "tag"
				}
			}`),
			expectedConfigBlocks: []common.MapStr{
				common.MapStr{
					"configuration_block": common.MapStr{
						"config":       "{\"_sub_type\":\"elasticsearch\",\"hosts\":[\"http://localhost:9200\"],\"password\":\"changeme\",\"username\":\"elastic\"}",
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
		"correct, transformable tag including two configurations: one filebeat input, one elasticsearch output": {
			tag: []byte(`
			{
				"_source":
				{
					"tag": {
						"color":        "#DD0A73",
						"id":           "test",
						"last_updated": "2019-01-22T14:38:15.461Z",
						"configuration_blocks": [
							{
								"type":        "filebeat.inputs",
								"description": "my input",
								"configs": [
									{
										"json.keys_under_root": true,
										"paths":                ["t.log"]
									}
								]
							},
							{
								"type":        "output",
								"description": "the best output",
								"configs": [
									{
										"elasticsearch": {
											"hosts":    ["http://localhost:9200"],
											"username": "elastic",
											"password": "changeme"
										},
										"output": "elasticsearch"
									}
								]
							}
						]
					},
					"type": "tag"
				}
			}`),
			expectedConfigBlocks: []common.MapStr{
				common.MapStr{
					"configuration_block": common.MapStr{
						"config":       "{\"_sub_type\":\"elasticsearch\",\"hosts\":[\"http://localhost:9200\"],\"password\":\"changeme\",\"username\":\"elastic\"}",
						"description":  "the best output",
						"last_updated": "2019-01-22T14:38:15.461Z",
						"tag":          "test",
						"type":         "output",
					},
					"type": "configuration_block",
				},
				common.MapStr{
					"configuration_block": common.MapStr{
						"config":       "{\"json.keys_under_root\":true,\"paths\":[\"t.log\"]}",
						"description":  "my input",
						"last_updated": "2019-01-22T14:38:15.461Z",
						"tag":          "test",
						"type":         "filebeat.inputs",
					},
					"type": "configuration_block",
				},
			},
			expectedError: "",
		},
		"correct, transformable tag including two configurations: one metricbeat module, one elasticsearch output": {
			tag: []byte(`
			{
				"_source":
				{
					"tag": {
						"color":        "#DD0A73",
						"id":           "test",
						"last_updated": "2019-01-22T14:38:15.461Z",
						"configuration_blocks": [
							{
								"type":        "metricbeat.modules",
								"description": "beat module",
								"configs": [
									{
										"node.namespace": "node",
										"module":         "munin",
										"hosts":          ["localhost"],
										"period":         "30s"
									}
								]
							},
							{
								"type":        "output",
								"description": "simpleput",
								"configs": [
									{
										"elasticsearch": {
											"hosts":    ["http://localhost:9200"],
											"username": "elastic",
											"password": "changeme"
										},
										"output": "elasticsearch"
									}
								]
							}
						]
					},
					"type": "tag"
				}
			}`),
			expectedConfigBlocks: []common.MapStr{
				common.MapStr{
					"configuration_block": common.MapStr{
						"config":       "{\"_sub_type\":\"munin\",\"hosts\":[\"localhost\"],\"node.namespace\":\"node\",\"period\":\"30s\"}",
						"description":  "beat module",
						"last_updated": "2019-01-22T14:38:15.461Z",
						"tag":          "test",
						"type":         "metricbeat.modules",
					},
					"type": "configuration_block",
				},
				common.MapStr{
					"configuration_block": common.MapStr{
						"config":       "{\"_sub_type\":\"elasticsearch\",\"hosts\":[\"http://localhost:9200\"],\"password\":\"changeme\",\"username\":\"elastic\"}",
						"description":  "simpleput",
						"last_updated": "2019-01-22T14:38:15.461Z",
						"tag":          "test",
						"type":         "output",
					},
					"type": "configuration_block",
				},
			},
			expectedError: "",
		},
		"correct, transformable tag including two configurations: one filebeat module, one elasticsearch output": {
			tag: []byte(`
			{
				"_source":
				{
					"tag": {
						"color":        "#DD0A73",
						"id":           "test",
						"last_updated": "2019-01-22T14:38:15.461Z",
						"configuration_blocks": [
							{
								"type":        "filebeat.modules",
								"description": "beat module",
								"configs": [
									{
										"module": "system"
									}
								]
							},
							{
								"type":        "output",
								"description": "simpleput",
								"configs": [
									{
										"elasticsearch": {
											"hosts":    ["http://localhost:9200"],
											"username": "elastic",
											"password": "changeme"
										},
										"output": "elasticsearch"
									}
								]
							}
						]
					},
					"type": "tag"
				}
			}`),
			expectedConfigBlocks: []common.MapStr{
				common.MapStr{
					"configuration_block": common.MapStr{
						"config":       "{\"_sub_type\":\"system\"}",
						"description":  "beat module",
						"last_updated": "2019-01-22T14:38:15.461Z",
						"tag":          "test",
						"type":         "filebeat.modules",
					},
					"type": "configuration_block",
				},
				common.MapStr{
					"configuration_block": common.MapStr{
						"config":       "{\"_sub_type\":\"elasticsearch\",\"hosts\":[\"http://localhost:9200\"],\"password\":\"changeme\",\"username\":\"elastic\"}",
						"description":  "simpleput",
						"last_updated": "2019-01-22T14:38:15.461Z",
						"tag":          "test",
						"type":         "output",
					},
					"type": "configuration_block",
				},
			},
			expectedError: "",
		},
		"configuration_blocks is missing from the document": {
			tag: []byte(`
			{
				"_source":
				{
					"tag": {
						"color":                "#DD0A73",
						"id":                   "test",
						"last_updated":         "2019-01-22T14:38:15.461Z"
					},
					"type": "tag"
				}
			}`),
			expectedConfigBlocks: []common.MapStr{},
			expectedError:        "",
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {

			var tagDoc TagDocument
			err := json.Unmarshal(test.tag, &tagDoc)
			if err != nil {
				t.Fatalf("Error while serializing test data: %+v", err)
			}

			configurationBlocks, err := transformTag(tagDoc.Source.Tag)

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
