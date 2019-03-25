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
	"fmt"
	"log"
	"time"

	//"gopkg.in/yaml.v2"

	"github.com/elastic/migrate-management-beats/libbeat/common/schema"
	"github.com/elastic/migrate-management-beats/libbeat/common/transport/tlscommon"
	"github.com/elastic/migrate-management-beats/libbeat/outputs/elasticsearch"
)

type config struct {
	URL      string            `yaml:url`
	Username string            `yaml:username`
	Password string            `yaml:password`
	SSL      *tlscommon.Config `yaml:ssl`
}

const (
	managementIndexName       = ".management-beats"
	backupManagementIndexName = ".management-beats-backup"
	newManagementIndexName    = ".management-beats-new"
)

var (
	newSchema = schema.Schema{
		"configuration_block": schema.Object{
			"tag":          schema.Conv{Key: "_source.tag.id", Func: schema.NestedKeys, Required: true},
			"last_updated": schema.Conv{Key: "_source.tag.last_updated", Func: schema.NestedKeys, Required: true},
		},
		"tag": schema.Object{
			"id":    schema.Conv{Key: "_source.tag.id", Func: schema.NestedKeys, Required: true},
			"name":  schema.Conv{Key: "_source.tag.name", Func: schema.NestedKeys, Optional: true},
			"color": schema.Conv{Key: "_source.tag.color", Func: schema.NestedKeys, Optional: true},
		},
	}

	defaultConfig = config{
		URL:      "http://localhost:9200",
		Username: "elastic",
		Password: "changeme",
		SSL:      nil,
	}

	migrationSteps = []step{
		backupStep{
			oldMapping:  make(map[string]interface{}, 0),
			oldSettings: make(map[string]interface{}, 0),
		},
		createNewIndexStep{},
		migrateFromOldIndexStep{},
		finalStep{},
	}
)

// migrate migrates beats central management from 6.6 to 6.7
// updates the mapping and the data
func migrate(c config, step uint) error {
	log.Println("Starting migration to Beats CM 6.7")
	client, err := connectToES(c)
	if err != nil {
		return fmt.Errorf("error while connecting to Elasticsearch: %+v", err)
	}

	for i := step - 1; i < uint(len(migrationSteps)); i++ {
		err = migrationSteps[i].Do(client)
		if err != nil {
			undoErr := migrationSteps[i].Undo(client)
			if undoErr != nil {
				return fmt.Errorf("error while rolling back from error %+v: %+v", err, undoErr)
			}
			return fmt.Errorf("rolled back migration due to %+v, can be continued from step %d", err, i+1)
		}
	}

	return nil
}

func connectToES(c config) (*elasticsearch.Client, error) {
	tlsConfig, err := tlscommon.LoadTLSConfig(c.SSL)
	if err != nil {
		return nil, err
	}

	client, err := elasticsearch.NewClient(elasticsearch.ClientSettings{
		URL:              c.URL,
		Username:         c.Username,
		Password:         c.Password,
		Timeout:          60 * time.Second,
		CompressionLevel: 3,
		TLS:              tlsConfig,
	})
	if err != nil {
		return nil, fmt.Errorf("error while creating Elasticsearch client: %+v", err)
	}
	err = client.Connect()
	if err != nil {
		return nil, err
	}
	log.Println("Connected to Elasticsearch")

	return client, nil
}
