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
	"fmt"
	"io/ioutil"
	"log"
	"time"

	"github.com/elastic/migrate-management-beats/libbeat/common"
	"github.com/elastic/migrate-management-beats/libbeat/common/schema"
	"github.com/elastic/migrate-management-beats/libbeat/outputs/elasticsearch"
)

type config struct {
	esURL    string
	username string
	password string
}

const (
	managementIndexName       = ".management-beats"
	backupManagementIndexName = ".management-beats-backup"
	newManagementIndexName    = ".management-beats-new"
	indexMappingFilePath      = "new-index-mapping.json"
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
)

// migrate migrates beats central management from 6.6 to 6.7
// updates the mapping and the data
func migrate(c config) error {
	log.Println("Starting migration to Beats CM 6.7")
	client, err := connectToES(c)
	if err != nil {
		return fmt.Errorf("error while connecting to Elasticsearch: %+v", err)
	}

	migrationSteps := []func(*elasticsearch.Client) error{
		backupOldIndex,
		createNewIndexWithMapping,
		migrateDataFromOldIndex,
		finalize,
	}

	for _, step := range migrationSteps {
		err = step(client)
		if err != nil {
			rollback(client)
			return fmt.Errorf("error while migrating index: %+v", err)
		}
	}

	return nil
}

func connectToES(c config) (*elasticsearch.Client, error) {
	client, err := elasticsearch.NewClient(elasticsearch.ClientSettings{
		URL:              c.esURL,
		Username:         c.username,
		Password:         c.password,
		Timeout:          60 * time.Second,
		CompressionLevel: 3,
	})
	if err != nil {
		return nil, fmt.Errorf("error while creating Elasticsearch client: %+v", err)
	}
	err = client.Connect()
	if err != nil {
		return nil, fmt.Errorf("error while connecting to Elasticsearch instance: %+v", err)
	}
	log.Println("Connected to Elasticsearch")

	//esVersion := client.GetVersion()
	//isRequiredVersion := esVersion.EqualMajorAndMinor(m.to)
	//if !isRequiredVersion {
	//	return nil, fmt.Errorf("cannot migrate because of Elasticsearch version. expected 6.7, actual %s", esVersion.String())
	//}
	return client, nil
}

func backupOldIndex(client *elasticsearch.Client) error {
	_, _, err := client.Reindex(managementIndexName, backupManagementIndexName, nil)
	if err != nil {
		return fmt.Errorf("error while backup up old .management-beats index: %+v", err)
	}
	log.Println("Old .management-beats index is reindexed into .management-beats-backup")

	_, err = client.DeleteIndex(managementIndexName)
	if err != nil {
		return fmt.Errorf("error while deleting old .management-beats index: %+v", err)
	}
	log.Println("Index .management-index is deleted")

	_, _, err = client.Alias(backupManagementIndexName, managementIndexName)
	if err != nil {
		return fmt.Errorf("error while creating alias for the backup: %+v", err)
	}
	log.Println("Alias .management-beats is created for the backup index")
	return nil
}

func createNewIndexWithMapping(client *elasticsearch.Client) error {
	newMapping, err := ioutil.ReadFile(indexMappingFilePath)
	if err != nil {
		return fmt.Errorf("error while reading new index mapping from file: %+v", err)
	}
	var mapping map[string]interface{}
	err = json.Unmarshal(newMapping, &mapping)
	if err != nil {
		return fmt.Errorf("error while marshaling new index mapping: %+v", err)
	}
	body := map[string]interface{}{
		"mappings": map[string]interface{}{
			"_doc": mapping,
		},
	}
	_, _, err = client.CreateIndex(newManagementIndexName, body)
	if err != nil {
		return fmt.Errorf("error while creating new index: %+v", err)
	}
	log.Println("New index .management-beats-new is created")
	return nil
}

func migrateDataFromOldIndex(client *elasticsearch.Client) error {
	// refresh data before searching
	_, _, err := client.Refresh(backupManagementIndexName)
	if err != nil {
		return fmt.Errorf("error while refreshing index before performing migration: %+v", err)
	}

	// reindex data which stays the same
	body := map[string]interface{}{
		"source": map[string]interface{}{
			"index": backupManagementIndexName,
			"query": map[string]interface{}{
				"terms": map[string]interface{}{
					"type": []string{"beat", "enrollment_token"},
				},
			},
		},
		"dest": map[string]interface{}{
			"index": newManagementIndexName,
		},
	}
	_, resp, err := client.Reindex(managementIndexName, backupManagementIndexName, body)
	if err != nil {
		return fmt.Errorf("error while copying beat and enrollment_token documents: %+v", err)
	}
	log.Printf("beat and enrollment_token documents migrated to new index: %d", resp.Created)

	// migrate tags
	body = map[string]interface{}{
		"source": map[string]interface{}{
			"index": backupManagementIndexName,
			"query": map[string]interface{}{
				"term": map[string]interface{}{
					"type": "tag",
				},
			},
		},
		"dest": map[string]interface{}{
			"index": newManagementIndexName,
		},
		"script": map[string]interface{}{
			"lang": "painless",
			"source": `List hasConfigurationBlockTypes = [];
for (configuration_block in ctx._source.tag.configuration_blocks) {
    if (configuration_block.type == 'output') {
       hasConfigurationBlockTypes = ['output'];
    }
}
ctx._source.tag.put("hasConfigurationBlockTypes", hasConfigurationBlockTypes);
ctx._source.tag.remove('configuration_blocks');
ctx._source.tag.remove('last_updated');
`,
		},
	}
	_, resp, err = client.Reindex(managementIndexName, backupManagementIndexName, body)
	if err != nil {
		return fmt.Errorf("error while migrating tags: %+v", err)
	}
	log.Printf("Tags migrated to new index: %d\n", resp.Created)

	// migrate configuration blocks from tags
	tags, err := getAllTagsToTransform(client)
	if err != nil {
		return fmt.Errorf("error querying tags from old index: %+v", err)
	}

	for _, tag := range tags {
		configurationBlocks, err := transformTags(tag)
		if err != nil {
			return fmt.Errorf("error while transforming tag into configuration_block: %+v", err)
		}

		for _, configurationBlock := range configurationBlocks {
			_, _, err = client.Index(newManagementIndexName, "_doc", "", nil, configurationBlock)
			if err != nil {
				return fmt.Errorf("error while indexing new configration_block: %+v", err)
			}
		}
	}
	log.Println("configuration_blocks are migrated to new index")
	return nil
}

func finalize(client *elasticsearch.Client) error {
	log.Println("Finilazing migration")
	_, _, err := client.DeleteAlias(backupManagementIndexName, managementIndexName)
	if err != nil {
		return fmt.Errorf("error while removing .management-beats alias: %+v", err)
	}
	log.Println("Alias .management-beats is removed")

	_, _, err = client.Reindex(newManagementIndexName, managementIndexName, nil)
	if err != nil {
		return fmt.Errorf("error while moving documents to .management-beats: %+v", err)
	}
	log.Println("New index is reindexed into .management-beats")

	indicesToDelete := []string{backupManagementIndexName, newManagementIndexName}
	for _, index := range indicesToDelete {
		_, err = client.DeleteIndex(index)
		if err != nil {
			return fmt.Errorf("error while deleting intermetiate index '%s': %+v", index, err)
		}
	}
	log.Println("Intermediate indices are deleted")
	return nil
}

func rollback(client *elasticsearch.Client) error {
	// delete alias if existing
	code, err := client.IndexExists(backupManagementIndexName)
	if err != nil {
		return fmt.Errorf("error while checking if backup index exists: %+v", err)
	}
	if code == 200 {
		aliases, err := client.Aliases(backupManagementIndexName)
		if err != nil {
			return fmt.Errorf("error while querying aliases: %+v", err)
		}
		if len(aliases) != 0 {
			_, _, err = client.DeleteAlias(backupManagementIndexName, managementIndexName)
			if err != nil {
				return fmt.Errorf("error while deleting alias for backup: %+v", err)
			}
		}
	} else {
		return nil
	}

	// reindex backup into .management-beats
	_, _, err = client.Reindex(backupManagementIndexName, managementIndexName, nil)
	if err != nil {
		return fmt.Errorf("error while rolling back from backup: %+v", err)
	}

	// remove intermediate indices
	_, err = client.DeleteIndex(backupManagementIndexName)
	if err != nil {
		return fmt.Errorf("error while deleting backup index: %+v", err)
	}
	code, err = client.IndexExists(newManagementIndexName)
	if err != nil {
		return fmt.Errorf("error while checking if new management index exists: %+v", err)
	}
	if code == 200 {
		_, err = client.DeleteIndex(newManagementIndexName)
		if err != nil {
			return fmt.Errorf("error while deleting intermediate index: %+v", err)
		}
	}
	return nil
}

func getAllTagsToTransform(client *elasticsearch.Client) ([]map[string]interface{}, error) {
	from := 0
	results, err := queryTagsToTransform(client, from)
	if err != nil {
		return nil, fmt.Errorf("error while getting tags to transform: %+v", err)
	}

	total := results.Hits.Total.Value
	tags := make([]map[string]interface{}, 0)

	for _, rawTag := range results.Hits.Hits {
		var tag common.MapStr
		err = json.Unmarshal(rawTag, &tag)
		if err != nil {
			return nil, fmt.Errorf("error unmarshaling data: %+v", err)
		}
		tags = append(tags, tag)
	}

	from += len(results.Hits.Hits)
	for from < total {
		results, err := queryTagsToTransform(client, from)
		if err != nil {
			return nil, err
		}

		for _, rawTag := range results.Hits.Hits {
			var tag map[string]interface{}
			err = json.Unmarshal(rawTag, &tag)
			if err != nil {
				return nil, fmt.Errorf("error unmarshaling data: %+v", err)
			}
			tags = append(tags, tag)
		}

		from += len(results.Hits.Hits)
	}

	return tags, nil
}

func queryTagsToTransform(client *elasticsearch.Client, from int) (*elasticsearch.SearchResults, error) {
	params := map[string]string{
		"q": "type:tag",
	}
	body := map[string]interface{}{
		"from": from,
	}

	c, results, err := client.SearchURIWithBody(managementIndexName, "", params, body)
	if err != nil {
		return nil, fmt.Errorf("error while querying tags from Elasticsearch: %+v", err)
	}
	if c != 200 {
		return nil, fmt.Errorf("status code is not 200: it is %d", c)
	}

	return results, nil
}

func transformTags(doc map[string]interface{}) ([]common.MapStr, error) {
	// get the common tag info for all new documents
	newDocBase, err := newSchema.Apply(doc)
	if err != nil {
		return nil, fmt.Errorf("error while extracting tag info: %+v", err)
	}

	// get configuration_configuration blocks to iterate over
	docMap := common.MapStr(doc)
	cc, err := docMap.GetValue("_source.tag.configuration_blocks")
	if err != nil {
		return nil, fmt.Errorf("error while getting tag.configuration_blocks from document: %+v", err)
	}

	configurationBlocks := make([]common.MapStr, 0)
	configurationBlocksOfTag := cc.([]map[string]interface{})
	for _, b := range configurationBlocksOfTag {
		cfgBlock := common.MapStr(b)
		iConfigs, err := cfgBlock.GetValue("configs")
		if err != nil {
			return nil, fmt.Errorf("error while getting configs from configuration_block: %+v", err)
		}
		cfgBlock.Delete("configs")

		configs := iConfigs.([]map[string]interface{})
		for _, cfg := range configs {
			config := common.MapStr(cfg).String()

			newCfgBlock := common.MapStr{}
			_, err := newCfgBlock.Put("configuration_block", cfgBlock)
			if err != nil {
				return nil, fmt.Errorf("error while nesting config info under configuration_block: %+v", err)
			}
			_, err = newCfgBlock.Put("configuration_block.config", config)
			if err != nil {
				return nil, fmt.Errorf("error while putting config string under configuration_block.config: %+v", err)
			}
			newCfgBlock.DeepUpdate(newDocBase)
			configurationBlocks = append(configurationBlocks, newCfgBlock)
		}
	}
	return configurationBlocks, nil
}

func isOutputConfig(config common.MapStr) bool {
	return false
}
