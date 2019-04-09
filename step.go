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
	"log"

	"github.com/gofrs/uuid"

	"github.com/elastic/migrate-management-beats/libbeat/common"
	"github.com/elastic/migrate-management-beats/libbeat/outputs/elasticsearch"
)

const (
	maxTagPageSize = 1000
)

type step interface {
	Do(*elasticsearch.Client) error
	Undo(*elasticsearch.Client) error
}

type backupStep struct {
	oldMapping        map[string]interface{}
	oldSettings       map[string]interface{}
	aliasCreated      bool
	backupIndexExists bool
	oldIndexDeleted   bool
}

type createNewIndexStep struct {
	created bool
}

type migrateFromOldIndexStep struct{}

type finalStep struct {
	aliasDeleted             bool
	mangementIndexCreated    bool
	newTemporaryIndexDeleted bool
	backupIndexDeleted       bool
}

func (b backupStep) Do(client *elasticsearch.Client) error {
	log.Printf("STEP #1: Backup existing %s index\n", managementIndexName)
	_, _, err := client.Reindex(managementIndexName, backupManagementIndexName, nil)
	if err != nil {
		return fmt.Errorf("error while backup up old .management-beats index: %+v", err)
	}
	b.backupIndexExists = true
	log.Printf("Old %s index is reindexed into %s\n", managementIndexName, backupManagementIndexName)

	_, err = client.DeleteIndex(managementIndexName)
	if err != nil {
		return fmt.Errorf("error while deleting old .management-beats index: %+v", err)
	}
	b.oldIndexDeleted = true
	log.Printf("Index %s is deleted\n", managementIndexName)

	_, _, err = client.Alias(backupManagementIndexName, managementIndexName)
	if err != nil {
		return fmt.Errorf("error while creating alias for the backup: %+v", err)
	}
	b.aliasCreated = true
	log.Printf("Alias %s is created for the backup index\n", managementIndexName)
	return nil
}

func (b backupStep) Undo(client *elasticsearch.Client) error {
	var err error
	if b.backupIndexExists {
		if b.aliasCreated {
			_, _, err = client.DeleteAlias(backupManagementIndexName, managementIndexName)
			if err != nil {
				return err
			}
		}
		if b.oldIndexDeleted {
			err = createNewIndexWithMapping(client, newManagementIndexName, b.oldMapping, b.oldSettings)
			if err != nil {
				return err
			}
			_, _, err := client.Reindex(backupManagementIndexName, newManagementIndexName, nil)
			if err != nil {
				return err
			}
		}
		_, err := client.DeleteIndex(backupManagementIndexName)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c createNewIndexStep) Do(client *elasticsearch.Client) error {
	log.Println("STEP #2: Create new temporary index")

	err := createNewIndexWithMapping(client, newManagementIndexName, newMapping, nil)
	if err != nil {
		return err
	}
	c.created = true
	return nil
}

func createNewIndexWithMapping(client *elasticsearch.Client, index string, mapping, settings map[string]interface{}) error {
	body := map[string]interface{}{
		"settings": settings,
		"mappings": map[string]interface{}{
			"_doc": mapping,
		},
	}
	_, _, err := client.CreateIndex(index, body)
	if err != nil {
		return fmt.Errorf("error while creating new index: %+v", err)
	}
	log.Printf("New index %s is created\n", index)
	return nil
}

func (c createNewIndexStep) Undo(client *elasticsearch.Client) error {
	if c.created {
		_, err := client.DeleteIndex(newManagementIndexName)
		if err != nil {
			return fmt.Errorf("error while deleting temporary .management-beats-new index: %+v", err)
		}

		log.Printf("Index %s has been removed\n", managementIndexName)
	}
	return nil
}

func (m migrateFromOldIndexStep) Do(client *elasticsearch.Client) error {
	log.Println("STEP #3: Migrate from old index")
	// refresh data before searching
	_, _, err := client.Refresh(backupManagementIndexName)
	if err != nil {
		return fmt.Errorf("error while refreshing index before performing migration: %+v", err)
	}

	err = migrateEnrollmentTokens(client)
	if err != nil {
		return err
	}

	err = migrateBeats(client)
	if err != nil {
		return err
	}

	err = migrateTags(client)
	if err != nil {
		return err
	}

	return nil
}

func migrateEnrollmentTokens(client *elasticsearch.Client) error {
	body := map[string]interface{}{
		"source": map[string]interface{}{
			"index": backupManagementIndexName,
			"query": map[string]interface{}{
				"term": map[string]interface{}{
					"type": "enrollment_token",
				},
			},
		},
		"dest": map[string]interface{}{
			"index": newManagementIndexName,
		},
	}
	_, resp, err := client.Reindex(backupManagementIndexName, newManagementIndexName, body)
	if err != nil {
		return fmt.Errorf("error while copying enrollment_token documents: %+v", err)
	}
	log.Printf("enrollment_token documents migrated to new index: %d\n", resp.Created)
	return nil
}

func migrateBeats(client *elasticsearch.Client) error {
	body := map[string]interface{}{
		"source": map[string]interface{}{
			"index": backupManagementIndexName,
			"query": map[string]interface{}{
				"term": map[string]interface{}{
					"type": "beat",
				},
			},
		},
		"dest": map[string]interface{}{
			"index": newManagementIndexName,
		},
		"script": map[string]interface{}{
			"lang":   "painless",
			"source": `ctx._id = "beat:" + ctx._source.beat.id;`,
		},
	}
	_, resp, err := client.Reindex(backupManagementIndexName, newManagementIndexName, body)
	if err != nil {
		return fmt.Errorf("error while copying beat documents: %+v", err)
	}
	log.Printf("beat documents migrated to new index: %d\n", resp.Created)
	return nil
}

func migrateTags(client *elasticsearch.Client) error {
	body := map[string]interface{}{
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
			"source": `List hasConfigurationBlocksTypes = [];
for (configuration_block in ctx._source.tag.configuration_blocks) {
    if (configuration_block.type == 'output') {
       hasConfigurationBlocksTypes = ['output'];
    }
}
ctx._source.tag.put("hasConfigurationBlocksTypes", hasConfigurationBlocksTypes);
ctx._source.tag.put("name", ctx._source.tag.id);
ctx._id = "tag:" + ctx._source.tag.id;
ctx._source.tag.remove('configuration_blocks');
ctx._source.tag.remove('last_updated');
`,
		},
	}
	_, resp, err := client.Reindex(backupManagementIndexName, newManagementIndexName, body)
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

		var docInBulk []interface{}
		for uid, configurationBlock := range configurationBlocks {
			indexReq := map[string]interface{}{
				"index": map[string]interface{}{
					"_index": newManagementIndexName,
					"_type":  "_doc",
					"_id":    fmt.Sprintf("configuration_block:%s", uid),
				},
			}
			docInBulk = append(docInBulk, indexReq)
			docInBulk = append(docInBulk, configurationBlock)
		}
		fmt.Printf("%+v\n", docInBulk)
		r, err := client.Bulk(newManagementIndexName, "_doc", nil, docInBulk)
		fmt.Println(r)
		if err != nil {
			return fmt.Errorf("error while performing bulk request: %+v", err)
		}

	}
	log.Println("configuration_blocks are migrated to new index")
	return nil
}

func getAllTagsToTransform(client *elasticsearch.Client) ([]map[string]interface{}, error) {
	from := 0
	results, err := queryTagsToTransform(client, from)
	if err != nil {
		return nil, fmt.Errorf("error while getting tags to transform: %+v", err)
	}

	tags := make([]map[string]interface{}, 0)
	for _, rawTag := range results.Hits.Hits {
		var tag common.MapStr
		err = json.Unmarshal(rawTag, &tag)
		if err != nil {
			return nil, fmt.Errorf("error unmarshaling data: %+v", err)
		}
		tags = append(tags, tag)
	}

	size := len(results.Hits.Hits)
	from += size
	for size == maxTagPageSize {
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

		size = len(results.Hits.Hits)
		from += size
	}

	return tags, nil
}

func queryTagsToTransform(client *elasticsearch.Client, from int) (*elasticsearch.SearchResults, error) {
	params := map[string]string{
		"q": "type:tag",
	}
	body := map[string]interface{}{
		"from": from,
		"size": maxTagPageSize,
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

func transformTags(doc map[string]interface{}) (map[string]common.MapStr, error) {
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

	configurationBlocks := make(map[string]common.MapStr, 0)
	configurationBlocksOfTag := make([]map[string]interface{}, 0)
	switch v := cc.(type) {
	case map[string]interface{}:
		cfgBlock := cc.(map[string]interface{})
		configurationBlocksOfTag = append(configurationBlocksOfTag, cfgBlock)
	case []map[string]interface{}:
		configurationBlocksOfTag = cc.([]map[string]interface{})
	case []interface{}:
		for _, b := range cc.([]interface{}) {
			configurationBlocksOfTag = append(configurationBlocksOfTag, b.(map[string]interface{}))
		}
	default:
		return nil, fmt.Errorf("unexpected tag format %v: %+v", v, cc)
	}

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
			uid, _ := uuid.NewV4()
			newCfgBlock.Put("configuration_block.id", uid.String())
			newCfgBlock.Put("type", "configuration_block")
			configurationBlocks[uid.String()] = newCfgBlock
		}
	}

	return configurationBlocks, nil
}

func (m migrateFromOldIndexStep) Undo(client *elasticsearch.Client) error {
	_, err := client.DeleteIndex(newManagementIndexName)
	if err != nil {
		return err
	}
	return createNewIndexWithMapping(client, newManagementIndexName, newMapping, nil)
}

func (f finalStep) Do(client *elasticsearch.Client) error {
	log.Println("STEP #4: Finilazing migration")
	_, _, err := client.DeleteAlias(backupManagementIndexName, managementIndexName)
	if err != nil {
		return fmt.Errorf("error while removing .management-beats alias: %+v", err)
	}
	f.aliasDeleted = true
	log.Printf("Alias %s is removed\n", managementIndexName)

	_, _, err = client.Refresh(newManagementIndexName)
	if err != nil {
		return fmt.Errorf("error while refreshing index after finishing migration: %+v", err)
	}
	err = createNewIndexWithMapping(client, managementIndexName, newMapping, nil)
	if err != nil {
		return fmt.Errorf("error while creating migrated .beats-management: %+v", err)
	}
	_, _, err = client.Reindex(newManagementIndexName, managementIndexName, nil)
	if err != nil {
		return fmt.Errorf("error while moving documents to .management-beats: %+v", err)
	}
	f.mangementIndexCreated = true
	log.Printf("New index is reindexed into %s\n", managementIndexName)

	_, err = client.DeleteIndex(newManagementIndexName)
	if err != nil {
		return fmt.Errorf("error while deleting index '%s': %+v", newManagementIndexName, err)
	}
	f.newTemporaryIndexDeleted = true

	_, err = client.DeleteIndex(backupManagementIndexName)
	if err != nil {
		return fmt.Errorf("error while deleting index '%s': %+v", backupManagementIndexName, err)
	}
	f.backupIndexDeleted = true

	log.Println("Intermediate indices are deleted")
	return nil
}

func (f finalStep) Undo(client *elasticsearch.Client) error {
	if f.aliasDeleted {
		if f.mangementIndexCreated {
			if !f.newTemporaryIndexDeleted {
				log.Printf("The data has been migrated, but the intermediate indices are not deleted: %s, %s. Try to delete them manually.\n",
					backupManagementIndexName, newManagementIndexName)
				return nil
			}
			if !f.backupIndexDeleted {
				log.Printf("The data has been migrated, but the backup index cannot be deleted %s. Try to delete it manually.",
					backupManagementIndexName)
				return nil
			}
		}
		_, _, err := client.Alias(backupManagementIndexName, managementIndexName)
		return err
	}

	return nil
}
