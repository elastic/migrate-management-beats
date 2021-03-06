package main

import "encoding/json"

var newMapping = mustJSON(`
{
    "dynamic": "strict",
    "properties": {
        "beat": {
            "properties": {
                "access_token": {
                    "type": "keyword"
                },
                "active": {
                    "type": "boolean"
                },
                "config_status": {
                    "type": "keyword"
                },
                "enrollment_token": {
                    "type": "keyword"
                },
                "ephemeral_id": {
                    "type": "keyword"
                },
                "host_ip": {
                    "type": "ip"
                },
                "host_name": {
                    "type": "keyword"
                },
                "id": {
                    "type": "keyword"
                },
                "last_checkin": {
                    "type": "date"
                },
                "metadata": {
                    "dynamic": "true",
                    "type": "object"
                },
                "name": {
                    "type": "keyword"
                },
                "status" : {
                    "properties" : {
                        "event" : {
                            "properties" : {
                                "message" : {
                                    "type" : "text"
                                },
                                "type" : {
                                    "type" : "keyword"
                                },
                                "uuid" : {
                                    "type" : "keyword"
                                }
                            }
                        },
                        "timestamp" : {
                            "type" : "date"
                        },
                        "type" : {
                            "type" : "keyword"
                        }
                    }
                },
                "tags": {
                    "type": "keyword"
                },
                "type": {
                    "type": "keyword"
                },
                "verified_on": {
                    "type": "date"
                },
                "version": {
                    "type": "keyword"
                }
            }
        },
        "configuration_block": {
            "properties": {
                "config": {
                    "type": "keyword"
                },
                "description": {
                    "type": "text"
                },
                "id": {
                    "type": "keyword"
                },
                "last_updated": {
                    "type": "date"
                },
                "tag": {
                    "type": "keyword"
                },
                "type": {
                    "type": "keyword"
                }
            }
        },
        "enrollment_token": {
            "properties": {
                "expires_on": {
                    "type": "date"
                },
                "token": {
                    "type": "keyword"
                }
            }
        },
        "tag": {
            "properties": {
                "color": {
                    "type": "keyword"
                },
                "hasConfigurationBlocksTypes": {
                    "type": "keyword"
                },
                "id": {
                    "type": "keyword"
                },
                "name": {
                    "type": "keyword"
                }
            }
        },
        "type": {
            "type": "keyword"
        }
    }
}
`)

func mustJSON(mapping string) map[string]interface{} {
	var newMapping map[string]interface{}
	err := json.Unmarshal([]byte(mapping), &newMapping)
	if err != nil {
		panic("error while marshaling new index mapping")
	}
	return newMapping
}
