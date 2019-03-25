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
	"flag"
	"io/ioutil"
	"log"

	yaml "gopkg.in/yaml.v2"
)

var (
	startingStep uint
)

func init() {
	flag.UintVar(&startingStep, "step", 1, "Number of step to start from after a failure")
}

func main() {
	flag.Parse()

	cfgFile, err := ioutil.ReadFile("migrate.yml")
	if err != nil {
		log.Fatalf("Error reading configuration: %+v", err)
	}
	conf := defaultConfig
	err = yaml.Unmarshal(cfgFile, &conf)
	if err != nil {
		log.Fatalf("Error while reading configuration: %+v", err)
	}

	err = migrate(conf, startingStep)
	if err != nil {
		log.Fatalf("Error while migrating: %+v", err)
	}
}
