// Copyright 2019 Bull S.A.S. Atos Technologies - Bull, Rue Jean Jaures, B.P.68, 78340, Les Clayes-sous-Bois, France.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package elastic

import (
	"context"
	"github.com/hashicorp/consul/api"
	"github.com/pkg/errors"
	"github.com/ystia/yorc/v4/helper/consulutil"
	"github.com/ystia/yorc/v4/storage/encoding"
	"github.com/ystia/yorc/v4/storage/store"
	"github.com/ystia/yorc/v4/storage/utils"
	"time"
	elasticsearch6 "github.com/elastic/go-elasticsearch/v6"
	"github.com/ystia/yorc/v4/log"
	"regexp"
	"github.com/elastic/go-elasticsearch/v6/esapi"
	"encoding/json"
	"bytes"
	"github.com/ystia/yorc/v4/config"
	"strings"
)

var re = regexp.MustCompile(`(?m)\_yorc\/(\w+)\/.+\/(.*)`)
var indicePrefix = "yorcinus_"
var sequenceIndiceName = indicePrefix + "anothersequence"


type elasticStore struct {
	codec encoding.Codec
	esClient *elasticsearch6.Client
	clusterId string
}

func (c *elasticStore) extractIndexNameAndTimestamp(k string) (string, string) {
	var indexName, timestamp string
	res := re.FindAllStringSubmatch(k, -1)
	for i := range res {
		indexName = res[i][1]
		timestamp = res[i][2]
	}
	return indexName, timestamp
}

// NewStore returns a new Elastic store
func NewStore(cfg config.Configuration) store.Store {
	esClient, _ := elasticsearch6.NewDefaultClient()
	log.Printf("Here are the ES cluster info")
	log.Println(esClient.Info());
	//log.Printf("ServerID: %s", cfg.ServerID)
	//var clusterId string = cfg.ServerID
	log.Printf("ClusterID: %s, ServerID: %s", cfg.ClusterID, cfg.ServerID)
	var clusterId string = cfg.ClusterID
	if len(clusterId) == 0 {
		clusterId = cfg.ServerID
	}
	InitSequenceIndices(esClient, clusterId, "logs")
	InitSequenceIndices(esClient, clusterId, "events")
	InitStorageIndices(esClient, indicePrefix + "logs")
	InitStorageIndices(esClient, indicePrefix + "events")
	return &elasticStore{encoding.JSON, esClient, clusterId}
}

func InitStorageIndices(esClient *elasticsearch6.Client, indiceName string) {

	log.Printf("Checking if index <%s> already exists", indiceName)

	// check if the sequences index exists
	req := esapi.IndicesExistsRequest{
		Index: []string{indiceName},
	}
	res, err := req.Do(context.Background(), esClient)
	debugESResponse("IndicesExistsRequest:" + indiceName, res, err)
	defer res.Body.Close()
	log.Printf("Status Code for IndicesExistsRequest (%s): %d", indiceName, res.StatusCode)

	if res.StatusCode == 200 {
		log.Printf("Indice %s was found, nothing to do !", indiceName)
	}

	if res.StatusCode == 404 {
		log.Printf("Indice %s was not found, let's create it !", indiceName)

		requestBodyData := `
{
     "mappings": {
         "logs_or_event": {
             "_all": {"enabled": 0},
             "dynamic": "false",
             "properties": {
                 "iid": {
                     "type": "long",
                     "index": true
                 },
                 "clusterId": {
                     "type": "keyword",
                     "index": true
                 }
             }
         }
     }
}`

		// indice doest not exist, let's create it
		req := esapi.IndicesCreateRequest{
			Index: indiceName,
			Body: strings.NewReader(requestBodyData),
		}
		res, err := req.Do(context.Background(), esClient)
		debugESResponse("IndicesCreateRequest:" + indiceName, res, err)
		defer res.Body.Close()
		log.Printf("Status Code for IndicesCreateRequest (%s) : %d", indiceName, res.StatusCode)
		if res.IsError() {
			var rsp_IndicesCreateRequest map[string]interface{}
			json.NewDecoder(res.Body).Decode(&rsp_IndicesCreateRequest)
			log.Printf("Response for IndicesCreateRequest (%s) : %+v", indiceName, rsp_IndicesCreateRequest)
		}

	}

}

func InitSequenceIndices(esClient *elasticsearch6.Client, clusterId string, sequenceName string) {

	sequence_id := sequenceName + "_" + clusterId;
	log.Printf("Initializing index <%s> with document <%s> for for sequence management.", sequenceIndiceName, sequence_id)

	// check if the sequences index exists
	req := esapi.IndicesExistsRequest{
		Index: []string{sequenceIndiceName},
	}
	res, err := req.Do(context.Background(), esClient)
	debugESResponse("IndicesExistsRequest:" + sequenceIndiceName, res, err)
	defer res.Body.Close()
	log.Printf("Status Code for IndicesExistsRequest (%s): %d", sequenceIndiceName, res.StatusCode)

	if res.StatusCode == 404 {
		log.Printf("Indice %s was not found, let's create it !", sequenceIndiceName)

		requestBodyData := `
{
     "settings": {
         "number_of_shards": 1
     },
     "mappings": {
         "sequence": {
             "_all": {"enabled": false},
             "dynamic": "strict",
             "properties": {
                 "iid": {
                     "type": "long",
                     "index": false
                 }
             }
         }
     }
}`

		// indice doest not exist, let's create it
		req := esapi.IndicesCreateRequest{
			Index: sequenceIndiceName,
			Body: strings.NewReader(requestBodyData),
		}
		res, err := req.Do(context.Background(), esClient)
		debugESResponse("IndicesCreateRequest:" + sequenceIndiceName, res, err)
		defer res.Body.Close()
		log.Printf("Status Code for IndicesCreateRequest (%s) : %d", sequenceIndiceName, res.StatusCode)
		if res.IsError() {
			var rsp_IndicesCreateRequest map[string]interface{}
			json.NewDecoder(res.Body).Decode(&rsp_IndicesCreateRequest)
			log.Printf("Response for IndicesCreateRequest: %+v", rsp_IndicesCreateRequest)
		}

	}

	log.Printf("Searching for document in sequence index <%s> with ID <%s>", sequenceIndiceName, sequence_id)
	// check if the document concerning this sequence is present
	req_get := esapi.GetRequest{
		Index: sequenceIndiceName,
		DocumentType: "_sequence",
		DocumentID: sequence_id,
	}
	res, err = req_get.Do(context.Background(), esClient)
	debugESResponse("GetRequest:"  + sequenceIndiceName + "/" + sequence_id, res, err)
	log.Printf("\nStatus Code for GetRequest (%s, %s) : %d", sequenceIndiceName, sequence_id, res.StatusCode)
	defer res.Body.Close()

	if res.StatusCode == 404 {
		log.Printf("Document with ID <%s> was not found in indice <%s>, let's create it !", sequence_id, sequenceIndiceName)
		// init log sequence
		req_index := esapi.IndexRequest{
			Index:      sequenceIndiceName,
			DocumentID: sequence_id,
			DocumentType: "sequence",
			Body:       strings.NewReader(`{"iid" : 0}`),
			Refresh:    "true",
		}
		res, err = req_index.Do(context.Background(), esClient)
		debugESResponse("IndexRequest:" + sequenceIndiceName + "/" + sequence_id, res, err)
		log.Printf("\nStatus Code for IndexRequest (%s, %s) : %d", sequenceIndiceName, sequence_id, res.StatusCode)
		if res.IsError() {
			var rsp_IndexRequest map[string]interface{}
			json.NewDecoder(res.Body).Decode(&rsp_IndexRequest)
			log.Printf("\nResponse for IndexRequest: %+v", rsp_IndexRequest)
		}
	}

}

func debugESResponse(msg string, res *esapi.Response, err error) {
	if err != nil {
		log.Printf("[%s] Error while requesting ES : %+v", msg, err)
	} else if res.IsError() {
		var rsp map[string]interface{}
		json.NewDecoder(res.Body).Decode(&rsp)
		log.Printf("[%s] Response Error while requesting ES (%d): %+v", msg, res.StatusCode, rsp)
	} else {
		var rsp map[string]interface{}
		json.NewDecoder(res.Body).Decode(&rsp)
		log.Printf("[%s] Success ES request (%d): %+v", msg, res.StatusCode, rsp)
	}
}

func GetNextSequence(esClient *elasticsearch6.Client, clusterId string, sequenceName string) (float64, error) {
	sequence_id := sequenceName + "_" + clusterId;

	log.Printf("Updating log sequence for indice <%s> document <%s>", sequenceIndiceName, sequence_id)
	req_update := esapi.UpdateRequest{
		Index: sequenceIndiceName,
		DocumentID: sequence_id,
		DocumentType: "sequence",
		Body: strings.NewReader(`{"script": "ctx._source.iid += 1", "lang": "groovy"}`),
		Fields: []string{"iid"},
	}
	res, err := req_update.Do(context.Background(), esClient)
	log.Printf("Status Code for UpdateRequest: %d", res.StatusCode)
	defer res.Body.Close()

	if err != nil {
		return -1, err
	}

	if res.IsError() {
		return -1, errors.Wrap(err, "Error while upgrading sequence iid")
	} else {
		// Deserialize the response into a map.
		var r map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
			return -1, errors.Wrap(err, "Error parsing the response body")
		} else {
			// Print the response status and indexed document version.
			//fmt.Printf("\n update response: %+v", r)
			//fmt.Printf("\n next id is : %s", r["fields"])
			var s  = ((r["get"].(map[string]interface{}))["fields"].(map[string]interface{}))["iid"].([]interface{})[0]
			log.Printf("Next iid for %s is : %v (%T)", sequence_id, s, s)
			return s.(float64), nil
			//fmt.Printf("\n[%s] %s; version=%d", res.Status(), r["result"], int(r["_version"].(float64)))
		}
	}
}

func (c *elasticStore) Set(ctx context.Context, k string, v interface{}) error {
	log.Printf("About to Set data into ES, k: %s, v (%T) : %+v", k, v, v)

	if err := utils.CheckKeyAndValue(k, v); err != nil {
		return err
	}

	data, err := c.codec.Marshal(v)
	if err != nil {
		return errors.Wrapf(err, "failed to marshal value %+v due to error:%+v", v, err)
	}
	// TODO: remove
	// We still continue to store data into consul
	consulutil.StoreConsulKey(k, data)

	log.Printf("About to Set data into ES, data (%T): %s", data, data)

	// enrich the data by adding the clusterId
	var v2 interface{}
	json.Unmarshal(data, &v2)
	enrichedData := v2.(map[string]interface{})
	enrichedData["clusterId"] = c.clusterId

	// Extract indice name by parsing the key
	indexName, timestamp := c.extractIndexNameAndTimestamp(k)
	log.Printf("indexName is: %s, timestamp is: %s", indexName, timestamp)

	iid, _ := GetNextSequence(c.esClient, c.clusterId, indexName)
	enrichedData["iid"] = iid

	var jsonData []byte
	jsonData, err = json.Marshal(enrichedData)
	log.Printf("After enrichment, about to Set data into ES, k: %s, v (%T) : %+v", k, jsonData, string(jsonData))

	// Prepare ES request
	req := esapi.IndexRequest{
		Index:      indicePrefix + indexName,
		DocumentType: "logs_or_event",
		Body:       bytes.NewReader(jsonData),
		Refresh:    "true",
	}
	res, err := req.Do(context.Background(), c.esClient)
	debugESResponse("IndexRequest:" + indicePrefix + indexName, res, err)

	defer res.Body.Close()

	if err != nil {
		return err
	} else if res.IsError() {
		return errors.Errorf("Error while indexing document, response code was <%d> and response message was <%s>", res.StatusCode, res.String())
	} else {
		// Deserialize the response into a map.
		var r map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
			log.Printf("Error parsing the response body: %s", err)
		} else {
			// Print the response status and indexed document version.
			log.Printf("[%s] %s; version=%d", res.Status(), r["result"], int(r["_version"].(float64)))
		}
		return nil
	}
}

func (c *elasticStore) SetCollection(ctx context.Context, keyValues []store.KeyValueIn) error {
	log.Printf("SetCollection called")
	if keyValues == nil || len(keyValues) == 0 {
		return nil
	}
	ctx, errGroup, consulStore := consulutil.WithContext(ctx)
	for _, kv := range keyValues {
		if err := utils.CheckKeyAndValue(kv.Key, kv.Value); err != nil {
			return err
		}
		log.Printf("About to Set data into ES, k: %s", kv.Key)

		data, err := c.codec.Marshal(kv.Value)
		if err != nil {
			return errors.Wrapf(err, "failed to marshal value %+v due to error:%+v", kv.Value, err)
		}
		log.Printf("About to Set data into ES, data: %s", data)

		consulStore.StoreConsulKey(kv.Key, data)
	}
	return errGroup.Wait()
}

func (c *elasticStore) Get(k string, v interface{}) (bool, error) {
	log.Printf("Get called, k: %s, v (%T) : %+v", k, v, v)
	if err := utils.CheckKeyAndValue(k, v); err != nil {
		return false, err
	}

	found, value, err := consulutil.GetValue(k)
	if err != nil || !found {
		return found, err
	}

	return true, errors.Wrapf(c.codec.Unmarshal(value, v), "failed to unmarshal data:%q", string(value))
}

func (c *elasticStore) Exist(k string) (bool, error) {
	log.Printf("Exist called k: %s", k)
	if err := utils.CheckKey(k); err != nil {
		return false, err
	}

	found, _, err := consulutil.GetValue(k)
	if err != nil {
		return false, err
	}
	return found, nil
}

func (c *elasticStore) Keys(k string) ([]string, error) {
	log.Printf("Keys called k: %s", k)
	return consulutil.GetKeys(k)
}

func (c *elasticStore) Delete(ctx context.Context, k string, recursive bool) error {
	log.Printf("Delete called k: %s, recursive: %t", k, recursive)
	return consulutil.Delete(k, recursive)
}

func (c *elasticStore) GetLastModifyIndex(k string) (uint64, error) {
	log.Printf("GetLastModifyIndex called k: %s", k)
	_, qm, err := consulutil.GetKV().Get(k, nil)
	if err != nil || qm == nil {
		return 0, errors.Errorf("Failed to retrieve last index for key:%q", k)
	}
	return qm.LastIndex, nil
}

func (c *elasticStore) List(ctx context.Context, k string, waitIndex uint64, timeout time.Duration) ([]store.KeyValueOut, uint64, error) {
	log.Printf("List called k: %s, waitIndex: %d, timeout: %v" , k, waitIndex, timeout)
	if err := utils.CheckKey(k); err != nil {
		return nil, 0, err
	}

	kvps, qm, err := consulutil.GetKV().List(k, &api.QueryOptions{WaitIndex: waitIndex, WaitTime: timeout})
	if err != nil || qm == nil {
		return nil, 0, err
	}
	if kvps == nil {
		return nil, qm.LastIndex, err
	}

	values := make([]store.KeyValueOut, 0)
	for _, kvp := range kvps {
		var value map[string]interface{}
		if err := c.codec.Unmarshal(kvp.Value, &value); err != nil {
			return nil, 0, errors.Wrapf(err, "failed to unmarshal stored value: %q", string(kvp.Value))
		}

		values = append(values, store.KeyValueOut{
			Key:             kvp.Key,
			LastModifyIndex: kvp.ModifyIndex,
			Value:           value,
			RawValue:        kvp.Value,
		})
	}
	return values, qm.LastIndex, nil
}
