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
	"strconv"
)

var indexNameAndTimestampRegex = regexp.MustCompile(`(?m)\_yorc\/(\w+)\/.+\/(.*)`)
//var indexNameRegex = regexp.MustCompile(`(?m)\_yorc\/(\w+)\/.*`)
var indexNameAndDeploymentIdRegex = regexp.MustCompile(`(?m)\_yorc\/(\w+)\/?(.+)?\/?`)
// All index used by yorc will be prefixed by this prefix
const indicePrefix = "yorc_"
// When querying logs and event, we wait this timeout before each request when it returns nothing (until something is returned or the waitTimeout is reached)
const esTimeout = (10 * time.Second)
// This timeout is used to wait for more than refresh_interval = 1s when querying logs and events indexes
const esRefreshTimeout = (3 * time.Second)
var pfalse = false

type elasticStore struct {
	codec encoding.Codec
	esClient *elasticsearch6.Client
	clusterId string
}

type LastIndexResponse struct {
	hits Hits `json:"hits"`
	aggregations LogOrEventAggregation `json:"aggregations"`
}
type Hits struct {
	total int `json:"total"`
}
type LogOrEventAggregation struct {
	logs_or_events LastIndexAggregation `json:"logs_or_events"`
}
type LastIndexAggregation struct {
	last_index Int64Value `json:"last_index"`
}
type Int64Value struct {
	value uint64 `json:"value"`
}

func (c *elasticStore) extractIndexNameAndTimestamp(k string) (string, string) {
	var indexName, timetstamp string
	res := indexNameAndTimestampRegex.FindAllStringSubmatch(k, -1)
	for i := range res {
		indexName = res[i][1]
		timetstamp = res[i][2]
	}
	return indexName, timetstamp
}

func (c *elasticStore) extractIndexNameAndDeploymentId(k string) (string, string) {
	var indexName, deploymentId string
	res := indexNameAndDeploymentIdRegex.FindAllStringSubmatch(k, -1)
	for i := range res {
		indexName = res[i][1]
		if len(res[i]) == 3 {
			deploymentId = res[i][2]
			if strings.HasSuffix(deploymentId, "/") {
				deploymentId = deploymentId[:len(deploymentId)-1]
			}
		}
	}
	return indexName, deploymentId
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
	InitStorageIndices(esClient, indicePrefix + "logs")
	InitStorageIndices(esClient, indicePrefix + "events")
	debugIndexSetting(esClient, indicePrefix + "logs")
	debugIndexSetting(esClient, indicePrefix + "events")
	return &elasticStore{encoding.JSON, esClient, clusterId}
}

func InitStorageIndices(esClient *elasticsearch6.Client, indiceName string) {

	log.Printf("Checking if index <%s> already exists", indiceName)

	// check if the sequences index exists
	req := esapi.IndicesExistsRequest{
		Index: []string{indiceName},
		ExpandWildcards: "none",
		AllowNoIndices: &pfalse,

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
     "settings": {
         "refresh_interval": "1s"
     },
     "mappings": {
         "logs_or_event": {
             "_all": {"enabled": false},
             "dynamic": "false",
             "properties": {
                 "clusterId": {
                     "type": "keyword",
                     "index": true
                 },
                 "deploymentId": {
                     "type": "keyword",
                     "index": true
                 },
                 "iid": {
                     "type": "long",
                     "index": true
                 },
                 "iid_str": {
                     "type": "keyword",
                     "index": false
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

func debugIndexSetting(esClient *elasticsearch6.Client, indiceName string) {
	log.Debugf("Get settings for index <%s>", indiceName)
	req_settings := esapi.IndicesGetSettingsRequest{
		Index: []string{indiceName},
		Pretty: true,
	}
	res, err := req_settings.Do(context.Background(), esClient)
	debugESResponse("IndicesGetSettingsRequest:" + indiceName, res, err)
	defer res.Body.Close()
}

func debugESResponse(msg string, res *esapi.Response, err error) {
	if err != nil {
		log.Debugf("[%s] Error while requesting ES : %+v", msg, err)
	} else if res.IsError() {
		var rsp map[string]interface{}
		json.NewDecoder(res.Body).Decode(&rsp)
		log.Debugf("[%s] Response Error while requesting ES (%d): %+v", msg, res.StatusCode, rsp)
	} else {
		var rsp map[string]interface{}
		json.NewDecoder(res.Body).Decode(&rsp)
		log.Debugf("[%s] Success ES request (%d): %+v", msg, res.StatusCode, rsp)
	}
}

func RefreshIndex(esClient *elasticsearch6.Client, indexName string) {
	req_get := esapi.IndicesRefreshRequest{
		Index: []string{indexName},
		ExpandWildcards: "none",
		AllowNoIndices: &pfalse,
	}
	res, err := req_get.Do(context.Background(), esClient)
	defer res.Body.Close()
	debugESResponse("IndicesRefreshRequest:"  + indexName, res, err)
}

func (c *elasticStore) Set(ctx context.Context, k string, v interface{}) error {
	log.Debugf("About to Set data into ES, k: %s, v (%T) : %+v", k, v, v)

	if err := utils.CheckKeyAndValue(k, v); err != nil {
		return err
	}

	data, err := c.codec.Marshal(v)
	if err != nil {
		return errors.Wrapf(err, "failed to marshal value %+v due to error:%+v", v, err)
	}

	//log.Debugf("About to Set data into ES, data (%T): %s", data, data)

	// enrich the data by adding the clusterId
	var v2 interface{}
	json.Unmarshal(data, &v2)
	enrichedData := v2.(map[string]interface{})
	enrichedData["clusterId"] = c.clusterId

	// Extract indice name and timestamp by parsing the key
	indexName, timestamp := c.extractIndexNameAndTimestamp(k)
	log.Debugf("indexName is: %s, timestamp: %s", indexName, timestamp)

	// Convert timestamp to an int64
	eventDate, err := time.Parse(time.RFC3339Nano, timestamp)
	if err != nil {
		return errors.Wrapf(err, "failed to parse timestamp %+v as time, error was: %+v", timestamp, err)
	}
	// Convert to UnixNano int64
	iid := eventDate.UnixNano()
	// Add the property iid to the document
	enrichedData["iid"] = iid
	// We also add it's string representation to avoid decoding issue
	enrichedData["iid_str"] = strconv.FormatInt(iid, 10)

	var jsonData []byte
	jsonData, err = json.Marshal(enrichedData)

	log.Debugf("After enrichment, about to Set data into ES, k: %s, v (%T) : %+v", k, jsonData, string(jsonData))
	// Prepare ES request
	req := esapi.IndexRequest{
		Index:      indicePrefix + indexName,
		DocumentType: "logs_or_event",
		Body:       bytes.NewReader(jsonData),
	}
	res, err := req.Do(context.Background(), c.esClient)
	debugESResponse("IndexRequest:" + indicePrefix + indexName, res, err)

	defer res.Body.Close()

	if err != nil {
		return err
	} else if res.IsError() {
		return errors.Errorf("Error while indexing document, response code was <%d> and response message was <%s>", res.StatusCode, res.String())
	} else {
		return nil
	}
}

func (c *elasticStore) SetCollection(ctx context.Context, keyValues []store.KeyValueIn) error {
	log.Println(strings.Repeat("=", 37))
	log.Println(strings.Repeat("=", 37))
	log.Println(strings.Repeat("=", 37))
	log.Printf("SetCollection called")
	log.Println(strings.Repeat("=", 37))
	log.Println(strings.Repeat("=", 37))
	log.Println(strings.Repeat("=", 37))
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
	log.Println(strings.Repeat("=", 37))
	log.Println(strings.Repeat("=", 37))
	log.Println(strings.Repeat("=", 37))
	log.Printf("Get called, k: %s, v (%T) : %+v", k, v, v)
	log.Println(strings.Repeat("=", 37))
	log.Println(strings.Repeat("=", 37))
	log.Println(strings.Repeat("=", 37))
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
	log.Println(strings.Repeat("=", 37))
	log.Println(strings.Repeat("=", 37))
	log.Println(strings.Repeat("=", 37))
	log.Printf("Exist called k: %s", k)
	log.Println(strings.Repeat("=", 37))
	log.Println(strings.Repeat("=", 37))
	log.Println(strings.Repeat("=", 37))
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
	log.Println(strings.Repeat("=", 37))
	log.Println(strings.Repeat("=", 37))
	log.Println(strings.Repeat("=", 37))
	log.Printf("Keys called k: %s", k)
	log.Println(strings.Repeat("=", 37))
	log.Println(strings.Repeat("=", 37))
	log.Println(strings.Repeat("=", 37))
	return consulutil.GetKeys(k)
}

func (c *elasticStore) Delete(ctx context.Context, k string, recursive bool) error {
	log.Debugf("Delete called k: %s, recursive: %t", k, recursive)

	// Extract indice name and deploymentId by parsing the key
	indexName, deploymentId := c.extractIndexNameAndDeploymentId(k)
	log.Debugf("indexName is: %s, deploymentId is: %s", indexName, deploymentId)

	query := `{"query" : { "bool" : { "must" : [{ "term": { "clusterId" : "` + c.clusterId + `" }}, { "term": { "deploymentId" : "` + deploymentId + `" }}]}}}`
	log.Debugf("query is : %s", query)

	var MaxInt = 1024000

	req := esapi.DeleteByQueryRequest{
		Index: []string{indicePrefix + indexName},
		Size: &MaxInt,
		Body: strings.NewReader(query),
	}
	res, err := req.Do(context.Background(), c.esClient)
	debugESResponse("DeleteByQueryRequest:" + indicePrefix + indexName, res, err)

	defer res.Body.Close()
	return err
}

func (c *elasticStore) GetLastModifyIndex(k string) (uint64, error) {
	log.Debugf("GetLastModifyIndex called k: %s", k)

	// Extract indice name and deploymentId by parsing the key
	indexName, deploymentId := c.extractIndexNameAndDeploymentId(k)
	log.Debugf("indexName is: %s, deploymentId is: %s", indexName, deploymentId)

	return c.InternalGetLastModifyIndex(indicePrefix + indexName, deploymentId)

}

func (c *elasticStore) InternalGetLastModifyIndex(indexName string, deploymentId string) (uint64, error) {

	// The last_index is query by using ES aggregation query ~= MAX(iid) HAVING deploymentId AND clusterId
	query := getLastModifiedIndexQuery(c.clusterId, deploymentId)
	log.Debugf("getLastModifiedIndexQuery is : %s", query)

	res, err := c.esClient.Search(
		c.esClient.Search.WithContext(context.Background()),
		c.esClient.Search.WithIndex(indexName),
		c.esClient.Search.WithSize(0),
		c.esClient.Search.WithBody(strings.NewReader(query)),
	)
	if err != nil {
		log.Printf("ERROR: %s", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		var e map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
			log.Printf("error parsing the response body: %s", err)
		} else {
			// Print the response status and error information.
			log.Printf("[%s] %s: %s",
				res.Status(),
				e["error"].(map[string]interface{})["type"],
				e["error"].(map[string]interface{})["reason"],
			)
		}
	}

	var r LastIndexResponse
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		log.Printf("Error parsing the response body: %s", err)
	}

	hits := r.hits.total
	var last_index uint64 = 0
	if (hits > 0) {
		last_index = r.aggregations.logs_or_events.last_index.value
	}

	// Print the response status, number of results, and request duration.
	log.Printf(
		"[%s] %d hits; last_index: %d",
		res.Status(),
		hits,
		last_index,
	)

	return last_index, nil
}

func (c *elasticStore) List(ctx context.Context, k string, waitIndex uint64, timeout time.Duration) ([]store.KeyValueOut, uint64, error) {
	log.Debugf("List called k: %s, waitIndex: %d, timeout: %v" , k, waitIndex, timeout)
	if err := utils.CheckKey(k); err != nil {
		return nil, 0, err
	}

	// Extract indice name by parsing the key
	indexName, deploymentId := c.extractIndexNameAndDeploymentId(k)
	log.Debugf("indexName is: %s, deploymentId", indexName, deploymentId)

	query := getListQuery(c.clusterId, deploymentId, waitIndex, 0)

	now := time.Now()
	end := now.Add(timeout - esRefreshTimeout)
	log.Debugf("Now is : %v, date after timeout will be %v (ES timeout duration will be %v)", now, end, timeout - esRefreshTimeout)
	var values = make([]store.KeyValueOut, 0)
	var lastIndex = waitIndex
	var hits = 0
	var err error
	for {
		hits, values, lastIndex, err = c.ListEs(indicePrefix + indexName, query, waitIndex);
		if err != nil {
			return values, waitIndex, errors.Wrapf(err, "Failed to request ES logs or events, error was: %+v", err)
		}
		now := time.Now()
		if hits > 0 || now.After(end) {
			break
		}
		log.Debugf("Hits is %d and timeout not reached, sleeping %v ...", hits, esTimeout)
		time.Sleep(esTimeout)
	}
	if (hits > 0) {
		query := getListQuery(c.clusterId, deploymentId, waitIndex, lastIndex)
		//RefreshIndex(c.esClient, indicePrefix + indexName);
		time.Sleep(esRefreshTimeout)
		oldLen := len(values)
		oldHits := hits
		hits, values, lastIndex, err = c.ListEs(indicePrefix + indexName, query, waitIndex);
		if err != nil {
			return values, waitIndex, errors.Wrapf(err, "Failed to request ES logs or events (after waiting for refresh), error was: %+v", err)
		}
		if len(values) > oldLen {
			log.Printf("%d > %d so sleeping %v to wait for ES refresh was usefull (index %s), hit was %d (and %d after timeout)", len(values), oldLen, esRefreshTimeout, indicePrefix + indexName, oldHits, hits)
		}
	}
	log.Printf("List called result k: %s, waitIndex: %d, timeout: %v, LastIndex: %d, len(values): %d" , k, waitIndex, timeout, lastIndex, len(values))
	return values, lastIndex, err
}

func getLastModifiedIndexQuery(clusterId string, deploymentId string) string {
	var query string
	if len(deploymentId) == 0 {
		query = `
{
    "aggs" : {
        "logs_or_events" : {
            "filter" : {
				"term": { "clusterId": "` + clusterId + `" }
            },
            "aggs" : {
                "last_index" : { "max" : { "field" : "iid" } }
            }
        }
    }
}`
	} else {
		query = `
{
    "aggs" : {
        "logs_or_events" : {
            "filter" : {
                "bool": {
                    "must": [
                        { "term": { "deploymentId": "` + deploymentId + `" } },
                        { "term": { "clusterId": "` + clusterId + `" } }
                     ]
                }
            },
            "aggs" : {
                "last_index" : { "max" : { "field" : "iid" } }
            }
        }
    }
}`
	}
	return query
}

func getListQuery(clusterId string, deploymentId string, waitIndex uint64, maxIndex uint64) string {

	var rangeQuery, query string
	if maxIndex > 0 {
		rangeQuery = `
            {
               "range":{
                  "iid":{
                     "gt":` + strconv.FormatUint(waitIndex, 10) + `,
					 "lte":` + strconv.FormatUint(maxIndex, 10) + `
                  }
               }
            }`
	} else {
		rangeQuery = `
            {
               "range":{
                  "iid":{
                     "gt":` + strconv.FormatUint(waitIndex, 10) + `
                  }
               }
            }`
	}
	if len(deploymentId) == 0 {
		query = `
{
   "query":{
      "bool":{
         "must":[
            {
               "term":{
                  "clusterId":"` + clusterId + `"
               }
            },` + rangeQuery + `
         ]
      }
   }
}`
	} else {
		query = `
{
   "query":{
      "bool":{
         "must":[
            {
               "term":{
                  "clusterId":"` + clusterId + `"
               }
            },
            {
               "term":{
                  "deploymentId":"` + deploymentId + `"
               }
            },` + rangeQuery + `
         ]
      }
   }
}`
	}
	return query
}

func (c *elasticStore) ListEs(index string, query string, waitIndex uint64) (int, []store.KeyValueOut, uint64, error) {
	log.Debugf("Search ES %s using query: %s", index, query)

	values := make([]store.KeyValueOut, 0)

	res, err := c.esClient.Search(
		c.esClient.Search.WithContext(context.Background()),
		c.esClient.Search.WithIndex(index),
		c.esClient.Search.WithSize(1000),
		c.esClient.Search.WithBody(strings.NewReader(query)),
		c.esClient.Search.WithSort("iid:asc"),
	)
	if err != nil {
		log.Println(strings.Repeat("§", 37))
		log.Printf("Failed to perform ES search on index %s, query was: <%s>, error was: %+v", index, query, err)
		log.Println(strings.Repeat("§", 37))
		return 0, values, waitIndex, errors.Wrapf(err, "Failed to perform ES search on index %s, query was: <%s>, error was: %+v", index, query, err)
	}
	defer res.Body.Close()

	if res.IsError() {
		var e map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
			log.Println(strings.Repeat("§", 37))
			log.Printf("An error occurred while performing ES search on index %s, query was: <%s>, response code was %d (%s). Wasn't able to decode response body !", index, query, res.StatusCode, res.Status())
			log.Println(strings.Repeat("§", 37))
			return 0, values, waitIndex, errors.Wrapf(err, "An error occurred while performing ES search on index %s, query was: <%s>, response code was %d (%s). Wasn't able to decode response body !", index, query, res.StatusCode, res.Status())
		} else {
			log.Println(strings.Repeat("§", 37))
			log.Printf("An error occurred while performing ES search on index %s, query was: <%s>, response code was %d (%s). Response body was: %+v", index, query, res.StatusCode, res.Status(), e)
			log.Println(strings.Repeat("§", 37))
			return 0, values, waitIndex, errors.Wrapf(err, "An error occurred while performing ES search on index %s, query was: <%s>, response code was %d (%s). Response body was: %+v", index, query, res.StatusCode, res.Status(), e)
		}
	}

	var r map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		log.Println(strings.Repeat("§", 37))
		log.Printf("Not able to decode ES response while performing ES search on index %s, query was: <%s>, response code was %d (%s)", index, query, res.StatusCode, res.Status())
		log.Println(strings.Repeat("§", 37))
		return 0, values, waitIndex, errors.Wrapf(err, "Not able to decode ES response while performing ES search on index %s, query was: <%s>, response code was %d (%s)", index, query, res.StatusCode, res.Status())
	}

	hits := int(r["hits"].(map[string]interface{})["total"].(float64))
	duration := int(r["took"].(float64))
	log.Debugf("Search ES request on index %s took %dms, hits=%d, response code was %d (%s)", index, duration, hits, res.StatusCode, res.Status())

	var lastIndex = waitIndex

	// Print the ID and document source for each hit.
	for _, hit := range r["hits"].(map[string]interface{})["hits"].([]interface{}) {
		id := hit.(map[string]interface{})["_id"].(string)
		source := hit.(map[string]interface{})["_source"].(map[string]interface{})
		iid := source["iid_str"]
		iid_uint64, err := strconv.ParseUint(iid.(string), 10, 64)
		if err != nil {
			log.Println(strings.Repeat("#", 37))
			log.Printf("Not able to parse iid_str property %s as uint64, document id: %s, source: %+v, ignoring this document !", iid, id, source);
			log.Println(strings.Repeat("#", 37))
		} else {
			jsonString, err := json.Marshal(source)
			if err != nil {
				log.Println(strings.Repeat("#", 37))
				log.Printf("Not able to marshall document source, document id: %s, source: %+v, ignoring this document !", id, source);
				log.Println(strings.Repeat("#", 37))
			} else {
				lastIndex = iid_uint64
				// append value to result
				values = append(values, store.KeyValueOut{
					Key:             id,
					LastModifyIndex: iid_uint64,
					Value:           source,
					RawValue:        jsonString,
				})
			}
		}
	}

	log.Printf("ListEs called result waitIndex: %d, LastIndex: %d, len(values): %d", waitIndex, lastIndex, len(values))
	return hits, values, lastIndex, nil
}

