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
	"math"
	"fmt"
)

var indexNameAndTimestampRegex = regexp.MustCompile(`(?m)\_yorc\/(\w+)\/.+\/(.*)`)
//var indexNameRegex = regexp.MustCompile(`(?m)\_yorc\/(\w+)\/.*`)
var indexNameAndDeploymentIdRegex = regexp.MustCompile(`(?m)\_yorc\/(\w+)\/?(.+)?\/?`)
// All index used by yorc will be prefixed by this prefix
var indicePrefix = "yorc_"
// When querying logs and event, we wait this timeout before each request when it returns nothing (until something is returned or the waitTimeout is reached)
var esQueryPeriod = 5 * time.Second
// This timeout is used to wait for more than refresh_interval = 1s when querying logs and events indexes
var esRefreshTimeout = (5 * time.Second)
// This is the maximum size (in term of number of documents) of bulk request sent while migrating data
var maxBulkCount = 1000
// This is approximately the maximum size (in kB) of bulk request sent while migrating data (+/- 2 bytes)
var maxBulkSize = 4000
var pfalse = false

type elasticStore struct {
	codec encoding.Codec
	esClient *elasticsearch6.Client
	clusterId string
}

type lastIndexResponse struct {
	hits         hits                  `json:"hits"`
	aggregations logOrEventAggregation `json:"aggregations"`
}
type hits struct {
	total int 	`json:"total"`
}
type logOrEventAggregation struct {
	logs_or_events lastIndexAggregation `json:"logs_or_events"`
}
type lastIndexAggregation struct {
	doc_count  int         `json:"doc_count"`
	last_index stringValue `json:"last_index"`
}
type stringValue struct {
	value string 	`json:"value"`
}

func ExtractIndexNameAndTimestamp(k string) (indexName string, timestamp string) {
	res := indexNameAndTimestampRegex.FindAllStringSubmatch(k, -1)
	for i := range res {
		indexName = res[i][1]
		timestamp = res[i][2]
	}
	return indexName, timestamp
}

func ExtractIndexNameAndDeploymentId(k string) (indexName string, deploymentId string) {
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
func NewStore(cfg config.Configuration, storeConfig config.Store) store.Store {

	// Just fail if this storage is used for anything different from logs or events
	for _, t := range storeConfig.Types {
		if t != "Log" && t != "Event" {
			log.Fatalf("Elastic store is not able to manage <%s>, just Log or Event, please change your config", t)
		}
	}

	// Get specific config from storage properties
	storeProperties := storeConfig.Properties
	var esConfig elasticsearch6.Config

	// The ES urls is required
	if (storeProperties.IsSet("es_urls")) {
		es_urls := storeProperties.GetStringSlice("es_urls")
		if (es_urls == nil || len(es_urls) == 0) {
			log.Fatalf("Not able to get ES configuration for elastic store, es_urls store property seems empty : %+v", storeProperties.Get("es_urls"))
		}
		esConfig = elasticsearch6.Config{
			Addresses: es_urls,
		}
	} else {
		log.Fatal("Not able to get ES configuration for elastic store, es_urls store property should be set !")
	}

	if (storeProperties.IsSet("es_query_period")) {
		esQueryPeriod = storeProperties.GetDuration("es_query_period")
	}
	if (storeProperties.IsSet("es_refresh_wait_timeout")) {
		esRefreshTimeout = storeProperties.GetDuration("es_refresh_wait_timeout")
	}
	if (storeProperties.IsSet("index_prefix")) {
		indicePrefix = storeProperties.GetString("index_prefix")
	}
	if (storeProperties.IsSet("max_bulk_size")) {
		maxBulkSize = storeProperties.GetInt("max_bulk_size")
	}
	if (storeProperties.IsSet("max_bulk_count")) {
		maxBulkCount = storeProperties.GetInt("max_bulk_count")
	}
	log.Printf("Will query ES for logs or events every %v and will wait for index refresh during %v", esQueryPeriod, esRefreshTimeout)
	log.Printf("While migrating data, the max bulk request size will be %d", maxBulkSize)
	log.Printf("Index prefix will be %s", indicePrefix)
	log.Printf("Will use this ES client configuration: %+v", esConfig)

	esClient, _ := elasticsearch6.NewClient(esConfig)

	log.Printf("Here is the ES cluster info")
	log.Println(esClient.Info());
	log.Printf("ClusterID: %s, ServerID: %s", cfg.ClusterID, cfg.ServerID)
	var clusterId string = cfg.ClusterID
	if len(clusterId) == 0 {
		clusterId = cfg.ServerID
	}

	InitStorageIndices(esClient, indicePrefix + "logs")
	InitStorageIndices(esClient, indicePrefix + "events")
	DebugIndexSetting(esClient, indicePrefix + "logs")
	DebugIndexSetting(esClient, indicePrefix + "events")
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
	DebugESResponse("IndicesExistsRequest:" + indiceName, res, err)
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
		DebugESResponse("IndicesCreateRequest:" + indiceName, res, err)
		defer res.Body.Close()
		log.Printf("Status Code for IndicesCreateRequest (%s) : %d", indiceName, res.StatusCode)
		if res.IsError() {
			var rsp_IndicesCreateRequest map[string]interface{}
			json.NewDecoder(res.Body).Decode(&rsp_IndicesCreateRequest)
			log.Printf("Response for IndicesCreateRequest (%s) : %+v", indiceName, rsp_IndicesCreateRequest)
		}

	}

}

func DebugIndexSetting(esClient *elasticsearch6.Client, indiceName string) {
	log.Debugf("Get settings for index <%s>", indiceName)
	req_settings := esapi.IndicesGetSettingsRequest{
		Index: []string{indiceName},
		Pretty: true,
	}
	res, err := req_settings.Do(context.Background(), esClient)
	DebugESResponse("IndicesGetSettingsRequest:" + indiceName, res, err)
	defer res.Body.Close()
}

func DebugESResponse(msg string, res *esapi.Response, err error) {
	if err != nil {
		log.Debugf("[%s] Error while requesting ES : %+v", msg, err)
	} else if res.IsError() {
		var rsp map[string]interface{}
		json.NewDecoder(res.Body).Decode(&rsp)
		log.Debugf("[%s] Response Error while requesting ES (%d): %+v", msg, res.StatusCode, rsp)
	} else {
		var rsp map[string]interface{}
		json.NewDecoder(res.Body).Decode(&rsp)
		log.Debugf("[%s] Success ES response (%d): %+v", msg, res.StatusCode, rsp)
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
	DebugESResponse("IndicesRefreshRequest:"  + indexName, res, err)
}


func (s *elasticStore) BuildElasticDocument(k string, v interface{}) (string, map[string]interface{}, error) {
	var document map[string]interface{}

	// Extract indice name and timestamp by parsing the key
	indexName, timestamp := ExtractIndexNameAndTimestamp(k)
	log.Debugf("indexName is: %s, timestamp: %s", indexName, timestamp)

	// Convert timestamp to an int64
	eventDate, err := time.Parse(time.RFC3339Nano, timestamp)
	if err != nil {
		return indexName, document, errors.Wrapf(err, "failed to parse timestamp %+v as time, error was: %+v", timestamp, err)
	}
	// Convert to UnixNano int64
	iid := eventDate.UnixNano()

	// v is a json.RawMessage
	data, err := s.codec.Marshal(v)
	if err != nil {
		return indexName, document, errors.Wrapf(err, "failed to marshal value %+v due to error:%+v", v, err)
	}
	var unmarshaledDocument interface{}
	json.Unmarshal(data, &unmarshaledDocument)
	document = unmarshaledDocument.(map[string]interface{})

	// enrich the document by adding the clusterId
	document["clusterId"] = s.clusterId

	// Add a sortable string representation of the iid to the document
	// TODO ?? ensure we have 19 cars ?
	document["iid"] = strconv.FormatInt(iid, 10)

	return indexName, document, nil
}

func (s *elasticStore) Set(ctx context.Context, k string, v interface{}) error {
	log.Debugf("Set called will key %s", k)

	if err := utils.CheckKeyAndValue(k, v); err != nil {
		return err
	}

	indexName, document, err := s.BuildElasticDocument(k, v)
	if err != nil {
		return err
	}

	body, err := s.codec.Marshal(document)
	if err != nil {
		return errors.Wrapf(err, "Failed to marshal document %+v due to error: %+v", document, err)
	}
	if log.IsDebug() {
		log.Debugf("About to index this document into ES index <%s> : %+v", indexName, string(body))
	}

	// Prepare ES request
	req := esapi.IndexRequest{
		Index:      GetIndexName(indexName),
		DocumentType: "logs_or_event",
		Body:       bytes.NewReader(body),
	}
	res, err := req.Do(context.Background(), s.esClient)
	DebugESResponse("IndexRequest:" + GetIndexName(indexName), res, err)

	defer res.Body.Close()

	if err != nil {
		return err
	} else if res.IsError() {
		return errors.Errorf("Error while indexing document, response code was <%d> and response message was <%s>", res.StatusCode, res.String())
	} else {
		return nil
	}
}

func GetIndexName(storeType string) string {
	return indicePrefix + storeType
}

func (s *elasticStore) SetCollection(ctx context.Context, keyValues []store.KeyValueIn) error {
	totalDocumentCount := len(keyValues)
	log.Printf("SetCollection called with an array of size %d", totalDocumentCount)

	if keyValues == nil || totalDocumentCount == 0 {
		return nil
	}

	iterationCount := int(math.Ceil((float64(totalDocumentCount) / float64(maxBulkSize))))
	log.Printf("max_bulk_size is %d, so a minimum of %d iterations will be necessary to bulk insert data of total length %d", maxBulkSize, iterationCount + 1, totalDocumentCount)

	// The current index in []keyValues (also the number of documents indexed)
	var kvi int = 0
	// The number of iterations
	var i int = 0
	for {
		if kvi == totalDocumentCount {
			break
		}
		fmt.Printf("Bulk iteration #%d", i)

		body := make([]byte, 0)
		bulkRequestSize := 0
		for {
			if kvi == totalDocumentCount || bulkRequestSize == maxBulkSize {
				break
			}

			kv := keyValues[kvi]
			if err := utils.CheckKeyAndValue(kv.Key, kv.Value); err != nil {
				return err
			}

			indexName, document, err := s.BuildElasticDocument(kv.Key, kv.Value)
			if err != nil {
				return err
			}

			// The bulk action
			index := `{ "index" : { "_index" : "` + GetIndexName(indexName) + `", "_type" : "logs_or_event" } }`

			// Marshal the document as byte array
			data, err := s.codec.Marshal(document)
			if err != nil {
				return errors.Wrapf(err, "failed to marshal value %+v due to error:%+v", kv.Value, err)
			}
			// 4 = len("\n\n\n")
			estimatedBodySize := len(body) + len(index) + len(data) + 6
			if estimatedBodySize > maxBulkSize * 1024 {
				log.Printf("The limit of bulk size (%d kB) will be reached (%d > %d), the current document will be sent in the next bulk request", maxBulkSize, estimatedBodySize, maxBulkSize * 1024)
				break
			} else {
				log.Debugf("Document built from key %s added to bulk request body, indexName was %s", kv.Key, indexName)

				body = append(body, index...)
				body = append(body, "\n"...)
				body = append(body, data...)
				body = append(body, "\n"...)

				kvi++;
				bulkRequestSize++;
			}
		}

		// The bulk request must be terminated by a newline
		body = append(body, "\n"...)
		log.Printf("About to bulk request index using %d documents (%d bytes)", bulkRequestSize, len(body))
		if log.IsDebug() {
			log.Debugf("About to send bulk request query to ES: %s", string(body))
		}

		// Prepare ES bulk request
		req := esapi.BulkRequest{
			Body: bytes.NewReader(body),
		}
		res, err := req.Do(context.Background(), s.esClient)
		//DebugESResponse("BulkRequest", res, err)

		defer res.Body.Close()

		if err != nil {
			return err
		} else if res.IsError() {
			return errors.Errorf("Error while sending bulk request, response code was <%d> and response message was <%s>", res.StatusCode, res.String())
		} else {
			var rsp map[string]interface{}
			json.NewDecoder(res.Body).Decode(&rsp)
			if rsp["errors"].(bool) {
				// The bulk request contains errors
				return errors.Errorf("The bulk request succeeded, but the response contains errors : %+v", rsp)
			} else {
				log.Printf("Bulk request containing %d documents (%d bytes) has been accepted without errors", bulkRequestSize, len(body))
			}
		}
		// increment the number of iterations
		i++
	}
	log.Printf("A total of %d documents have been successfully indexed using %d bulk requests", kvi, iterationCount)

	return nil
}

func (s *elasticStore) Get(k string, v interface{}) (bool, error) {
	if err := utils.CheckKeyAndValue(k, v); err != nil {
		return false, err
	}
	// This function is not used for logs nor events
	log.Fatalf("Function Get(string, interface{}) not yet implemented for Elastic store !")
	return false, errors.Errorf("Function Get(string, interface{}) not yet implemented for Elastic store !")
}

func (s *elasticStore) Exist(k string) (bool, error) {
	if err := utils.CheckKey(k); err != nil {
		return false, err
	}
	// This function is not used for logs nor events
	log.Fatalf("Function Exist(string) not yet implemented for Elastic store !")
	return false, errors.Errorf("Function Exist(string) not yet implemented for Elastic store !")
}

func (s *elasticStore) Keys(k string) ([]string, error) {
	// This function is not used for logs nor events
	log.Fatalf("Function Keys(string) not yet implemented for Elastic store !")
	return nil, errors.Errorf("Function Keys(string) not yet implemented for Elastic store !")
}

func (s *elasticStore) Delete(ctx context.Context, k string, recursive bool) error {
	log.Debugf("Delete called k: %s, recursive: %t", k, recursive)

	// Extract indice name and deploymentId by parsing the key
	indexName, deploymentId := ExtractIndexNameAndDeploymentId(k)
	log.Debugf("indexName is: %s, deploymentId is: %s", indexName, deploymentId)

	query := `{"query" : { "bool" : { "must" : [{ "term": { "clusterId" : "` + s.clusterId + `" }}, { "term": { "deploymentId" : "` + deploymentId + `" }}]}}}`
	log.Debugf("query is : %s", query)

	var MaxInt = 1024000

	req := esapi.DeleteByQueryRequest{
		Index: []string{GetIndexName(indexName)},
		Size: &MaxInt,
		Body: strings.NewReader(query),
	}
	res, err := req.Do(context.Background(), s.esClient)
	DebugESResponse("DeleteByQueryRequest:" + GetIndexName(indexName), res, err)

	defer res.Body.Close()
	return err
}

func (s *elasticStore) GetLastModifyIndex(k string) (uint64, error) {
	log.Debugf("GetLastModifyIndex called k: %s", k)

	// Extract indice name and deploymentId by parsing the key
	indexName, deploymentId := ExtractIndexNameAndDeploymentId(k)
	log.Debugf("indexName is: %s, deploymentId is: %s", indexName, deploymentId)

	return s.InternalGetLastModifyIndex(GetIndexName(indexName), deploymentId)

}

func ParseInt64StringToUint64(value string) (uint64, error) {
	valueInt, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, errors.Wrapf(err, "Not able to parse string: %s to int64, error was: %+v", value, err)
	}
	result := uint64(valueInt)
	return result, nil
}

func (s *elasticStore) InternalGetLastModifyIndex(indexName string, deploymentId string) (uint64, error) {

	// The last_index is query by using ES aggregation query ~= MAX(iid) HAVING deploymentId AND clusterId
	query := GetLastModifiedIndexQuery(s.clusterId, deploymentId)
	log.Debugf("GetLastModifiedIndexQuery is : %s", query)

	res, err := s.esClient.Search(
		s.esClient.Search.WithContext(context.Background()),
		s.esClient.Search.WithIndex(indexName),
		s.esClient.Search.WithSize(0),
		s.esClient.Search.WithBody(strings.NewReader(query)),
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

	var r lastIndexResponse
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		log.Printf("Error parsing the response body: %s", err)
		return 0, err
	}

	hits := r.hits.total
	var last_index uint64 = 0
	if (hits > 0) {
		last_index, err = ParseInt64StringToUint64(r.aggregations.logs_or_events.last_index.value)
		if err != nil {
			return last_index, err
		}
	}

	// Print the response status, number of results, and request duration.
	log.Debugf(
		"[%s] %d hits; last_index: %d",
		res.Status(),
		hits,
		last_index,
	)

	return last_index, nil
}

func (s *elasticStore) List(ctx context.Context, k string, waitIndex uint64, timeout time.Duration) ([]store.KeyValueOut, uint64, error) {
	log.Debugf("List called k: %s, waitIndex: %d, timeout: %v" , k, waitIndex, timeout)
	if err := utils.CheckKey(k); err != nil {
		return nil, 0, err
	}

	// Extract indice name by parsing the key
	indexName, deploymentId := ExtractIndexNameAndDeploymentId(k)
	log.Debugf("indexName is: %s, deploymentId", indexName, deploymentId)

	query := GetListQuery(s.clusterId, deploymentId, waitIndex, 0)

	now := time.Now()
	end := now.Add(timeout - esRefreshTimeout)
	log.Debugf("Now is : %v, date after timeout will be %v (ES timeout duration will be %v)", now, end, timeout - esRefreshTimeout)
	var values = make([]store.KeyValueOut, 0)
	var lastIndex = waitIndex
	var hits = 0
	var err error
	for {
		// first just query to know if they is something to fetch, we just want the max iid (so order desc, size 1)
		hits, values, lastIndex, err = s.DoQueryEs(GetIndexName(indexName), query, waitIndex, 1, "desc");
		if err != nil {
			return values, waitIndex, errors.Wrapf(err, "Failed to request ES logs or events, error was: %+v", err)
		}
		now := time.Now()
		if hits > 0 || now.After(end) {
			break
		}
		log.Debugf("hits is %d and timeout not reached, sleeping %v ...", hits, esQueryPeriod)
		time.Sleep(esQueryPeriod)
	}
	if (hits > 0) {
		// we do have something to retrieve, we will just wait esRefreshTimeout to let any document that has just been stored to be indexed
		// then we just retrieve this 'time window' (between waitIndex and lastIndex)
		query := GetListQuery(s.clusterId, deploymentId, waitIndex, lastIndex)
		// force refresh for this index
		RefreshIndex(s.esClient, GetIndexName(indexName));
		time.Sleep(esRefreshTimeout)
		oldHits := hits
		hits, values, lastIndex, err = s.DoQueryEs(GetIndexName(indexName), query, waitIndex, 10000, "asc");
		if err != nil {
			return values, waitIndex, errors.Wrapf(err, "Failed to request ES logs or events (after waiting for refresh), error was: %+v", err)
		}
		if hits > oldHits {
			log.Printf("%d > %d so sleeping %v to wait for ES refresh was usefull (index %s), %d documents has been fetched", hits, oldHits, esRefreshTimeout, GetIndexName(indexName), len(values))
		}
	}
	log.Debugf("List called result k: %s, waitIndex: %d, timeout: %v, LastIndex: %d, len(values): %d" , k, waitIndex, timeout, lastIndex, len(values))
	return values, lastIndex, err
}

func GetLastModifiedIndexQuery(clusterId string, deploymentId string) string {
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

// the max uint is 9223372036854775807 (19 cars), this is the maximum nano for a time (2262-04-12 00:47:16.854775807 +0100 CET)
// since we use nanotimestamp as ID for events and logs, and since we store this ID as string in ES index, we must ensure that
// the string will be comparable.
func GetSortableStringFromUint64(nanoTimestamp uint64) string {
	nanoTimestampStr := strconv.FormatUint(nanoTimestamp, 10)
	if len(nanoTimestampStr) < 19 {
		nanoTimestampStr = strings.Repeat("0", 19 - len(nanoTimestampStr)) + nanoTimestampStr
	}
	return nanoTimestampStr
}

func GetListQuery(clusterId string, deploymentId string, waitIndex uint64, maxIndex uint64) string {

	var rangeQuery, query string
	if maxIndex > 0 {
		rangeQuery = `
            {
               "range":{
                  "iid":{
                     "gt": "` + GetSortableStringFromUint64(waitIndex) + `",
					 "lte": "` + GetSortableStringFromUint64(maxIndex) + `"
                  }
               }
            }`
	} else {
		rangeQuery = `
            {
               "range":{
                  "iid":{
                     "gt": "` + GetSortableStringFromUint64(waitIndex) + `"
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

func (s *elasticStore) DoQueryEs(index string, query string, waitIndex uint64, size int, order string) (int, []store.KeyValueOut, uint64, error) {
	log.Debugf("Search ES %s using query: %s", index, query)

	values := make([]store.KeyValueOut, 0)

	res, err := s.esClient.Search(
		s.esClient.Search.WithContext(context.Background()),
		s.esClient.Search.WithIndex(index),
		s.esClient.Search.WithSize(size),
		s.esClient.Search.WithBody(strings.NewReader(query)),
		s.esClient.Search.WithSort("iid:" + order),
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
		iid := source["iid"]
		iid_int64, err := ParseInt64StringToUint64(iid.(string))
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
				lastIndex = iid_int64
				// append value to result
				values = append(values, store.KeyValueOut{
					Key:             id,
					LastModifyIndex: iid_int64,
					Value:           source,
					RawValue:        jsonString,
				})
			}
		}
	}

	log.Debugf("DoQueryEs called result waitIndex: %d, LastIndex: %d, len(values): %d", waitIndex, lastIndex, len(values))
	return hits, values, lastIndex, nil
}

