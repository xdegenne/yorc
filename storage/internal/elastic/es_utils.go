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
	"bytes"
	"context"
	"encoding/json"
	elasticsearch6 "github.com/elastic/go-elasticsearch/v6"
	"github.com/elastic/go-elasticsearch/v6/esapi"
	"github.com/pkg/errors"
	"github.com/ystia/yorc/v4/log"
	"github.com/ystia/yorc/v4/storage/store"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"time"
)

var pfalse = false

// structs for lastIndexRequest response decoding.
type lastIndexResponse struct {
	hits         hits                  `json:"hits"`
	aggregations logOrEventAggregation `json:"aggregations"`
}
type hits struct {
	total int `json:"total"`
}
type logOrEventAggregation struct {
	logsOrEvents lastIndexAggregation `json:"logs_or_events"`
}
type lastIndexAggregation struct {
	lastIndex uintValue `json:"last_index"`
	docCount int64 `json:"doc_count"`

}
type uintValue struct {
	value float64 `json:"value"`
}

func prepareEsClient(elasticStoreConfig elasticStoreConf) (*elasticsearch6.Client, error) {
	var esConfig elasticsearch6.Config

	esConfig = elasticsearch6.Config{Addresses: elasticStoreConfig.esUrls}

	if len(elasticStoreConfig.caCertPath) > 0 {
		log.Printf("Reading CACert file from %s", elasticStoreConfig.caCertPath)
		cert, err := ioutil.ReadFile(elasticStoreConfig.caCertPath)
		if err != nil {
			return nil, errors.Wrapf(err, "Not able to read Cert file from <%s>", elasticStoreConfig.caCertPath)
		}
		esConfig.CACert = cert
	}
	if log.IsDebug() {
		// In debug mode we add a custom logger
		esConfig.Logger = &debugLogger{}
	}

	log.Printf("Elastic storage will run using this configuration: %+v", elasticStoreConfig)
	log.Printf("\t- Index prefix will be %s", elasticStoreConfig.indicePrefix)
	log.Printf("\t- Will query ES for logs or events every %v and will wait for index refresh during %v",
		elasticStoreConfig.esQueryPeriod, elasticStoreConfig.esRefreshWaitTimeout)
	willRefresh := ""
	if !elasticStoreConfig.esForceRefresh {
		willRefresh = "not "
	}
	log.Printf("\t- Will %srefresh index before waiting for indexation", willRefresh)
	log.Printf("\t- While migrating data, the max bulk request size will be %d documents and will never exceed %d kB",
		elasticStoreConfig.maxBulkCount, elasticStoreConfig.maxBulkSize)
	log.Printf("\t- Will use this ES client configuration: %+v", esConfig)

	esClient, e := elasticsearch6.NewClient(esConfig)
	log.Printf("Here is the ES cluster info")
	log.Println(esClient.Info())
	return esClient, e
}

// Init ES index for logs or events storage: create it if not found.
func initStorageIndex(c *elasticsearch6.Client, elasticStoreConfig elasticStoreConf, storeType string) error {

	indexName := getIndexName(elasticStoreConfig, storeType)
	log.Printf("Checking if index <%s> already exists", indexName)

	// check if the sequences index exists
	req := esapi.IndicesExistsRequest{
		Index:           []string{indexName},
		ExpandWildcards: "none",
		AllowNoIndices:  &pfalse,
	}
	res, err := req.Do(context.Background(), c)
	defer closeResponseBody("IndicesExistsRequest:"+indexName, res)

	if err != nil {
		return err
	}

	if res.StatusCode == 200 {
		log.Printf("Indice %s was found, nothing to do !", indexName)
		return nil
	} else if res.StatusCode == 404 {
		log.Printf("Indice %s was not found, let's create it !", indexName)

		requestBodyData := buildInitStorageIndexQuery()

		// indice doest not exist, let's create it
		req := esapi.IndicesCreateRequest{
			Index: indexName,
			Body:  strings.NewReader(requestBodyData),
		}
		res, err := req.Do(context.Background(), c)
		defer closeResponseBody("IndicesCreateRequest:"+indexName, res)
		return handleESResponseError(res, "IndicesCreateRequest:"+indexName, requestBodyData, err)
	} else {
		return handleESResponseError(res, "IndicesExistsRequest:"+indexName, "", err)
	}
}

// Perform a refresh query on ES cluster for this particular index.
func refreshIndex(c *elasticsearch6.Client, indexName string) {
	req := esapi.IndicesRefreshRequest{
		Index:           []string{indexName},
		ExpandWildcards: "none",
		AllowNoIndices:  &pfalse,
	}
	res, err := req.Do(context.Background(), c)
	err = handleESResponseError(res, "IndicesRefreshRequest:"+indexName, "", err)
	if err != nil {
		log.Println("An error occurred while refreshing index, due to : %+v", err)
	}
	defer closeResponseBody("IndicesRefreshRequest:"+indexName, res)
}

// Query ES for events or logs specifying the expected results 'size' and the sort 'order'.
func doQueryEs(c *elasticsearch6.Client,
	index string,
	query string,
	waitIndex uint64,
	size int,
	order string,
) (hits int, values []store.KeyValueOut, lastIndex uint64, err error) {

	log.Debugf("Search ES %s using query: %s", index, query)
	lastIndex = waitIndex

	res, e := c.Search(
		c.Search.WithContext(context.Background()),
		c.Search.WithIndex(index),
		c.Search.WithSize(size),
		c.Search.WithBody(strings.NewReader(query)),
		// important sort on iid
		c.Search.WithSort("iid:"+order),
	)
	if e != nil {
		err = errors.Wrapf(err, "Failed to perform ES search on index %s, query was: <%s>, error was: %+v", index, query, err)
		return
	}
	defer closeResponseBody("Search:"+index, res)

	err = handleESResponseError(res, "Search:"+index, query, e)
	if err != nil {
		return
	}

	var r map[string]interface{}
	if decodeErr := json.NewDecoder(res.Body).Decode(&r); decodeErr != nil {
		err = errors.Wrapf(decodeErr,
			"Not able to decode ES response while performing ES search on index %s, query was: <%s>, response code was %d (%s)",
			index, query, res.StatusCode, res.Status(),
		)
		return
	}

	hits = int(r["hits"].(map[string]interface{})["total"].(float64))
	duration := int(r["took"].(float64))
	log.Debugf("Search ES request on index %s took %dms, hits=%d, response code was %d (%s)", index, duration, hits, res.StatusCode, res.Status())

	lastIndex = decodeEsQueryResponse(r, &values)

	log.Debugf("doQueryEs called result waitIndex: %d, LastIndex: %d, len(values): %d", waitIndex, lastIndex, len(values))
	return hits, values, lastIndex, nil
}

// Decode the response and define the last index
func decodeEsQueryResponse(r map[string]interface{}, values *[]store.KeyValueOut) (lastIndex uint64) {
	// Print the ID and document source for each hit.
	for _, hit := range r["hits"].(map[string]interface{})["hits"].([]interface{}) {
		id := hit.(map[string]interface{})["_id"].(string)
		source := hit.(map[string]interface{})["_source"].(map[string]interface{})
		iid := source["iid_str"]
		iidInt64, err := parseInt64StringToUint64(iid.(string))
		if err != nil {
			log.Printf("Not able to parse iid_str property %s as uint64, document id: %s, source: %+v, ignoring this document !", iid, id, source)
		} else {
			jsonString, err := json.Marshal(source)
			if err != nil {
				log.Printf("Not able to marshall document source, document id: %s, source: %+v, ignoring this document !", id, source)
			} else {
				// since the result is sorted on iid, we can use the last hit to define lastIndex
				lastIndex = iidInt64
				// append value to result
				*values = append(*values, store.KeyValueOut{
					Key:             id,
					LastModifyIndex: iidInt64,
					Value:           source,
					RawValue:        jsonString,
				})
			}
		}
	}
	return
}

// Send the bulk request to ES and ensure no error is returned.
func sendBulkRequest(c *elasticsearch6.Client, opeCount int, body *[]byte) error {
	log.Printf("About to bulk request containing %d operations (%d bytes)", opeCount, len(*body))
	if log.IsDebug() {
		log.Debugf("About to send bulk request query to ES: %s", string(*body))
	}

	// Prepare ES bulk request
	req := esapi.BulkRequest{
		Body: bytes.NewReader(*body),
	}
	res, err := req.Do(context.Background(), c)
	defer closeResponseBody("BulkRequest", res)

	if err != nil {
		return err
	} else if res.IsError() {
		return handleESResponseError(res, "BulkRequest", string(*body), err)
	} else {
		var rsp map[string]interface{}
		err = json.NewDecoder(res.Body).Decode(&rsp)
		if err != nil {
			// Don't know if the bulk request response contains error so fail by default
			return errors.Errorf(
				"The bulk request succeeded (%s), but not able to decode the response, so not able to determine if bulk operations are correctly handled",
				res.Status(),
			)
		}
		if rsp["errors"].(bool) {
			// The bulk request contains errors
			return errors.Errorf("The bulk request succeeded, but the response contains errors : %+v", rsp)
		}
	}
	log.Printf("Bulk request containing %d operations (%d bytes) has been accepted without errors", opeCount, len(*body))
	return nil
}

// Consider the ES Response and wrap errors when needed
func handleESResponseError(res *esapi.Response, requestDescription string, query string, requestError error) error {
	if requestError != nil {
		return errors.Wrapf(requestError, "Error while sending %s, query was: %s", requestDescription, query)
	}
	if res.IsError() {
		return errors.Errorf(
			"An error was returned by ES while sending %s, status was %s, query was: %s, response: %+v",
			requestDescription, res.Status(), query, res.String())
	}
	return nil
}

// Close response body, if an error occur, just print it
func closeResponseBody(requestDescription string, res *esapi.Response) {
	err := res.Body.Close()
	if err != nil {
		log.Printf("[%s] Was not able to close resource response body, error was: %+v", requestDescription, err)
	}
}

type debugLogger struct{}

// RequestBodyEnabled makes the client pass request body to logger
func (l *debugLogger) RequestBodyEnabled() bool { return true }

// ResponseBodyEnabled makes the client pass response body to logger
func (l *debugLogger) ResponseBodyEnabled() bool { return true }

// LogRoundTrip will use log to debug ES request and response (when debug is activated)
func (l *debugLogger) LogRoundTrip(
	req *http.Request,
	res *http.Response,
	err error,
	start time.Time,
	dur time.Duration,
) error {

	var level string
	switch {
	case err != nil:
		level = "Exception"
	case res != nil && res.StatusCode > 0 && res.StatusCode < 300:
		level = "Success"
	case res != nil && res.StatusCode > 299 && res.StatusCode < 500:
		level = "Warn"
	case res != nil && res.StatusCode > 499:
		level = "Error"
	default:
		level = "Unknown"
	}

	var reqBuffer, resBuffer bytes.Buffer
	if req != nil && req.Body != nil && req.Body != http.NoBody {
		// We explicitly ignore errors here since it's a debug feature
		_, _ = io.Copy(&reqBuffer, req.Body)
	}
	reqStr := reqBuffer.String()
	if res != nil && res.Body != nil && res.Body != http.NoBody {
		// We explicitly ignore errors here since it's a debug feature
		_, _ = io.Copy(&resBuffer, res.Body)
	}
	resStr := resBuffer.String()
	log.Debugf("ES Request [%s][%v][%s][%s][%d][%v] [%+v] : [%+v]",
		level, start, req.Method, req.URL.String(), res.StatusCode, dur, reqStr, resStr)

	return nil
}

func testIidAsLong(c *elasticsearch6.Client, elasticStoreConfig elasticStoreConf, clusterID string) error {
	initStorageIndex(c, elasticStoreConfig, "test_iid")
	indexName := getIndexName(elasticStoreConfig, "test_iid")

	var iid int64 = 1591271005841389857
	iidStr := strconv.FormatInt(iid, 10)

	message := `{"iid": ` + iidStr +`, "iid_str": "` + iidStr +`", "clusterId": "` + clusterID +`"}`

	// Prepare ES request
	req := esapi.IndexRequest{
		Index:        indexName,
		DocumentType: "logs_or_event",
		Body:         strings.NewReader(message),
	}
	res, err := req.Do(context.Background(), c)
	defer closeResponseBody("IndexRequest:"+indexName, res)
	err = handleESResponseError(res, "IndexRequest:"+indexName, message, err)
	if err != nil {
		return err
	}

	query := buildLastModifiedIndexQuery(clusterID, "")

	res, err = c.Search(
		c.Search.WithContext(context.Background()),
		c.Search.WithIndex(indexName),
		c.Search.WithSize(0),
		c.Search.WithBody(strings.NewReader(query)),
	)
	defer closeResponseBody("LastModifiedIndexQuery for ", res)
	err = handleESResponseError(res, "LastModifiedIndexQuery for ", query, err)
	if err != nil {
		return err
	}

	var r lastIndexResponse
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		e := errors.Wrapf(
			err,
			"Not able to parse response body after LastModifiedIndexQuery was sent for key %s, status was %s, query was: %s",
			"myTest", res.Status(), query,
		)
		return e
	}

	var lastIndexF float64
	docCount := r.aggregations.logsOrEvents.docCount
	log.Printf("Here is the value of docCount: %d", docCount)
	if docCount > 0 {
		log.Printf("Here is the value of last_index: %e", r.aggregations.logsOrEvents.lastIndex.value)
		lastIndexF = r.aggregations.logsOrEvents.lastIndex.value
	}
	if (lastIndexF != float64(iid)) {
		return errors.Errorf("float64(iid): %e and lastIndexF: %e don't match ! iid = %d, iidStr = %s", float64(iid), lastIndexF, iid, iidStr)
	}
	log.Printf("float64(iid): %e and lastIndexF: %e do match ! iid = %d, iidStr = %s", float64(iid), lastIndexF, iid, iidStr)
	return nil
}