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

// Return the query that is used to create indexes for event and log storage.
// We only index the needed fields to optimize ES indexing performance (no dynamic mapping).
func buildInitStorageIndexQuery() string {
	query := `
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
	return query
}

// This ES aggregation query is built using clusterId and eventually deploymentId.
func buildLastModifiedIndexQuery(clusterID string, deploymentID string) string {
	var query string
	if len(deploymentID) == 0 {
		query = `
{
    "aggs" : {
        "logs_or_events" : {
            "filter" : {
				"term": { "clusterId": "` + clusterID + `" }
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
                        { "term": { "deploymentId": "` + deploymentID + `" } },
                        { "term": { "clusterId": "` + clusterID + `" } }
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

// This ES range query is built using 'waitIndex' and eventually 'maxIndex' and filtered using 'clusterId' and eventually 'deploymentId'.
func getListQuery(clusterID string, deploymentID string, waitIndex uint64, maxIndex uint64) string {

	var rangeQuery, query string
	if maxIndex > 0 {
		rangeQuery = `
            {
               "range":{
                  "iid":{
                     "gt": "` + getSortableStringFromUint64(waitIndex) + `",
					 "lte": "` + getSortableStringFromUint64(maxIndex) + `"
                  }
               }
            }`
	} else {
		rangeQuery = `
            {
               "range":{
                  "iid":{
                     "gt": "` + getSortableStringFromUint64(waitIndex) + `"
                  }
               }
            }`
	}
	if len(deploymentID) == 0 {
		query = `
{
   "query":{
      "bool":{
         "must":[
            {
               "term":{
                  "clusterId":"` + clusterID + `"
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
                  "clusterId":"` + clusterID + `"
               }
            },
            {
               "term":{
                  "deploymentId":"` + deploymentID + `"
               }
            },` + rangeQuery + `
         ]
      }
   }
}`
	}
	return query
}