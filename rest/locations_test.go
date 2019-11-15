// Copyright 2018 Bull S.A.S. Atos Technologies - Bull, Rue Jean Jaures, B.P.68, 78340, Les Clayes-sous-Bois, France.
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

package rest

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/hashicorp/consul/api"
	"github.com/hashicorp/consul/testutil"
	"github.com/stretchr/testify/require"
	"github.com/ystia/yorc/v4/helper/consulutil"
	"github.com/ystia/yorc/v4/log"
)

func testLocationsHandlers(t *testing.T, client *api.Client, srv *testutil.TestServer) {
	t.Run("testListLocations", func(t *testing.T) {
		testListLocations(t, client, srv)
	})
}

func testListLocations(t *testing.T, client *api.Client, srv *testutil.TestServer) {
	log.SetDebug(true)

	// Populate KV base with some location definitions
	srv.PopulateKV(t, map[string][]byte{
		consulutil.LocationsPrefix + "/loc1": []byte("{\"Name\":\"loc1\",\"Type\":\"t1\",\"Properties\":{\"p11\":\"v11\",\"p21\":\"v21\"}}"),
		consulutil.LocationsPrefix + "/loc2": []byte("{\"Name\":\"loc2\",\"Type\":\"t2\",\"Properties\":{\"p12\":\"v12\",\"p22\":\"v22\"}}"),
		consulutil.LocationsPrefix + "/loc3": []byte("{\"Name\":\"loc3\",\"Type\":\"t3\",\"Properties\":{\"p13\":\"v13\",\"p23\":\"v23\"}}"),
	})

	// Make a request
	req := httptest.NewRequest("GET", "/locations", nil)
	req.Header.Add("Accept", "application/json")
	resp := newTestHTTPRouter(client, req)
	body, err := ioutil.ReadAll(resp.Body)

	require.Nil(t, err, "unexpected error reading body response")
	require.NotNil(t, resp, "unexpected nil response")
	require.Equal(t, http.StatusOK, resp.StatusCode, "unexpected status code %d instead of %d", resp.StatusCode, http.StatusOK)
	require.Equal(t, "application/json", resp.Header.Get("Content-Type"))

	var collection LocationCollection
	err = json.Unmarshal(body, &collection)
	require.Nil(t, err, "unexpected error unmarshalling json body")
	require.NotNil(t, collection, "unexpected nil locations collection")
	require.Equal(t, 3, len(collection.Locations))
	require.Equal(t, "/locations/loc1", collection.Locations[0].Href)
	require.Equal(t, "location", collection.Locations[0].Rel)
	require.Equal(t, "/locations/loc2", collection.Locations[1].Href)
	require.Equal(t, "location", collection.Locations[1].Rel)
	require.Equal(t, "/locations/loc3", collection.Locations[2].Href)
	require.Equal(t, "location", collection.Locations[2].Rel)

	// Clean-up KV
	client.KV().DeleteTree(consulutil.LocationsPrefix+"/loc1", nil)
	client.KV().DeleteTree(consulutil.LocationsPrefix+"/loc2", nil)
	client.KV().DeleteTree(consulutil.LocationsPrefix+"/loc3", nil)
}
