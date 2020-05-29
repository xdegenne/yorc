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
)

type elasticStore struct {
	codec encoding.Codec
	esClient *elasticsearch6.Client
}

// NewStore returns a new Consul store
func NewStore() store.Store {
	esClient, _ := elasticsearch6.NewDefaultClient()
	log.Printf("Here are the ES cluster info")
	log.Println(esClient.Info());
	return &elasticStore{encoding.JSON, esClient}
}

func (c *elasticStore) Set(ctx context.Context, k string, v interface{}) error {
	log.Printf("About to Set data into ES, k: %s", k)
	if err := utils.CheckKeyAndValue(k, v); err != nil {
		return err
	}

	data, err := c.codec.Marshal(v)
	if err != nil {
		return errors.Wrapf(err, "failed to marshal value %+v due to error:%+v", v, err)
	}
	log.Printf("About to Set data into ES, data: %s", data)

	return consulutil.StoreConsulKey(k, data)
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
	log.Printf("Get called")
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
	log.Printf("Delete called k: %s", k)
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
	log.Printf("List called k: %s", k)
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
