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

package server

import (
	"github.com/ystia/yorc/v3/config"
	"github.com/ystia/yorc/v3/registry"
	"github.com/ystia/yorc/v3/vault"
)

func buildVaultClient(cfg config.Configuration) (vault.Client, error) {
	vaultType := cfg.Vault.GetString("type")
	if vaultType == "" {
		return nil, nil
	}
	cb, err := registry.GetRegistry().GetVaultClientBuilder(vaultType)
	if err != nil {
		return nil, err
	}
	return cb.BuildClient(cfg)
}
