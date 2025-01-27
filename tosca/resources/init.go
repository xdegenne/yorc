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

package resources

import (
	"log"

	"github.com/ystia/yorc/v3/registry"
)

func init() {
	reg := registry.GetRegistry()
	resources, err := getToscaResources()
	if err != nil {
		log.Panicf("Failed to load builtin Tosca definition. %v", err)
	}
	for defName, defContent := range resources {
		reg.AddToscaDefinition(defName, registry.BuiltinOrigin, defContent)
	}
}
