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
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"path"
	"strings"

	"github.com/julienschmidt/httprouter"

	"github.com/ystia/yorc/v3/deployments"
	"github.com/ystia/yorc/v3/log"
	"github.com/ystia/yorc/v3/prov/operations"
	"github.com/ystia/yorc/v3/tasks"
)

func (s *Server) newCustomCommandHandler(w http.ResponseWriter, r *http.Request) {
	var params httprouter.Params
	ctx := r.Context()
	params = ctx.Value(paramsLookupKey).(httprouter.Params)
	id := params.ByName("id")

	dExits, err := deployments.DoesDeploymentExists(s.consulClient.KV(), id)
	if err != nil {
		log.Panicf("%v", err)
	}
	if !dExits {
		writeError(w, r, errNotFound)
		return
	}

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Panic(err)
	}

	var ccRequest CustomCommandRequest
	if err = json.Unmarshal(body, &ccRequest); err != nil {
		log.Panic(err)
	}
	ccRequest.InterfaceName = strings.ToLower(ccRequest.InterfaceName)

	inputsName, err := s.getInputNameFromCustom(id, ccRequest.NodeName, ccRequest.InterfaceName, ccRequest.CustomCommandName)
	if err != nil {
		log.Panic(err)
	}

	data := make(map[string]string)

	// For now custom commands are for all instances
	instances, err := deployments.GetNodeInstancesIds(s.consulClient.KV(), id, ccRequest.NodeName)
	data[path.Join("nodes", ccRequest.NodeName)] = strings.Join(instances, ",")
	data["commandName"] = ccRequest.CustomCommandName
	data["interfaceName"] = ccRequest.InterfaceName

	for _, name := range inputsName {
		if err != nil {
			log.Panic(err)
		}
		data[path.Join("inputs", name)] = ccRequest.Inputs[name].String()
	}

	taskID, err := s.tasksCollector.RegisterTaskWithData(id, tasks.TaskTypeCustomCommand, data)
	if err != nil {
		if ok, _ := tasks.IsAnotherLivingTaskAlreadyExistsError(err); ok {
			writeError(w, r, newBadRequestError(err))
			return
		}
		log.Panic(err)
	}

	w.Header().Set("Location", fmt.Sprintf("/deployments/%s/tasks/%s", id, taskID))
	w.WriteHeader(http.StatusAccepted)
}

func (s *Server) getInputNameFromCustom(deploymentID, nodeName, interfaceName, customCName string) ([]string, error) {
	kv := s.consulClient.KV()
	op, err := operations.GetOperation(context.Background(), kv, deploymentID, nodeName, interfaceName+"."+customCName, "", "")
	if err != nil {
		return nil, err
	}
	inputs, err := deployments.GetOperationInputs(kv, deploymentID, op.ImplementedInNodeTemplate, op.ImplementedInType, op.Name)
	if err != nil {
		log.Panic(err)
	}

	result := inputs[:0]
	for _, inputName := range inputs {
		isPropDef, err := deployments.IsOperationInputAPropertyDefinition(kv, deploymentID, op.ImplementedInNodeTemplate, op.ImplementedInType, op.Name, inputName)
		if err != nil {
			return nil, err
		}
		if isPropDef {
			result = append(result, inputName)
		}
	}
	return result, nil
}
