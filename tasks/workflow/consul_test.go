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

package workflow

import (
	"os"
	"path"
	"testing"

	"github.com/hashicorp/consul/api"
	"github.com/stretchr/testify/require"
	"github.com/ystia/yorc/v4/helper/consulutil"
	"github.com/ystia/yorc/v4/testutil"
)

// The aim of this function is to run all package tests with consul server dependency with only one consul server start
func TestRunConsulWorkflowPackageTests(t *testing.T) {
	srv, client, workingDir := testutil.NewTestConsulInstance(t)
	defer func() {
		srv.Stop()
		os.RemoveAll(workingDir)
	}()

	t.Run("groupWorkflow", func(t *testing.T) {
		t.Run("testRunStep", func(t *testing.T) {
			testRunStep(t, srv, client)
		})
		t.Run("testRegisterInlineWorkflow", func(t *testing.T) {
			testRegisterInlineWorkflow(t, srv, client)
		})
		t.Run("testDeleteExecutionTreeSamePrefix", func(t *testing.T) {
			testDeleteExecutionTreeSamePrefix(t, client)
		})
		t.Run("testDeleteTaskExecutionSamePrefix", func(t *testing.T) {
			testDeleteTaskExecutionSamePrefix(t, client)
		})
		t.Run("testDispatcherRun", func(t *testing.T) {
			testDispatcherRun(t, srv, client)
		})
	})

	populateKV(t, srv)
	t.Run("groupWorker", func(t *testing.T) {
		t.Run("TestRunQueryInfraUsage", func(t *testing.T) {
			testRunQueryInfraUsage(t, srv, client)
		})
		t.Run("TestRunPurge", func(t *testing.T) {
			testRunPurge(t, srv, client)
		})
		t.Run("TestRunPurgeFails", func(t *testing.T) {
			testRunPurgeFails(t, srv, client)
		})
	})
}

func createTaskExecutionKVWithKey(t *testing.T, execID, keyName, keyValue string) {
	t.Helper()
	_, err := consulutil.GetKV().Put(&api.KVPair{Key: path.Join(consulutil.ExecutionsTaskPrefix, execID, keyName), Value: []byte(keyValue)}, nil)
	require.NoError(t, err)
}
