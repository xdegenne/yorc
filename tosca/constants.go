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

package tosca

// RunnableInterfaceName is the fully qualified name of the Runnable interface
const RunnableInterfaceName = "tosca.interfaces.node.lifecycle.runnable"

// RunnableSubmitOperationName is the fully qualified name of the Submit operation
const RunnableSubmitOperationName = RunnableInterfaceName + ".submit"

// RunnableRunOperationName is the fully qualified name of the Run operation
const RunnableRunOperationName = RunnableInterfaceName + ".run"

// RunnableCancelOperationName is the fully qualified name of the Cancel operation
const RunnableCancelOperationName = RunnableInterfaceName + ".cancel"
