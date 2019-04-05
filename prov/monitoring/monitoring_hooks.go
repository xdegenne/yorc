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

package monitoring

import (
	"context"
	"github.com/hashicorp/consul/api"
	"github.com/pkg/errors"
	"github.com/ystia/yorc/v3/config"
	"github.com/ystia/yorc/v3/deployments"
	"github.com/ystia/yorc/v3/events"
	"github.com/ystia/yorc/v3/log"
	"github.com/ystia/yorc/v3/tasks"
	"github.com/ystia/yorc/v3/tasks/workflow"
	"github.com/ystia/yorc/v3/tasks/workflow/builder"
	"github.com/ystia/yorc/v3/tosca"
	"strconv"
	"strings"
	"time"
)

func init() {
	workflow.RegisterPreActivityHook(removeMonitoringHook)
	workflow.RegisterPostActivityHook(addMonitoringHook)
}

const (
	httpMonitoring = "yorc.policies.monitoring.HTTPMonitoring"
	tcpMonitoring  = "yorc.policies.monitoring.TCPMonitoring"
	baseMonitoring = "yorc.policies.Monitoring"
)

func addMonitoringHook(ctx context.Context, cfg config.Configuration, taskID, deploymentID, target string, activity builder.Activity) {
	// Monitoring check are added after (post-hook):
	// - Delegate activity and install operation
	// - SetState activity and node state "Started"

	switch {
	case activity.Type() == builder.ActivityTypeDelegate && strings.ToLower(activity.Value()) == "install",
		activity.Type() == builder.ActivityTypeSetState && activity.Value() == tosca.NodeStateStarted.String():

		// Check if monitoring is required
		isMonitorReq, policyName, err := checkExistingMonitoringPolicy(defaultMonManager.cc.KV(), deploymentID, target)
		if err != nil {
			events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelWARN, deploymentID).
				Registerf("Failed to check if monitoring is required for node name:%q due to: %v", target, err)
			return
		}
		if !isMonitorReq {
			return
		}

		err = addMonitoringPolicyForTarget(defaultMonManager.cc.KV(), taskID, deploymentID, target, policyName)
		if err != nil {
			events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelWARN, deploymentID).
				Registerf("Failed to add monitoring policy for node name:%q due to: %v", target, err)
		}
	}
}

func removeMonitoringHook(ctx context.Context, cfg config.Configuration, taskID, deploymentID, target string, activity builder.Activity) {
	// Monitoring check are removed before (pre-hook):
	// - Delegate activity and uninstall operation
	// - SetState activity and node state "Deleted"
	switch {
	case activity.Type() == builder.ActivityTypeDelegate && strings.ToLower(activity.Value()) == "uninstall",
		activity.Type() == builder.ActivityTypeSetState && activity.Value() == tosca.NodeStateDeleted.String():

		// Check if monitoring has been required
		isMonitorReq, _, err := checkExistingMonitoringPolicy(defaultMonManager.cc.KV(), deploymentID, target)
		if err != nil {
			events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelWARN, deploymentID).
				Registerf("Failed to check if monitoring is required for node name:%q due to: %v", target, err)
			return
		}
		if !isMonitorReq {
			return
		}

		instances, err := tasks.GetInstances(defaultMonManager.cc.KV(), taskID, deploymentID, target)
		if err != nil {
			events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelWARN, deploymentID).
				Registerf("Failed to retrieve instances for node name:%q due to: %v", target, err)
			return
		}

		for _, instance := range instances {
			if err := defaultMonManager.flagCheckForRemoval(deploymentID, target, instance); err != nil {
				events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelWARN, deploymentID).
					Registerf("Failed to unregister check for node name:%q due to: %v", target, err)
				return
			}
		}
	}
}

func checkExistingMonitoringPolicy(kv *api.KV, deploymentID, target string) (bool, string, error) {
	policies, err := deployments.GetPoliciesForTypeAndNode(kv, deploymentID, baseMonitoring, target)
	if err != nil {
		return false, "", err
	}
	if len(policies) == 0 {
		return false, "", nil
	}
	if len(policies) > 1 {
		return false, "", errors.Errorf("Found more than one monitoring policy to apply to node name:%q. No monitoring policy will be applied", target)
	}

	return true, policies[0], nil
}

func addMonitoringPolicyForTarget(kv *api.KV, taskID, deploymentID, target, policyName string) error {
	log.Debugf("Add monitoring policy:%q for deploymentID:%q, node name:%q", policyName, deploymentID, target)
	policyType, err := deployments.GetPolicyType(kv, deploymentID, policyName)
	if err != nil {
		return err
	}

	// Retrieve time_interval and port
	tiValue, err := deployments.GetPolicyPropertyValue(kv, deploymentID, policyName, "time_interval")
	if err != nil || tiValue == nil || tiValue.RawString() == "" {
		return errors.Errorf("Failed to retrieve time_interval for monitoring policy:%q due to: %v", policyName, err)
	}

	timeInterval, err := time.ParseDuration(tiValue.RawString())
	if err != nil {
		return errors.Errorf("Failed to retrieve time_interval as correct duration for monitoring policy:%q due to: %v", policyName, err)
	}
	portValue, err := deployments.GetPolicyPropertyValue(kv, deploymentID, policyName, "port")
	if err != nil || portValue == nil || portValue.RawString() == "" {
		return errors.Errorf("Failed to retrieve port for monitoring policy:%q due to: %v", policyName, err)
	}
	port, err := strconv.Atoi(portValue.RawString())
	if err != nil {
		return errors.Errorf("Failed to retrieve port as correct integer for monitoring policy:%q due to: %v", policyName, err)
	}
	instances, err := tasks.GetInstances(defaultMonManager.cc.KV(), taskID, deploymentID, target)
	if err != nil {
		return err
	}

	switch policyType {
	case httpMonitoring:
		//TODO
		return nil
	case tcpMonitoring:
		return applyTCPMonitoringPolicy(deploymentID, target, timeInterval, port, instances)
	default:
		return errors.Errorf("Unsupported policy type:%q for policy:%q", policyType, policyName)
	}
}

func applyTCPMonitoringPolicy(deploymentID, target string, timeInterval time.Duration, port int, instances []string) error {
	for _, instance := range instances {
		ipAddress, err := deployments.GetInstanceAttributeValue(defaultMonManager.cc.KV(), deploymentID, target, instance, "ip_address")
		if err != nil {
			return errors.Errorf("Failed to retrieve ip_address for node name:%q due to: %v", target, err)
		}
		if ipAddress == nil || ipAddress.RawString() == "" {
			return errors.Errorf("No attribute ip_address has been found for nodeName:%q, instance:%q with deploymentID:%q", target, instance, deploymentID)
		}

		if err := defaultMonManager.registerTCPCheck(deploymentID, target, instance, ipAddress.RawString(), port, timeInterval); err != nil {
			return errors.Errorf("Failed to register check for node name:%q due to: %v", target, err)
		}
	}
	return nil
}
