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

package slurm

import (
	"context"
	"fmt"
	"github.com/hashicorp/consul/api"
	"github.com/pkg/errors"
	"github.com/ystia/yorc/config"
	"github.com/ystia/yorc/events"
	"github.com/ystia/yorc/helper/sshutil"
	"github.com/ystia/yorc/helper/stringutil"
	"github.com/ystia/yorc/log"
	"github.com/ystia/yorc/prov"
	"github.com/ystia/yorc/prov/scheduling"
	"github.com/ystia/yorc/tasks"
	"github.com/ystia/yorc/tasks/collector"
	"path"
	"strconv"
	"strings"
)

type actionOperator struct {
	client              *sshutil.SSHClient
	consulClient        *api.Client
	action              *prov.Action
	stepName            string
	jobID               string
	taskID              string
	isBatch             bool
	remoteBaseDirectory string
	remoteExecDirectory string
	outputs             []string
}

func (o actionOperator) ExecAction(ctx context.Context, cfg config.Configuration, taskID, deploymentID string, action *prov.Action) error {
	log.Debugf("Execute Action:%+v with taskID:%q, deploymentID:%q", action, taskID, deploymentID)
	var err error
	o.client, err = GetSSHClient(cfg)
	if err != nil {
		return err
	}
	o.consulClient, err = cfg.GetConsulClient()
	if err != nil {
		return err
	}
	o.action = action
	switch action.ActionType {
	case "job-monitoring":
		err = o.monitorJob(ctx, deploymentID)
		if err != nil {
			// action scheduling needs to be unregistered
			errSche := scheduling.UnregisterAction(o.consulClient, o.action.ID)
			if errSche != nil {
				log.Printf("failed to unregister job Monitoring job info with actionID:%q due to error:%+v", o.action.ID, errSche)
			}
			return err
		}
	default:
		return errors.Errorf("Unsupported actionType %q", action.ActionType)
	}
	return nil
}

func (o actionOperator) monitorJob(ctx context.Context, deploymentID string) error {
	var (
		err error
		ok  bool
	)
	// Check jobID
	o.jobID, ok = o.action.Data["jobID"]
	if !ok {
		return errors.Errorf("Missing mandatory information jobID for actionType:%q", o.action.ActionType)
	}
	// Check stepName
	o.stepName, ok = o.action.Data["stepName"]
	if !ok {
		return errors.Errorf("Missing mandatory information stepName for actionType:%q", o.action.ActionType)
	}
	// Check isBatch
	isBatchStr, ok := o.action.Data["isBatch"]
	if !ok {
		return errors.Errorf("Missing mandatory information isBatch for actionType:%q", o.action.ActionType)
	}
	o.isBatch, err = strconv.ParseBool(isBatchStr)
	if err != nil {
		return errors.Errorf("Invalid information isBatch for actionType:%q", o.action.ActionType)
	}
	// Check remoteBaseDirectory
	o.remoteBaseDirectory, ok = o.action.Data["remoteBaseDirectory"]
	if !ok {
		return errors.Errorf("Missing mandatory information remoteBaseDirectory for actionType:%q", o.action.ActionType)
	}
	// Check taskID
	o.taskID, ok = o.action.Data["taskID"]
	if !ok {
		return errors.Errorf("Missing mandatory information taskID for actionType:%q", o.action.ActionType)
	}
	// Check outputs
	outputStr, ok := o.action.Data["outputs"]
	if !ok {
		return errors.Errorf("Missing mandatory information taskID for actionType:%q", o.action.ActionType)
	}
	o.outputs = strings.Split(outputStr, ",")
	if !o.isBatch && len(o.outputs) != 1 {
		return errors.Errorf("Incorrect outputs files nb:%d for interactive job with id:%q. Only one is required.", len(o.outputs), o.jobID)
	}
	// remoteExecDirectory can be empty for interactive jobs
	o.remoteExecDirectory = o.action.Data["remoteExecDirectory"]

	// Fill log optional fields for log registration
	wfName, err := tasks.GetTaskData(o.consulClient.KV(), o.taskID, "workflowName")
	if err != nil {
		return errors.Wrapf(err, "failed to retrieve workflow name for task:%q", o.taskID)
	}
	logOptFields := events.LogOptionalFields{
		events.WorkFlowID:    wfName,
		events.NodeID:        o.action.Data["nodeName"],
		events.OperationName: stringutil.GetLastElement(o.action.Data["operationName"], "."),
		events.InterfaceName: stringutil.GetAllExceptLastElement(o.action.Data["operationName"], "."),
	}
	ctx = events.NewContext(ctx, logOptFields)

	info, err := getJobInfo(o.client, o.jobID, "")
	if err != nil {
		_, done := err.(*noJobFound)
		if done {
			err = o.endJob(ctx, deploymentID)
			if err != nil {
				return err
			}
		}
		return errors.Wrapf(err, "failed to get job info with jobID:%q", o.jobID)
	}

	mess := fmt.Sprintf("Job Name:%s, Job ID:%s, Job State:%s", info.name, info.ID, info.state)
	events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, deploymentID).RegisterAsString(mess)
	o.displayTempOutput(ctx, deploymentID)
	return nil
}

func (o *actionOperator) endJob(ctx context.Context, deploymentID string) error {
	// action scheduling needs to be unregistered
	// job run step is set to done and workflow resumed
	// we assume errors in monitoring doesn't affect job run step status
	defer func() {
		err := scheduling.UnregisterAction(o.consulClient, o.action.ID)
		if err != nil {
			log.Printf("failed to unregister job Monitoring job info with actionID:%q, jobID:%q due to error:%+v", o.action.ID, o.jobID, err)
		}
		err = o.resumeWorkflow()
		if err != nil {
			log.Printf("failed to resume job run workflow with actionID:%q, jobID:%q due to error:%+v", o.action.ID, o.jobID, err)
		}
	}()
	events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, deploymentID).RegisterAsString(fmt.Sprintf("Job with JobID:%s is DONE", o.jobID))
	// If batch job, cleanup needs to be processed after logging output files
	if o.isBatch {
		err := o.endBatchOutput(ctx, deploymentID)
		if err != nil {
			return errors.Wrapf(err, "failed to handle batch outputs with jobID:%q", o.jobID)
		}
		o.cleanUp()
	} else {
		err := o.endInteractiveOutput(ctx, deploymentID)
		if err != nil {
			return errors.Wrapf(err, "failed to handle interactive output with jobID:%q", o.jobID)
		}
	}
	return nil
}

func (o *actionOperator) resumeWorkflow() error {
	// job running step must be set to done and workflow must be resumed
	step := &tasks.TaskStep{Status: tasks.TaskStepStatusDONE.String(), Name: o.stepName}
	err := tasks.UpdateTaskStepStatus(o.consulClient.KV(), o.taskID, step)
	if err != nil {
		return errors.Wrapf(err, "failed to update step status to DONE for taskID:%q, stepName:%q", o.taskID, o.stepName)
	}
	coll := collector.NewCollector(o.consulClient)
	err = coll.ResumeTask(o.taskID)
	if err != nil {
		return errors.Wrapf(err, "failed to resume task with taskID:%q", o.taskID)
	}
	return nil
}

func (o *actionOperator) endBatchOutput(ctx context.Context, deploymentID string) error {
	// Look for outputs with relative path
	relOutputs := make([]string, 0)
	for _, output := range o.outputs {
		if !path.IsAbs(output) {
			relOutputs = append(relOutputs, output)
		} else {
			o.logFile(ctx, deploymentID, output)
		}
	}

	if len(relOutputs) > 0 {
		// Copy the outputs with relative path in <JOB_ID>_outputs directory at root level
		outputDir := fmt.Sprintf("job_" + o.jobID + "_outputs")
		cmd := fmt.Sprintf("mkdir %s", outputDir)
		events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, deploymentID).RegisterAsString(fmt.Sprintf("Run the command: %q", cmd))
		output, err := o.client.RunCommand(cmd)
		if err != nil {
			return errors.Wrap(err, output)
		}
		for _, relOutput := range relOutputs {
			oldPath := path.Join(o.remoteExecDirectory, relOutput)
			newPath := path.Join(outputDir, relOutput)
			// Copy the file in the output dir
			cmd := fmt.Sprintf("cp -f %s %s", oldPath, newPath)
			events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, deploymentID).RegisterAsString(fmt.Sprintf("Run the command: %q", cmd))
			output, err := o.client.RunCommand(cmd)
			if err != nil {
				return errors.Wrap(err, output)
			}
			err = o.logFile(ctx, deploymentID, newPath)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (o *actionOperator) displayTempOutput(ctx context.Context, deploymentID string) {
	for _, output := range o.outputs {
		var tempFile string
		if path.IsAbs(output) {
			tempFile = output
		} else {
			tempFile = path.Join(o.remoteExecDirectory, output)
		}
		o.logFile(ctx, deploymentID, tempFile)
	}
}

func (o *actionOperator) endInteractiveOutput(ctx context.Context, deploymentID string) error {
	// rename the output file and copy it into specific output folder
	newName := fmt.Sprintf("slurm-%s.out", o.jobID)
	outputDir := fmt.Sprintf("job_" + o.jobID + "_outputs")
	cmd := fmt.Sprintf("mkdir %s", outputDir)
	events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, deploymentID).RegisterAsString(fmt.Sprintf("Run the command: %q", cmd))
	output, err := o.client.RunCommand(cmd)
	if err != nil {
		return errors.Wrap(err, output)
	}

	newPath := path.Join(outputDir, newName)
	// Move the file in the output dir
	cmd = fmt.Sprintf("mv %s %s", o.outputs[0], newPath)
	events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, deploymentID).RegisterAsString(fmt.Sprintf("Run the command: %q", cmd))
	output, err = o.client.RunCommand(cmd)
	if err != nil {
		return errors.Wrap(err, output)
	}
	return o.logFile(ctx, deploymentID, newPath)
}

func (o *actionOperator) cleanUp() {
	log.Debugf("Cleanup the operation remote base directory")
	cmd := fmt.Sprintf("rm -rf %s", o.remoteBaseDirectory)
	_, err := o.client.RunCommand(cmd)
	if err != nil {
		log.Printf("an error:%+v occurred during cleanup for remote base directory:%q", err, o.remoteBaseDirectory)
	}
}

func (o *actionOperator) logFile(ctx context.Context, deploymentID, filePath string) error {
	var err error
	cmd := fmt.Sprintf("cat %s", filePath)
	events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, deploymentID).RegisterAsString(fmt.Sprintf("Run the command: %q", cmd))
	output, err := o.client.RunCommand(cmd)
	if err != nil {
		log.Debugf("an error:%+v occurred during logging file:%q", err, filePath)
		return errors.Wrapf(err, "failed to log file:%q", filePath)
	}
	events.WithContextOptionalFields(ctx).NewLogEntry(events.LogLevelINFO, deploymentID).RegisterAsString("\n" + output)
	return nil
}