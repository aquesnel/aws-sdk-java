/*
 * Copyright 2012-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazonaws.services.simpleworkflow.flow.examples.helloworld;

import com.amazonaws.services.simpleworkflow.AmazonSimpleWorkflow;
import com.amazonaws.services.simpleworkflow.flow.examples.common.ConfigHelper;
import com.amazonaws.services.simpleworkflow.model.DescribeWorkflowExecutionRequest;
import com.amazonaws.services.simpleworkflow.model.GetWorkflowExecutionHistoryRequest;
import com.amazonaws.services.simpleworkflow.model.History;
import com.amazonaws.services.simpleworkflow.model.HistoryEvent;
import com.amazonaws.services.simpleworkflow.model.WorkflowExecution;
import com.amazonaws.services.simpleworkflow.model.WorkflowExecutionCompletedEventAttributes;
import com.amazonaws.services.simpleworkflow.model.WorkflowExecutionInfo;

public class WorkflowExecutionStarter {

    private static final String COMPLETED = "COMPLETED";
    private static final String CLOSED = "CLOSED";

    private static final int MAX_RETRIES = 1000;
    private static final int BASE_SINGLE_RETRY_WAIT_MILLISECONDS = 1000;
    private static final int MAX_SINGLE_RETRY_WAIT_MILLISECONDS = 5 * 60 * 1000;
    
    private static ConfigHelper configHelper;
    private static AmazonSimpleWorkflow swfService;

    public static void main(String[] args) throws Exception {
        configHelper = ConfigHelper.createConfig();
        swfService = configHelper.createSWFClient();

        HelloWorldWorkflowClientExternalFactory clientFactory = 
                new HelloWorldWorkflowClientExternalFactoryImpl(
                        swfService, configHelper.getDomain());
        HelloWorldWorkflowClientExternal workflow = clientFactory.getClient();
        
        // Start Workflow Execution
        workflow.helloWorld("User");
        
        // WorkflowExecution is available after workflow creation 
        WorkflowExecution workflowExecution = workflow.getWorkflowExecution();
        System.out.println("Started helloWorld workflow with workflowId=\""
                + workflowExecution.getWorkflowId()
                + "\" and runId=\"" + workflowExecution.getRunId() + "\"");

        boolean workflowComplete = pollWorkflowStatusUntilComplete(workflowExecution);

        if (workflowComplete)
        {
            String result = getWorkflowResult(workflowExecution);
            HelloWorldResult helloWorldResult =
                    workflow.getDataConverter().fromData(result, HelloWorldResult.class);
            System.out.println("Workflow result: " + helloWorldResult);
        }
        else
        {
            System.out.println("There is no result because the workflow failed.");
        }
    }

    private static String getWorkflowResult(WorkflowExecution workflowExecution)
    {
        String nextPageToken = null;

        GetWorkflowExecutionHistoryRequest historyRequest =
                new GetWorkflowExecutionHistoryRequest();
        historyRequest.setDomain(configHelper.getDomain());
        historyRequest.setExecution(workflowExecution);
        historyRequest.setReverseOrder(true);

        do
        {
            historyRequest.setNextPageToken(nextPageToken);
            History workflowExecutionHistory =
                    swfService.getWorkflowExecutionHistory(historyRequest);
            nextPageToken = workflowExecutionHistory.getNextPageToken();

            for (HistoryEvent event : workflowExecutionHistory.getEvents())
            {
                WorkflowExecutionCompletedEventAttributes workflowCompleteAttributes =
                        event.getWorkflowExecutionCompletedEventAttributes();
                if (null != workflowCompleteAttributes)
                {
                    return workflowCompleteAttributes.getResult();
                }
            }
        } while (null != nextPageToken);

        throw new IllegalStateException("Workflow complete event not found.");
    }

    /**
     * Polls for workflow until workflow is closed or until all retries are exhausted
     * @param id the ID of the workflow to poll
     * @return boolean status indicating whether the workflow completed successfully or not
     * @throws InterruptedException if an exception occurs during thread sleep
     */
    private static boolean pollWorkflowStatusUntilComplete(final WorkflowExecution execution)
            throws InterruptedException
    {
        DescribeWorkflowExecutionRequest request = new DescribeWorkflowExecutionRequest();
        request.setExecution(execution);
        request.setDomain(configHelper.getDomain());

        int requestCount = 0;
        boolean success = false;
        // Default to a non-passing status
        String status = "unknown";

        while (requestCount <= MAX_RETRIES)
        {
            requestCount++;
            System.out.println("Getting workflow status attempt " + Integer.toString(requestCount)
                    + " of " + Integer.toString(MAX_RETRIES));
            WorkflowExecutionInfo info =
                    swfService.describeWorkflowExecution(request).getExecutionInfo();
            System.out.println("Workflow execution status is " + info.getExecutionStatus());

            if (CLOSED.equals(info.getExecutionStatus()))
            {
                status = info.getCloseStatus();
                break;
            }
            else
            {
                // Exponential backoff algorithm
                long sleepTime = Math.min(
                        MAX_SINGLE_RETRY_WAIT_MILLISECONDS,
                        BASE_SINGLE_RETRY_WAIT_MILLISECONDS
                                * Math.round(Math.pow(2, requestCount - 1)));

                System.out.println("Waiting " + Long.toString(sleepTime)
                        + " milliseconds before retrying.");
                Thread.sleep(sleepTime);
            }
        }

        System.out.println("Workflow status: " + status);
        if (COMPLETED.equals(status))
        {
            success = true;
        }
        return success;
    }
}
