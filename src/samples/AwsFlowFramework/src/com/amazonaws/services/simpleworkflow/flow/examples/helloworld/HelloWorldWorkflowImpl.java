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

import com.amazonaws.services.simpleworkflow.flow.annotations.Asynchronous;
import com.amazonaws.services.simpleworkflow.flow.core.Promise;
import com.amazonaws.services.simpleworkflow.flow.core.Settable;

/**
 * Implementation of the hello world workflow
 */
public class HelloWorldWorkflowImpl implements HelloWorldWorkflow{

    HelloWorldActivitiesClient client = new HelloWorldActivitiesClientImpl();

    @Override
    public Promise<HelloWorldResult> helloWorld(final String name)
    {
        final Promise<String> result = client.printHello(name);
        return makeResult(name, result);
    }

    @Asynchronous
    private Promise<HelloWorldResult> makeResult(String name, Promise<String> printResult)
    {
        HelloWorldResult helloWorldResult = new HelloWorldResult();
        helloWorldResult.setResult(printResult.get());
        helloWorldResult.setUser(name);

        Settable<HelloWorldResult> result = new Settable<HelloWorldResult>();
        result.set(helloWorldResult);
        return result;
    }

}