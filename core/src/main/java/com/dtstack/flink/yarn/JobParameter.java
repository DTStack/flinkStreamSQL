/*
 * Copyright (C) 2018 The Sylph Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtstack.flink.yarn;

import java.util.Objects;
import java.util.Properties;

public class JobParameter
{
    private int parallelism = 1;
    private String queue = "default";
    private int taskManagerMemoryMb = 1024;
    private int taskManagerCount = 1;
    private int taskManagerSlots = 1;
    private int jobManagerMemoryMb = 1024;

    public JobParameter() {}

    public JobParameter(Properties confProperties) {
        this.parallelism = confProperties.getProperty("parallelism")==null?parallelism:Integer.parseInt(confProperties.getProperty("parallelism"));
        this.queue = confProperties.getProperty("queue")==null?queue:confProperties.getProperty("queue");
        this.taskManagerMemoryMb = confProperties.getProperty("taskManagerMemoryMb")==null?taskManagerMemoryMb:Integer.parseInt(confProperties.getProperty("taskManagerMemoryMb"));
        this.taskManagerCount = confProperties.getProperty("taskManagerCount")==null?taskManagerCount:Integer.parseInt(confProperties.getProperty("taskManagerCount"));
        this.taskManagerSlots = confProperties.getProperty("taskManagerSlots")==null?taskManagerSlots:Integer.parseInt(confProperties.getProperty("taskManagerSlots"));
        this.jobManagerMemoryMb = confProperties.getProperty("jobManagerMemoryMb")==null?jobManagerMemoryMb:Integer.parseInt(confProperties.getProperty("jobManagerMemoryMb"));
    }

    public JobParameter(int parallelism, String queue, int taskManagerMemoryMb, int taskManagerCount, int taskManagerSlots, int jobManagerMemoryMb) {
        this.parallelism = parallelism;
        this.queue = queue;
        this.taskManagerMemoryMb = taskManagerMemoryMb;
        this.taskManagerCount = taskManagerCount;
        this.taskManagerSlots = taskManagerSlots;
        this.jobManagerMemoryMb = jobManagerMemoryMb;
    }

    public void setQueue(String queue)
    {
        this.queue = queue;
    }

    public void setTaskManagerCount(int taskManagerCount)
    {
        this.taskManagerCount = taskManagerCount;
    }

    public void setTaskManagerMemoryMb(int taskManagerMemoryMb)
    {
        this.taskManagerMemoryMb = taskManagerMemoryMb;
    }

    public void setTaskManagerSlots(int taskManagerSlots)
    {
        this.taskManagerSlots = taskManagerSlots;
    }

    public void setJobManagerMemoryMb(int jobManagerMemoryMb)
    {
        this.jobManagerMemoryMb = jobManagerMemoryMb;
    }

    public void setParallelism(int parallelism)
    {
        this.parallelism = parallelism;
    }

    public int getParallelism()
    {
        return parallelism;
    }

    public String getQueue()
    {
        return queue;
    }

    public int getJobManagerMemoryMb()
    {
        return jobManagerMemoryMb;
    }

    public int getTaskManagerSlots()
    {
        return taskManagerSlots;
    }

    public int getTaskManagerCount()
    {
        return taskManagerCount;
    }

    public int getTaskManagerMemoryMb()
    {
        return taskManagerMemoryMb;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        JobParameter jobParameter = (JobParameter) o;
        return Objects.equals(this.queue, jobParameter.queue) &&
                Objects.equals(this.taskManagerCount, jobParameter.taskManagerCount) &&
                Objects.equals(this.taskManagerMemoryMb, jobParameter.taskManagerMemoryMb);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(queue, taskManagerMemoryMb, taskManagerCount);
    }
}
