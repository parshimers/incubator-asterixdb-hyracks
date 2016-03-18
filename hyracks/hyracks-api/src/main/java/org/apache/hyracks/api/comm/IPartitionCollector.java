/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hyracks.api.comm;

import java.util.Collection;

import org.apache.hyracks.api.dataflow.ConnectorDescriptorId;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.partitions.PartitionId;

public interface IPartitionCollector {
    public JobId getJobId();

    public ConnectorDescriptorId getConnectorId();

    public int getReceiverIndex();

    public void open() throws HyracksException;

    public void addPartitions(Collection<PartitionChannel> partitions) throws HyracksException;

    public IFrameReader getReader() throws HyracksException;

    public void close() throws HyracksException;

    public Collection<PartitionId> getRequiredPartitionIds() throws HyracksException;

    public void abort();
}
