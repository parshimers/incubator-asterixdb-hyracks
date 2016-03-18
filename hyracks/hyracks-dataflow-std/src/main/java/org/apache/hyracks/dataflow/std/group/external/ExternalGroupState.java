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
package org.apache.hyracks.dataflow.std.group.external;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.dataflow.common.io.RunFileWriter;
import org.apache.hyracks.dataflow.std.base.AbstractStateObject;
import org.apache.hyracks.dataflow.std.group.ISpillableTable;

public class ExternalGroupState extends AbstractStateObject {

    private RunFileWriter[] runs;
    private ISpillableTable gTable;
    private int[] spilledNumTuples;

    ExternalGroupState(JobId jobId, Object id) {
        super(jobId, id);
    }

    public RunFileWriter[] getRuns() {
        return runs;
    }

    public void setRuns(RunFileWriter[] runs) {
        this.runs = runs;
    }

    public ISpillableTable getSpillableTable() {
        return gTable;
    }

    public void setSpillableTable(ISpillableTable gTable) {
        this.gTable = gTable;
    }

    @Override
    public void toBytes(DataOutput out) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void fromBytes(DataInput in) throws IOException {
        throw new UnsupportedOperationException();
    }

    public void setSpilledNumTuples(int[] spilledNumTuples) {
        this.spilledNumTuples = spilledNumTuples;
    }

    public int[] getSpilledNumTuples() {
        return spilledNumTuples;
    }
}
