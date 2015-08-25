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
package org.apache.hyracks.algebricks.runtime.operators.base;

import org.apache.hyracks.api.comm.IFrameFieldAppender;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.IFrameTupleAppender;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.util.ExecutionTimeProfiler;
import org.apache.hyracks.api.util.ExecutionTimeStopWatch;
import org.apache.hyracks.dataflow.common.comm.io.FrameFixedFieldTupleAppender;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;

public abstract class AbstractOneInputOneOutputOneFieldFramePushRuntime extends
        AbstractOneInputOneOutputOneFramePushRuntime {

    @Override
    protected IFrameTupleAppender getTupleAppender() {
        return (FrameFixedFieldTupleAppender) appender;
    }

    protected IFrameFieldAppender getFieldAppender() {
        return (FrameFixedFieldTupleAppender) appender;
    }

    protected final void initAccessAppendFieldRef(IHyracksTaskContext ctx) throws HyracksDataException {
        frame = new VSizeFrame(ctx);
        appender = new FrameFixedFieldTupleAppender(inputRecordDesc.getFieldCount());
        appender.reset(frame, true);
        tAccess = new FrameTupleAccessor(inputRecordDesc);
        tRef = new FrameTupleReference();
    }

    protected void appendField(byte[] array, int start, int length) throws HyracksDataException {
        appendField(array, start, length, null);
    }

    // the same as the above method. Added the StopWatch to measure the execution time
    protected void appendField(byte[] array, int start, int length, ExecutionTimeStopWatch execTimeProfilerSW)
            throws HyracksDataException {
        if (!ExecutionTimeProfiler.PROFILE_MODE || execTimeProfilerSW == null) {
            FrameUtils.appendFieldToWriter(writer, getFieldAppender(), array, start, length);
        } else {
            FrameUtils.appendFieldToWriter(writer, getFieldAppender(), array, start, length, execTimeProfilerSW);
        }
    }

    protected void appendField(IFrameTupleAccessor accessor, int tid, int fid) throws HyracksDataException {
        appendField(accessor, tid, fid, null);
    }

    // the same as the above method. Added the StopWatch to measure the execution time
    protected void appendField(IFrameTupleAccessor accessor, int tid, int fid, ExecutionTimeStopWatch execTimeProfilerSW)
            throws HyracksDataException {
        if (!ExecutionTimeProfiler.PROFILE_MODE || execTimeProfilerSW == null) {
            FrameUtils.appendFieldToWriter(writer, getFieldAppender(), accessor, tid, fid);
        } else {
            FrameUtils.appendFieldToWriter(writer, getFieldAppender(), accessor, tid, fid, execTimeProfilerSW);
        }
    }

}
