/*
 * Copyright 2009-2013 by The Regents of the University of California
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hyracks.dataflow.std.sort;

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.dataflow.common.io.RunFileWriter;
import org.apache.hyracks.dataflow.std.buffermanager.EnumFreeSlotPolicy;

public class ExternalSortRunGenerator extends AbstractExternalSortRunGenerator {

    public ExternalSortRunGenerator(IHyracksTaskContext ctx, int[] sortFields,
            INormalizedKeyComputerFactory firstKeyNormalizerFactory, IBinaryComparatorFactory[] comparatorFactories,
            RecordDescriptor recordDesc, Algorithm alg, int framesLimit) throws HyracksDataException {
        this(ctx, sortFields, firstKeyNormalizerFactory, comparatorFactories, recordDesc, alg,
                EnumFreeSlotPolicy.LAST_FIT, framesLimit);
    }

    public ExternalSortRunGenerator(IHyracksTaskContext ctx, int[] sortFields,
            INormalizedKeyComputerFactory firstKeyNormalizerFactory, IBinaryComparatorFactory[] comparatorFactories,
            RecordDescriptor recordDesc, Algorithm alg, EnumFreeSlotPolicy policy, int framesLimit)
                    throws HyracksDataException {
        this(ctx, sortFields, firstKeyNormalizerFactory, comparatorFactories, recordDesc, alg, policy, framesLimit,
                Integer.MAX_VALUE);
    }

    public ExternalSortRunGenerator(IHyracksTaskContext ctx, int[] sortFields,
            INormalizedKeyComputerFactory firstKeyNormalizerFactory, IBinaryComparatorFactory[] comparatorFactories,
            RecordDescriptor recordDesc, Algorithm alg, EnumFreeSlotPolicy policy, int framesLimit, int outputLimit)
                    throws HyracksDataException {
        super(ctx, sortFields, firstKeyNormalizerFactory, comparatorFactories, recordDesc, alg, policy, framesLimit,
                outputLimit);
    }

    @Override
    protected RunFileWriter getRunFileWriter() throws HyracksDataException {
        FileReference file = ctx.getJobletContext()
                .createManagedWorkspaceFile(ExternalSortRunGenerator.class.getSimpleName());
        return new RunFileWriter(file, ctx.getIOManager());
    }

    @Override
    protected IFrameWriter getFlushableFrameWriter(RunFileWriter writer) throws HyracksDataException {
        return writer;
    }

}
