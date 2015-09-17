/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hyracks.storage.am.lsm.common.impls;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.common.api.IndexException;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;

public class ExponentialMergePolicy implements ILSMMergePolicy {

    private long maxMergableComponentSize;
    private long maxToleranceComponentCount;
    private long maxMergableComponentLevelCount;
    private boolean isFirstCall = true;
    private LinkedList<LinkedList<AbstractDiskLSMComponent>> allMergableLevelComponents = new LinkedList<LinkedList<AbstractDiskLSMComponent>>();

    @Override
    public synchronized void diskComponentAdded(final ILSMIndex index, boolean fullMergeIsRequested,
            AbstractDiskLSMComponent newComponent) throws HyracksDataException, IndexException {

        if (fullMergeIsRequested) {
            throw new UnsupportedOperationException("Full merge is not supported by ExponentialMergePolicy.");
        }

        if (isFirstCall) {
            //read all existing components and add them into the first level 
            //except components whose size is greater than maxMergableComponentSize
            List<ILSMComponent> immutableComponents = new ArrayList<ILSMComponent>(index.getImmutableComponents());

            int size = immutableComponents.size();
            int firstNonMergableComponentIndex = size;
            for (int i = 0; i < size; i++) {
                if (((AbstractDiskLSMComponent) immutableComponents.get(i)).getComponentSize() > maxMergableComponentSize) {
                    firstNonMergableComponentIndex = i;
                    break;
                }
            }

            //get only mergable components and put into candidateComponents.
            List<ILSMComponent> candidateComponents = immutableComponents.subList(0, firstNonMergableComponentIndex);
            //sort components from oldest to newer components
            Collections.reverse(candidateComponents);
            LinkedList<AbstractDiskLSMComponent> aLevelComponents = allMergableLevelComponents.get(0);
            //add all candidate components to the first level list and merge added components if necessary 
            for (ILSMComponent c : candidateComponents) {
                aLevelComponents.add(0, (AbstractDiskLSMComponent) c);
                if (aLevelComponents.size() > maxToleranceComponentCount) {
                    Integer destLevel = new Integer(0);
                    List<ILSMComponent> mergableComponents = new ArrayList<ILSMComponent>();
                    mergableComponents.addAll(aLevelComponents);
                    aLevelComponents.clear();
                    ILSMIndexAccessor accessor = (ILSMIndexAccessor) index.createAccessor(
                            NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
                    accessor.scheduleMerge(index.getIOOperationCallback(), mergableComponents, destLevel);
                }
            }

            isFirstCall = false;
            return;
        }

        //1. decide destination level to which to add the newComponent
        Integer destLevel = new Integer(0);
        if (newComponent.getMergePolicyInfo() != null) {
            destLevel = ((Integer) newComponent.getMergePolicyInfo()) + 1;
        } else {
            destLevel = findProperDestLevel(newComponent.getComponentSize());
        }

        //2. don't merge a component whose level is higher than the max mergable component level
        if (destLevel+1 > maxMergableComponentLevelCount) {
            return;
        }

        //3. add the newComponent to the destination level
        LinkedList<AbstractDiskLSMComponent> aLevelComponents = allMergableLevelComponents.get(destLevel);
        aLevelComponents.add(0, newComponent);

        //4. if the number of components in the level exceeds the maxToleranceComponentCount, schedule a merge
        if (aLevelComponents.size() > maxToleranceComponentCount) {
            List<ILSMComponent> mergableComponents = new ArrayList<ILSMComponent>();
            mergableComponents.addAll(aLevelComponents);
            aLevelComponents.clear();
            ILSMIndexAccessor accessor = (ILSMIndexAccessor) index.createAccessor(NoOpOperationCallback.INSTANCE,
                    NoOpOperationCallback.INSTANCE);
            accessor.scheduleMerge(index.getIOOperationCallback(), mergableComponents, destLevel);
        }
    }

    private int findProperDestLevel(long componentSize) {
        //TODO put the newComponent into a level of which component average size is most similar to the size of the new component
        //simply add to the first level for now. 
        return 0;
    }

    @Override
    public void configure(Map<String, String> properties) {
//        maxMergableComponentSize = Long.parseLong(properties.get("max-mergable-component-size"));
//        maxMergableComponentLevel = Long.parseLong(properties.get("max-mergable-component-level"));
//        maxToleranceComponentCount = Integer.parseInt(properties.get("max-tolerance-component-count"));
//        for (int i = 0; i < maxMergableComponentLevel; i++) {
//            allMergableLevelComponents.add(new LinkedList<AbstractDiskLSMComponent>());
//        }
        
        maxMergableComponentSize = 1024 * 1024 * 1024;
        maxMergableComponentLevelCount = 3;
        maxToleranceComponentCount = 9;
        for (int i = 0; i < maxMergableComponentLevelCount; i++) {
            allMergableLevelComponents.add(new LinkedList<AbstractDiskLSMComponent>());
        }
    }

    @Override
    public boolean isMergeLagging(ILSMIndex index) {
        //TODO implement properly according to the merge policy
        return false;
    }
}
