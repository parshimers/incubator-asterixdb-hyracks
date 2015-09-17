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

package org.apache.hyracks.api.util;

import java.text.SimpleDateFormat;
import java.util.Calendar;

public class SpatialIndexProfiler {
    public static final SpatialIndexProfiler INSTANCE = new SpatialIndexProfiler();
    public static final String PROFILE_HOME_DIR = "/mnt/data/sdg/youngsk2/";
    public ExperimentProfiler shbtreeNumOfSearchPerQuery;
    public ExperimentProfiler dhbtreeNumOfSearchPerQuery;
    public ExperimentProfiler dhvbtreeNumOfSearchPerQuery;
    public ExperimentProfiler rtreeNumOfSearchPerQuery;
    public ExperimentProfiler sifNumOfSearchPerQuery;
    public ExperimentProfiler falsePositivePerQuery;
    public ExperimentProfiler cacheMissPerQuery;
    public ExperimentProfiler pidxSearchTimePerQuery;
    public ExperimentProfiler sidxCPUCostProfiler;

    private SpatialIndexProfiler() {
        if (ExperimentProfiler.PROFILE_MODE) {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd-HH:mm:ss.SSS");
            shbtreeNumOfSearchPerQuery = new ExperimentProfiler(PROFILE_HOME_DIR + "shbtreeNumOfSearchPerQuery-" + sdf.format(Calendar.getInstance().getTime()) + ".txt", 1);
            shbtreeNumOfSearchPerQuery.begin();
            dhbtreeNumOfSearchPerQuery = new ExperimentProfiler(PROFILE_HOME_DIR + "dhbtreeNumOfSearchPerQuery-" + sdf.format(Calendar.getInstance().getTime()) + ".txt", 1);
            dhbtreeNumOfSearchPerQuery.begin();
            dhvbtreeNumOfSearchPerQuery = new ExperimentProfiler(PROFILE_HOME_DIR + "dhvbtreeNumOfSearchPerQuery-" + sdf.format(Calendar.getInstance().getTime()) + ".txt", 1);
            dhvbtreeNumOfSearchPerQuery.begin();
            rtreeNumOfSearchPerQuery = new ExperimentProfiler(PROFILE_HOME_DIR + "rtreeNumOfSearchPerQuery-" + sdf.format(Calendar.getInstance().getTime()) + ".txt", 1);
            rtreeNumOfSearchPerQuery.begin();
            sifNumOfSearchPerQuery = new ExperimentProfiler(PROFILE_HOME_DIR + "sifNumOfSearchPerQuery-" + sdf.format(Calendar.getInstance().getTime()) + ".txt", 1);
            sifNumOfSearchPerQuery.begin();
            
            falsePositivePerQuery = new ExperimentProfiler(PROFILE_HOME_DIR + "falsePositivePerQuery-" + sdf.format(Calendar.getInstance().getTime()) + ".txt", 1);
            falsePositivePerQuery.begin();
            cacheMissPerQuery = new ExperimentProfiler(PROFILE_HOME_DIR + "cacheMissPerQuery-" + sdf.format(Calendar.getInstance().getTime()) + ".txt", 1);
            cacheMissPerQuery.begin();
            pidxSearchTimePerQuery = new ExperimentProfiler(PROFILE_HOME_DIR + "pidxSearchTimePerQuery-" + sdf.format(Calendar.getInstance().getTime()) + ".txt", 1);
            pidxSearchTimePerQuery.begin();
            sidxCPUCostProfiler = new ExperimentProfiler(PROFILE_HOME_DIR + "sidxCPUCostProfiler-" + sdf.format(Calendar.getInstance().getTime()) + ".txt", 1);
            sidxCPUCostProfiler.begin();
        }
    }
}
