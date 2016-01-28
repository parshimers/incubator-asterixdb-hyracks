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
package org.apache.hyracks.control.nc.io;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;

import org.apache.hyracks.api.io.FileReference;

public class IOLocalSubSystem implements IIOSubSystem {

    @Override
    public boolean exists(FileReference fileRef) {
        return fileRef.getFile().exists();
    }

    @Override
    public boolean mkdirs(FileReference fileRef) {
        return fileRef.getFile().mkdirs();
    }

    @Override
    public boolean delete(FileReference fileRef, boolean recursive) {
        if(recursive) {
            return deleteRecursive(fileRef.getFile());
        } else {
            return fileRef.getFile().delete();
        }
    }

    @Override
    public boolean deleteOnExit(FileReference fileRef) throws IllegalArgumentException, IOException {
        fileRef.getFile().deleteOnExit();
        return true;
    }

    @Override
    public boolean isDirectory(FileReference fileRef) throws IllegalArgumentException, IOException {
        return fileRef.getFile().isDirectory();
    }

    private boolean deleteRecursive(File f) {
        if (f.isDirectory()) {
            for (File c : f.listFiles()) {
                if(!deleteRecursive(c)) return false;
            }
        }
        f.delete();
        return true;
    }

    @Override
    public String[] listFiles(FileReference fileRef, FilenameFilter filter) throws FileNotFoundException,
            IllegalArgumentException, IOException {
        File dir = new File(fileRef.getPath());
        return dir.list(filter);
    }

    @Override public FileReference getParent(FileReference child) throws IllegalArgumentException, IOException {
        return new FileReference(child.getFile().getParent());
    }

}
