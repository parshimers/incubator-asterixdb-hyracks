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
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import org.apache.hyracks.api.io.FileReference;

public class IOHDFSSubSystem implements IIOSubSystem {
    private URI uri = null;
    private String fsName;
    private static FileSystem fs;
    Configuration conf;

    public IOHDFSSubSystem(String confPath){
        conf = new Configuration();
        //    conf.set("dfs.namenode.replication.considerLoad","false");
        //    conf.set("dfs.datanode.socket.write.timeout","0");
        //    conf.set("dfs.client.read.shortcircuit","true");
        //    conf.set("dfs.domain.socket.path","file:///mnt/heap/hdfs-data/dn_socket");
        //    conf.set("dfs.namenode.replication.considerLoad","false");
        conf.addResource(new Path(confPath + "core-site.xml"));
        conf.addResource(new Path(confPath + "mapred-site.xml"));
        fsName = conf.get("fs.default.name");
        try {
            uri = new URI(fsName);
            fs = FileSystem.get(uri, conf);
        } catch (IOException | URISyntaxException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public static FileSystem getFileSystem(){
        return fs;
    }

    @Override
    public boolean exists(FileReference fileRef) throws IllegalArgumentException, IOException {
        return fs.exists(new Path(uri.toString()   + fileRef.getPath()));
    }

    @Override
    public boolean mkdirs(FileReference fileRef) throws IllegalArgumentException, IOException {
        return fs.mkdirs(new Path(uri.toString()   + fileRef.getPath()));
    }

    @Override
    public boolean delete(FileReference fileRef, boolean recursive) throws IllegalArgumentException, IOException {
        return fs.delete(new Path(uri.toString()   + fileRef.getPath()), recursive);
    }

    @Override
    public boolean deleteOnExit(FileReference fileRef) throws IllegalArgumentException, IOException {
        return fs.deleteOnExit(new Path(uri.toString()   + fileRef.getPath()));
    }

    @Override
    public boolean isDirectory(FileReference fileRef) throws IllegalArgumentException, IOException {
        return fs.isDirectory(new Path(uri.toString()   + fileRef.getPath()));
    }

    @Override
    public String[] listFiles(FileReference fileRef, FilenameFilter filter) throws FileNotFoundException, IllegalArgumentException, IOException {
        ArrayList<String> files = new ArrayList<>();
        FileStatus[] fileStatuses = fs.listStatus(new Path(uri.toString() + fileRef.getPath()));
        for(FileStatus fileStatus: fileStatuses){
            if(filter.accept(new File(Path.getPathWithoutSchemeAndAuthority(fileStatus.getPath().getParent()).toString()),fileStatus.getPath().getName()) && fileStatus.isFile()) {
                files.add((Path.getPathWithoutSchemeAndAuthority(fileStatus.getPath()).toString()));
            }
        }
        String tmp[] = new String[files.size()];
        tmp = files.toArray(tmp);
        return tmp;
    }

    @Override
    public FileReference getParent(FileReference child) throws FileNotFoundException, IOException{
        Path childPath = new Path(uri.toString() + child.getPath());
        Path parentPath = childPath.getParent();
        return new FileReference(Path.getPathWithoutSchemeAndAuthority(parentPath).toString(),
                FileReference.FileReferenceType.DISTRIBUTED_IF_AVAIL);
    }

}
