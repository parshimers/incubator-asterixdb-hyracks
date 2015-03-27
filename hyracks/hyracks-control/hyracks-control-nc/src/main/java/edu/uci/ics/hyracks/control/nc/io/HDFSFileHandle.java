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
package edu.uci.ics.hyracks.control.nc.io;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.io.IIOManager.FileReadWriteMode;
import edu.uci.ics.hyracks.api.io.IIOManager.FileSyncMode;
import edu.uci.ics.hyracks.control.nc.io.IFileHandleInternal;
import edu.uci.ics.hyracks.api.io.IFileHandle;

public class HDFSFileHandle implements IFileHandle, IFileHandleInternal {
    private URI uri;
    static {
        Configuration conf = new Configuration();
        conf.addResource(new Path("config/core-site.xml"));
        conf.addResource(new Path("config/hdfs-site.xml"));
        conf.addResource(new Path("config/mapred-site.xml"));
        System.out.println("SHORTCIRCUIT " + conf.get("dfs.client.read.shortcircuit"));
        try {
            fs = FileSystem.get(new URI("hdfs://sandbox.hortonworks.com/hdfstest/"), conf);
        } catch (IOException | URISyntaxException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    private static FileSystem fs;
    private FSDataOutputStream out = null;
    private FSDataInputStream in = null;
    private Path path;
    private FileReference fileRef;

    public HDFSFileHandle(FileReference fileRef) {
        try {
            this.uri = new URI("hdfs://sandbox.hortonworks.com/hdfstest/" + fileRef.getPath());
            this.fileRef = fileRef;
            path = new Path(uri.getPath());
        } catch (URISyntaxException e) {
            // can't happen. -.-
        }
    }
    
    @Override
    public void open(FileReadWriteMode rwMode, FileSyncMode syncMode) throws IOException {
        if(syncMode != FileSyncMode.METADATA_ASYNC_DATA_ASYNC) throw new IOException("Sync I/O not (yet) supported for HDFS");
        
        if(rwMode == FileReadWriteMode.READ_WRITE) out = fs.create(path, false);
    }

    @Override
    public void close() throws IOException {
        if(!fs.exists(path)) return;
        if(out != null) out.close();
        if(in != null) in.close();
        out = null;
        in = null;
    }

    @Override
    public FileReference getFileReference() {
        return fileRef;
    }

    @Override
    public RandomAccessFile getRandomAccessFile() {
        throw new NotImplementedException();
    }

    @Override
    public void sync(boolean metadata) throws IOException {
        out.hsync();
        in = null;
    }

    @Override
    public long getSize() throws IOException {
        return fs.getFileStatus(path).getLen();
    }

    @Override
    public int write(ByteBuffer data, long offset) throws IOException {
        System.out.println("WRITE " + path + " @ " + offset);
        assert(out.getPos() == offset);
        out.write(data.array(), 0, data.limit());
        in = null;
        return data.limit();
    }

    @Override
    public int read(ByteBuffer data, long offset) throws IOException {
        if(in == null) {
            out.hflush();
            in = fs.open(path);
        }
        System.out.println("READ " + path + " @ " + offset);
        
        in.seek(offset);
        return in.read(data);
    }

    
}
