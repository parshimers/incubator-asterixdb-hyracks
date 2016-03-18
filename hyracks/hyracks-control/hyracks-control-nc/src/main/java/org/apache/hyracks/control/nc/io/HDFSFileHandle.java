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
package org.apache.hyracks.control.nc.io;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
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

import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager.FileReadWriteMode;
import org.apache.hyracks.api.io.IIOManager.FileSyncMode;
import org.apache.hyracks.control.nc.io.IFileHandleInternal;
import org.apache.hyracks.api.io.IFileHandle;

public class HDFSFileHandle implements IFileHandle, IFileHandleInternal {
    private URI uri;
    private static String fsName;
    private static FileSystem fs = null;
    private FSDataOutputStream out = null;
    private FSDataInputStream in = null;
    private Path path;
    private FileReference fileRef;
    private FileReadWriteMode rwMode;

    public HDFSFileHandle(FileReference fileRef) {
        try {
            if(fs == null) {
                fs = IOHDFSSubSystem.getFileSystem();
            }
            fsName = fs.getConf().get("fs.default.name");
            this.uri = new URI(fsName + fileRef.getPath());
            this.fileRef = fileRef;
            path = new Path(uri.getPath());
        } catch ( URISyntaxException e) {
        }
    }
    
    @Override
    public void open(FileReadWriteMode rwMode, FileSyncMode syncMode) throws IOException {
        if(syncMode != FileSyncMode.METADATA_ASYNC_DATA_ASYNC) throw new IOException("Sync I/O not (yet) supported for HDFS");
           if (rwMode == FileReadWriteMode.READ_WRITE) {
               if (fs.exists(path)) {
                   out = fs.append(path);
               } else {
                   out = fs.create(path, false);
               }
           }
        else if(rwMode == FileReadWriteMode.READ_ONLY){
               in = fs.open(path);
           }
        this.rwMode = rwMode;

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
    }


    @Override
    public long getSize() throws IOException {
        return fs.getFileStatus(path).getLen();
    }

    @Override
    public int write(ByteBuffer data, long offset) throws IOException {
        out.write(data.array(), 0, data.limit()-data.position());
        data.position(data.limit());
        return data.limit();
    }

    @Override
    public int append(ByteBuffer data) throws IOException {
        out.write(data.array(), data.position(), data.limit()-data.position());
        data.position(data.limit());
        return data.limit();
    }

    @Override
    public int read(ByteBuffer data, long offset) throws IOException {
        if(in == null && rwMode == FileReadWriteMode.READ_WRITE){
            if(out!=null) out.hsync();
            in = fs.open(path);
        }
        try {
            in.seek(offset);
        } catch (EOFException e){
            return -1;
        }
        return in.read(data);
    }

    @Override
    public InputStream getInputStream() throws IOException{
        if(in == null && rwMode == FileReadWriteMode.READ_WRITE){
            if(out!=null) out.hsync();
            in = fs.open(path);
        }
        return in;
    }

    
}
