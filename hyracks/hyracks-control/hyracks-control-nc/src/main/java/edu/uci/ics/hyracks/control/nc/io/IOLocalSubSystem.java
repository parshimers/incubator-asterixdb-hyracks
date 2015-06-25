package edu.uci.ics.hyracks.control.nc.io;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;

import edu.uci.ics.hyracks.api.io.FileReference;

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

}
