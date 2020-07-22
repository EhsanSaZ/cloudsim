package org.mysim.utils;

import com.sun.xml.internal.ws.api.ha.StickyFeature;
import org.mysim.FileItem;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ReplicaCatalog {
    public enum FileSystem {
        SHARED, LOCAL
    }

    private static Map<String, FileItem> fileName2File;

    private static FileSystem fileSystem;

    private static Map<String, List<String>> dataReplicaCatalog;

    public static void init(FileSystem fs) {
        fileSystem = fs;
        dataReplicaCatalog = new HashMap<>();
        fileName2File = new HashMap <>();
    }

    public static FileSystem getFileSystem() {
        return fileSystem;
    }

    public static FileItem getFile(String fileName) {
        return fileName2File.get(fileName);
    }

    public static void setFile(String fileName, FileItem file) {
        fileName2File.put(fileName, file);
    }

    public static void deleteFile(String fileName){
        fileName2File.remove(fileName);
    }

    public static boolean containsFile(String fileName) {
        return fileName2File.containsKey(fileName);
    }

    public static List<String> getStorageList(String file) {
        return dataReplicaCatalog.get(file);
    }

    public static void addFileToStorage(String file, String storage) {
        if (!dataReplicaCatalog.containsKey(file)) {
            dataReplicaCatalog.put(file, new ArrayList<>());
        }
        List<String> list = getStorageList(file);
        if (!list.contains(storage)) {
            list.add(storage);
        }
    }

    public static void removeStorageFromStorageList(String storage){
//        List<String> fileToRemove = new ArrayList<>();
        for (String file:dataReplicaCatalog.keySet()){
            List<String> list = getStorageList(file);
            list.remove(storage);
//            if (list.size() == 0) {
//                fileToRemove.add(file);
//            }
        }
//        for ( String file: fileToRemove){
//            dataReplicaCatalog.remove(file);
//        }
    }

    public static void removeFileStorage(String file) {
        dataReplicaCatalog.remove(file);
    }
}
