package org.containerWorkflowsimِDemo.utils;
import org.containerWorkflowsimِDemo.ContainerFileItem;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ContainerReplicaCatalog {
    public enum FileSystem {
        SHARED, LOCAL
    }

    private static Map<String, ContainerFileItem> fileName2File;

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

    public static ContainerFileItem getFile(String fileName) {
        return fileName2File.get(fileName);
    }

    public static void setFile(String fileName, ContainerFileItem file) {
        fileName2File.put(fileName, file);
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
}
