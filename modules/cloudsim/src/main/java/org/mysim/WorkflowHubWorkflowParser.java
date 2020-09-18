package org.mysim;

import org.cloudbus.cloudsim.Consts;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.core.CloudSim;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.mysim.utils.Parameters;
import org.mysim.utils.ReplicaCatalog;
import org.mysim.utils.WorkflowParser;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;

public class WorkflowHubWorkflowParser extends WorkflowParser {
    private final String workflowsDirectory;

//    private final List<String> daxPaths;

//    private List<Task> taskList;

    //    private List<Workflow> workflowList;
    private List<String> workflowPathList;

    private final int userId;

    private int jobIdStartsFrom;
    private int workflowIdStartsFrom;

    protected Map<String, Task> mName2Task;

    private int totalWorkflowNumbers;
    JSONParser jsonParser = new JSONParser();

    public WorkflowHubWorkflowParser(int userId) {
        this.userId = userId;
        this.mName2Task = new HashMap<>();
//        this.daxPath = Parameters.getDaxPath();
//        this.daxPaths = Parameters.getDAXPaths();
        this.jobIdStartsFrom = 0;
        this.workflowIdStartsFrom = 0;
//        setWorkflowList(new ArrayList<>());
        setWorkflowPathList(new ArrayList<>());
        this.totalWorkflowNumbers = 0;
        this.workflowsDirectory = Parameters.getWorkflowsDirectory();
        populateWorkflowPathList();
    }

    public List<String> getWorkflowPathList() {
        return workflowPathList;
    }

    public void setWorkflowPathList(List<String> workflowPathList) {
        this.workflowPathList = workflowPathList;
    }
    public int getJobIdStartsFrom() {
        return jobIdStartsFrom;
    }

    public void setJobIdStartsFrom(int jobIdStartsFrom) {
        this.jobIdStartsFrom = jobIdStartsFrom;
    }

    public int getUserId() {
        return userId;
    }

    public boolean hasNextWorkflow(){
        return getWorkflowPathList().size() > 0;
    }

    public Workflow get_next_workflow(){
        if (getWorkflowPathList().size() > 0){
            String path  = getWorkflowPathList().get(0);
            getWorkflowPathList().remove(0);
            return parseJSONFile(path);
        }
        return null;
    }

    public void populateWorkflowPathList(){
        Log.printLine(String.format("Start populateWorkflowPathList from %s", workflowsDirectory));
        File directoryPath = new File(workflowsDirectory);
        File[] l = directoryPath.listFiles();
        for(File file : directoryPath.listFiles()){
            if (file.isFile()){
                getWorkflowPathList().add(file.getAbsolutePath());
            }
        }
        // T ODO add total number of workflows as an attributes
        setTotalWorkflowNumbers(getWorkflowPathList().size());
        Log.printLine(String.format("TotalWorkflowNumbers is %s", getTotalWorkflowNumbers()));
    }

    private void setDepth(Task task, int depth) {
        if (depth > task.getDepth()) {
            task.setDepth(depth);
        }
        for (Task cTask : task.getChildList()) {
            setDepth(cTask, task.getDepth() + 1);
        }
    }

    public int getTotalWorkflowNumbers() {
        return totalWorkflowNumbers;
    }

    public void setTotalWorkflowNumbers(int totalWorkflowNumbers) {
        this.totalWorkflowNumbers = totalWorkflowNumbers;
    }

    private Workflow parseJSONFile(String path) {
        try {

            JSONObject jo = (JSONObject) jsonParser.parse(new FileReader(path));

            String workflowFileName = Paths.get(path).getFileName().toString().split("\\.")[0];
            String WorkflowID = Paths.get(path).getFileName().toString().split("_")[0];
            Workflow wf;
            synchronized (this) {
                wf = new Workflow(workflowFileName,this.workflowIdStartsFrom, -1, -1);
                this.workflowIdStartsFrom++;
            }
            Log.printLine(String.format("Start parsing %s", workflowFileName));
            JSONObject workflowJson = (JSONObject) jo.get("workflow");
            JSONArray jobsJsonArray = (JSONArray) workflowJson.get("jobs");
            for( Object taskObj: jobsJsonArray){
                JSONObject taskJson = (JSONObject) taskObj;
                long length = 0;
                // in workflowhub ID and name are the same and is name like this individuals_00000001
                String nodeName = (String) taskJson.get("name"); // id in pegasus workflow
                String nodeType = (String) taskJson.get("name"); // nam in pegasus workflow
                /**
                 * capture runtime. If not exist, by default the runtime
                 * is 0.1. Otherwise CloudSim would ignore this task.
                 * BUG/#11
                 */
                //multiply the scale, by default it is 1.0
                double runtime;
                if (taskJson.get("runtime")!= null){
                    runtime = (Double) taskJson.get("runtime");
                    if (runtime < 100) {
                        runtime = 100;
                    }
                    length = (long) runtime;
                }else {
                    Log.printLine("Cannot find runtime for " + nodeName + ",set it to be 0");
                }
                // this generator does not provide memory for tasks yet.. set to small value.
                double peak_memory = Parameters.getMinPeakMemory(); ;
                length *= Parameters.getRuntimeScale();
                peak_memory *= Parameters.getPeakMemoryScale();// memory is in Byte
                peak_memory = Math.ceil(peak_memory / Consts.MILLION); // convert memory to MB

                List<FileItem> mFileList = new ArrayList<>();
                JSONArray fileJsonArray = (JSONArray) taskJson.get("files");
                for( Object fileObj: fileJsonArray){
                    JSONObject fileJson = (JSONObject) fileObj;
                    String fileName = (String) fileJson.get("name");
                    if (fileName == null) {
                        Log.print("Error in parsing Json");
                    }
                    fileName = workflowFileName + "_" + fileName;
                    String inout = (String) fileJson.get("link");
                    double size = 0.0;
                    if (fileJson.get("size") != null){
                        size = (Long) fileJson.get("size");
                    }else {
                        Log.printLine("File Size not found for " + fileName);
                    }
                    /*
                      a bug of cloudsim, size 0 causes a problem. 1
                      is ok.
                     */
                    if (size == 0) {
                        size++;
                    }
                    /*
                      Sets the file type 1 is input 2 is output
                     */
                    Parameters.FileType type = Parameters.FileType.NONE;
                    switch (inout) {
                        case "input":
                            type = Parameters.FileType.INPUT;
                            break;
                        case "output":
                            type = Parameters.FileType.OUTPUT;
                            break;
                        default:
                            Log.printLine("Parsing Error");
                            break;
                    }
                    if (size < 0) {
                        size = 0 - size;
                        Log.printLine("Size is negative, I assume it is a parser error");
                    }
                    /*
                     * Note that CloudSim use size as MB,
                     * WorkflowHub generates in KB so we need to convert it to Bytes..
                     */
                    size *= 100;

                    FileItem tFile;
                    if (type == Parameters.FileType.OUTPUT) {
                        /**
                         * It is good that CloudSim does tell
                         * whether a size is zero
                         */
                        tFile = new FileItem(fileName, size);
                    }else if (ReplicaCatalog.containsFile(fileName)) {
                        tFile = ReplicaCatalog.getFile(fileName);
                    }else {
                        tFile = new FileItem(fileName, size);
                        ReplicaCatalog.setFile(fileName, tFile);
                    }
                    tFile.setType(type);
                    mFileList.add(tFile);
                }
                Task task;
                synchronized (this) {
                    //T ODO EHSAN: create task appropriate utilization models..
                    task = new Task(this.jobIdStartsFrom, wf.getWorkflowId(),length, peak_memory);
                    this.jobIdStartsFrom++;
                }
                task.setType(nodeType);
                task.setUserId(userId);
                task.setClassType(Parameters.ClassType.COMPUTE.value);
                mName2Task.put(nodeName, task);
                for (FileItem file : mFileList) {
                    task.addRequiredFile(file.getName());
                }
                task.setFileList(mFileList);
                wf.getTaskList().add(task);
            }

            for( Object taskObj: jobsJsonArray) {
                JSONObject taskJson = (JSONObject) taskObj;
                String childName = (String) taskJson.get("name");
                if (mName2Task.containsKey(childName)) {
                    Task childTask = (Task) mName2Task.get(childName);
                    JSONArray parentsJsonArray = (JSONArray) taskJson.get("parents");
                    for(Object parentNameObj: parentsJsonArray){
                        String parentName = (String) parentNameObj;
                        if (mName2Task.containsKey(parentName)) {
                            Task parentTask = (Task) mName2Task.get(parentName);
                            parentTask.addChild(childTask);
                            childTask.addParent(parentTask);
                        }
                    }
                }
            }

            ArrayList roots = new ArrayList<>();
            for (Task task : mName2Task.values()) {
                task.setDepth(0);
                if (task.getParentList().isEmpty()) {
                    roots.add(task);
                }
            }

            /**
             * Add depth from top to bottom.
             */
            for (Iterator it = roots.iterator(); it.hasNext();) {
                Task task = (Task) it.next();
                setDepth(task, 1);
            }

            this.mName2Task.clear();
            return wf;
        } catch (IOException ioe) {
            Log.printLine("IO Exception;Please make sure dax.path is correctly set in your config file");
        } catch (Exception e) {
            e.printStackTrace();
            Log.printLine("Parsing Exception");
        }
        return null;
    }
}
