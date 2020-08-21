package org.mysim;


import org.cloudbus.cloudsim.Consts;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.core.CloudSim;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;
import org.mysim.utils.Parameters;
import org.mysim.utils.ReplicaCatalog;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;

public class WorkflowParser {

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

    public WorkflowParser(int userId) {
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

//    public List<Workflow> getWorkflowList() {
//        return workflowList;
//    }

//    public void setWorkflowList(List<Workflow> workflowList) {
//        this.workflowList = workflowList;
//    }
    public List<String> getWorkflowPathList() {
        return workflowPathList;
    }

    public void setWorkflowPathList(List<String> workflowPathList) {
        this.workflowPathList = workflowPathList;
    }

//    public void parse(String Path) {
//        parseXmlFile(Path);
//    }


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
            return parseXmlFile(path);
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
        Log.printLine(String.format("TotalWorkflowNumbers is %s", workflowsDirectory));
    }

    private void setDepth(Task task, int depth) {
        if (depth > task.getDepth()) {
            task.setDepth(depth);
        }
        for (Task cTask : task.getChildList()) {
            setDepth(cTask, task.getDepth() + 1);
        }
    }

    private Workflow parseXmlFile(String path) {
        try {
            SAXBuilder builder = new SAXBuilder();
            //parse using builder to get DOM representation of the XML file
            Document dom = builder.build(new File(path));
            Element root = dom.getRootElement();
            List<Element> list = root.getChildren();
            String workflowFileName = Paths.get(path).getFileName().toString().split("\\.")[0];
            String WorkflowID = Paths.get(path).getFileName().toString().split("_")[0];
            Workflow wf;

            synchronized (this) {
                wf = new Workflow(workflowFileName,this.workflowIdStartsFrom, -1, -1);
                this.workflowIdStartsFrom++;
            }
            Log.printLine(String.format("Start parsing %s", workflowFileName));

            for (Element node : list) {
                switch (node.getName().toLowerCase()) {
                    case "job":
                        long length = 0;
                        String nodeName = node.getAttributeValue("id");
                        String nodeType = node.getAttributeValue("name");
                        /**
                         * capture runtime. If not exist, by default the runtime
                         * is 0.1. Otherwise CloudSim would ignore this task.
                         * BUG/#11
                         */
                        //multiply the scale, by default it is 1.0
                        double runtime;
                        if (node.getAttributeValue("runtime") != null) {
                            String nodeTime = node.getAttributeValue("runtime");
                            runtime = 1000 * Double.parseDouble(nodeTime);
                            if (runtime < 100) {
                                runtime = 100;
                            }
                            length = (long) runtime;
                        } else {
                            Log.printLine("Cannot find runtime for " + nodeName + ",set it to be 0");
                        }

                        // T ODO EHSAN: MEMORY IS IN BYTES..CONVERT IF NEEDED
                        double peak_memory = 0;
                        if (node.getAttributeValue("peak_memory") != null) {
                            String node_peak_memory = node.getAttributeValue("peak_memory");
                            peak_memory = Double.parseDouble(node_peak_memory);
                            if (peak_memory < Parameters.getMinPeakMemory()) {
                                peak_memory =  Parameters.getMinPeakMemory();
                            }

                        } else {
                            Log.printLine("Cannot find peak_memory for " + nodeName + ",set it to be 0");
                        }

                        length *= Parameters.getRuntimeScale();
                        peak_memory *= Parameters.getPeakMemoryScale();// memory is in Byte
                        peak_memory = Math.ceil(peak_memory / Consts.MILLION); // convert memory to MB

                        List<Element> fileList = node.getChildren();
                        List<FileItem> mFileList = new ArrayList<>();
                        for (Element file : fileList) {
                            if (file.getName().toLowerCase().equals("uses")) {
                                String fileName = file.getAttributeValue("name");//DAX version 3.3
                                if (fileName == null) {
                                    fileName = file.getAttributeValue("file");//DAX version 3.0
                                }
                                if (fileName == null) {
                                    Log.print("Error in parsing xml");
                                }
                                fileName = workflowFileName + "_" + fileName;

                                String inout = file.getAttributeValue("link");
                                double size = 0.0;

                                String fileSize = file.getAttributeValue("size");
                                if (fileSize != null) {
                                    size = Double.parseDouble(fileSize) /*/ 1024*/;
                                } else {
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

                                FileItem tFile;
                                /*
                                 * Already exists an input file (forget output file)
                                 */
                                if (size < 0) {
                                    size = 0 - size;
                                    Log.printLine("Size is negative, I assume it is a parser error");
                                }
                                /*
                                 * Note that CloudSim use size as MB, in this case we use it as Byte
                                 */
                                if (type == Parameters.FileType.OUTPUT) {
                                    /**
                                     * It is good that CloudSim does tell
                                     * whether a size is zero
                                     */
                                    tFile = new FileItem(fileName, size);
                                } else if (ReplicaCatalog.containsFile(fileName)) {
                                    tFile = ReplicaCatalog.getFile(fileName);
                                } else {
                                    tFile = new FileItem(fileName, size);
                                    ReplicaCatalog.setFile(fileName, tFile);
                                }

                                tFile.setType(type);
                                mFileList.add(tFile);
                            }
                        }
                        Task task;
                        //In case of multiple workflow submission. Make sure the jobIdStartsFrom is consistent.
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
                        break;
                    case "child":
                        List<Element> pList = node.getChildren();
                        String childName = node.getAttributeValue("ref");
                        if (mName2Task.containsKey(childName)) {
                            Task childTask = (Task) mName2Task.get(childName);
                            for (Element parent : pList) {
                                String parentName = parent.getAttributeValue("ref");
                                if (mName2Task.containsKey(parentName)) {
                                    Task parentTask = (Task) mName2Task.get(parentName);
                                    parentTask.addChild(childTask);
                                    childTask.addParent(parentTask);
                                }
                            }
                        }
                        break;
                }
            }
            /**
             * If a task has no parent, then it is root task.
             */
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

//            wf.setTaskList(taskList);
            //T ODO EHSAN: add deadline and budget to a workflow
//            wf.setDeadline();
//            wf.setBudget();
//            this.workflowList.add(wf);
            /*
              Clean them so as to save memory. Parsing workflow may take much memory
             */
            this.mName2Task.clear();
            return wf;

        } catch (JDOMException jde) {
            Log.printLine("JDOM Exception;Please make sure your dax file is valid");
        } catch (IOException ioe) {
            Log.printLine("IO Exception;Please make sure dax.path is correctly set in your config file");

        } catch (Exception e) {
            e.printStackTrace();
            Log.printLine("Parsing Exception");
        }
        return null;
    }

    public int getTotalWorkflowNumbers() {
        return totalWorkflowNumbers;
    }

    public void setTotalWorkflowNumbers(int totalWorkflowNumbers) {
        this.totalWorkflowNumbers = totalWorkflowNumbers;
    }
}
