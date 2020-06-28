package org.containerWorkflowsimDemo;

import org.cloudbus.cloudsim.Log;
import org.containerWorkflowsimDemo.utils.ContainerReplicaCatalog;
import org.containerWorkflowsimDemo.utils.ContainerParameters;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;

import java.io.File;
import java.io.IOException;
import java.util.*;

public final class ContainerWorkflowParser{

    private final String daxPath;

    private final List<String> daxPaths;

    private List<ContainerTask> taskList;

    private final int userId;

    private int jobIdStartsFrom;

    @SuppressWarnings("unchecked")
    public List<ContainerTask> getTaskList() {
        return taskList;
    }

    protected void setTaskList(List<ContainerTask> taskList) {
        this.taskList = taskList;
    }

    protected Map<String, ContainerTask> mName2Task;

    public ContainerWorkflowParser(int userId) {
        this.userId = userId;
        this.mName2Task = new HashMap<>();
        this.daxPath = ContainerParameters.getDaxPath();
        this.daxPaths = ContainerParameters.getDAXPaths();
        this.jobIdStartsFrom = 1;

        setTaskList(new ArrayList<>());
    }

    public void parse() {
        if (this.daxPath != null) {
            containerParseXmlFile(this.daxPath);
        } else if (this.daxPaths != null) {
            for (String path : this.daxPaths) {
                containerParseXmlFile(path);
            }
        }
    }

    private void setDepth(ContainerTask task, int depth) {
        if (depth > task.getDepth()) {
            task.setDepth(depth);
        }
        for (ContainerTask cTask : task.getChildList()) {
            setDepth(cTask, task.getDepth() + 1);
        }
    }

    private void containerParseXmlFile(String path) {
        try {
            SAXBuilder builder = new SAXBuilder();
            //parse using builder to get DOM representation of the XML file
            Document dom = builder.build(new File(path));
            Element root = dom.getRootElement();
            List<Element> list = root.getChildren();
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
                        // TODO EHSAN: SCALE OF MEMORY AND RUNTIME NEEDED TO BE CHECKED
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
                        }   //multiple the scale, by default it is 1.0
                        // TODO EHSAN: MEMORY IS IN BYTES..CONVERT IF NEEDED
                        double peak_memory = 0;
                        if (node.getAttributeValue("peak_memory") != null) {
                            String node_peak_memory = node.getAttributeValue("peak_memory");
                            peak_memory = Double.parseDouble(node_peak_memory);
                            if (peak_memory < 100) {
                                peak_memory = 100;
                            }

                        } else {
                            Log.printLine("Cannot find peak_memory for " + nodeName + ",set it to be 0");
                        }   //multiple the scale, by default it is 1.0


                        length *= ContainerParameters.getRuntimeScale();
                        List<Element> fileList = node.getChildren();
                        List<ContainerFileItem> mFileList = new ArrayList<>();
                        for (Element file : fileList) {
                            if (file.getName().toLowerCase().equals("uses")) {
                                String fileName = file.getAttributeValue("name");//DAX version 3.3
                                if (fileName == null) {
                                    fileName = file.getAttributeValue("file");//DAX version 3.0
                                }
                                if (fileName == null) {
                                    Log.print("Error in parsing xml");
                                }

                                String inout = file.getAttributeValue("link");
                                double size = 0.0;

                                String fileSize = file.getAttributeValue("size");
                                if (fileSize != null) {
                                    size = Double.parseDouble(fileSize) /*/ 1024*/;
                                } else {
                                    Log.printLine("File Size not found for " + fileName);
                                }

                                /**
                                 * a bug of cloudsim, size 0 causes a problem. 1
                                 * is ok.
                                 */
                                if (size == 0) {
                                    size++;
                                }
                                /**
                                 * Sets the file type 1 is input 2 is output
                                 */
                                ContainerParameters.FileType type = ContainerParameters.FileType.NONE;
                                switch (inout) {
                                    case "input":
                                        type = ContainerParameters.FileType.INPUT;
                                        break;
                                    case "output":
                                        type = ContainerParameters.FileType.OUTPUT;
                                        break;
                                    default:
                                        Log.printLine("Parsing Error");
                                        break;
                                }
                                ContainerFileItem tFile;
                                /*
                                 * Already exists an input file (forget output file)
                                 */
                                if (size < 0) {
                                    /*
                                     * Assuming it is a parsing error
                                     */
                                    size = 0 - size;
                                    Log.printLine("Size is negative, I assume it is a parser error");
                                }
                                /*
                                 * Note that CloudSim use size as MB, in this case we use it as Byte
                                 */
                                if (type == ContainerParameters.FileType.OUTPUT) {
                                    /**
                                     * It is good that CloudSim does tell
                                     * whether a size is zero
                                     */
                                    tFile = new ContainerFileItem(fileName, size);
                                } else if (ContainerReplicaCatalog.containsFile(fileName)) {
                                    tFile = ContainerReplicaCatalog.getFile(fileName);
                                } else {

                                    tFile = new ContainerFileItem(fileName, size);
                                    ContainerReplicaCatalog.setFile(fileName, tFile);
                                }

                                tFile.setType(type);
                                mFileList.add(tFile);

                            }
                        }
                        ContainerTask task;
                        //In case of multiple workflow submission. Make sure the jobIdStartsFrom is consistent.
                        synchronized (this) {
                            //TODO EHSAN: CREATE TASK WITH MORE INFO..
                            task = new ContainerTask(this.jobIdStartsFrom, length, peak_memory);
                            this.jobIdStartsFrom++;
                        }
                        task.setType(nodeType);
                        task.setUserId(userId);
                        mName2Task.put(nodeName, task);
                        for (ContainerFileItem file : mFileList) {
                            task.addRequiredFile(file.getName());
                        }
                        task.setFileList(mFileList);
                        this.getTaskList().add(task);

                        /**
                         * Add dependencies info.
                         */
                        break;
                    case "child":
                        List<Element> pList = node.getChildren();
                        String childName = node.getAttributeValue("ref");
                        if (mName2Task.containsKey(childName)) {

                            ContainerTask childTask = (ContainerTask) mName2Task.get(childName);

                            for (Element parent : pList) {
                                String parentName = parent.getAttributeValue("ref");
                                if (mName2Task.containsKey(parentName)) {
                                    ContainerTask parentTask = (ContainerTask) mName2Task.get(parentName);
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
            for (ContainerTask task : mName2Task.values()) {
                task.setDepth(0);
                if (task.getParentList().isEmpty()) {
                    roots.add(task);
                }
            }

            /**
             * Add depth from top to bottom.
             */
            for (Iterator it = roots.iterator(); it.hasNext();) {
                ContainerTask task = (ContainerTask) it.next();
                setDepth(task, 1);
            }
            /**
             * Clean them so as to save memory. Parsing workflow may take much
             * memory
             */
            this.mName2Task.clear();

        } catch (JDOMException jde) {
            Log.printLine("JDOM Exception;Please make sure your dax file is valid");

        } catch (IOException ioe) {
            Log.printLine("IO Exception;Please make sure dax.path is correctly set in your config file");

        } catch (Exception e) {
            e.printStackTrace();
            Log.printLine("Parsing Exception");
        }
    }
}
