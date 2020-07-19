package org.mysim;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.container.core.ContainerCloudlet;
import org.cloudbus.cloudsim.core.CloudSim;
import org.mysim.deadlinedistribution.DeadlineDistributionSimpleUpwardRank;
import org.mysim.utils.Parameters;
import org.mysim.utils.ReplicaCatalog;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

public class learn {
    public static void main(String[] args) {
//        String path = "E:\\term12\\Dataset\\small workload\\0_SIPHT_1000.xml";
//
//        System.out.println(Paths.get(path).getFileName().toString().split("\\.")[0]);

//        Map<Integer, Task> map = new HashMap<Integer, Task >();
//        List<Task> l = new ArrayList<>();
//
//        Task b = new Task(1,2,3);
//        l.add(b);
//
//        map.put(1,b);
//
//        for (Task s : map.values()) {
//            s.setDepth(500);
//        }
//
//        map.clear();
//        System.out.println(b);


//        ReplicaCatalog.FileSystem file_system = ReplicaCatalog.FileSystem.SHARED;
//        ReplicaCatalog.init(file_system);
//
//        Parameters.init("E:\\term12\\Dataset\\small workload");
//        WorkflowParser wp = new WorkflowParser(1);
//
////        wp.populateWorkflowPathList();
////        wp.get_next_workflow();
//        while(wp.hasNextWorkflow()){
//            wp.get_next_workflow().getName();
////            System.out.println(wp.get_next_workflow().getName());
//        }
//        wp.getWorkflowList();
//        wp.getWorkflowList();
////        wp.parse("E:\\term12\\Dataset\\small workload");
////        wp.parse("E:\\term12\\Dataset\\testworkload");
//        System.out.println("Paths.get(path).getFileName().toString()[0]");



//        ReplicaCatalog.FileSystem file_system = ReplicaCatalog.FileSystem.SHARED;
//        ReplicaCatalog.init(file_system);
//
//        Parameters.init("E:\\term12\\Dataset\\test workload");
//
//        WorkflowParser wp = new WorkflowParser(1);
//
//        while (wp.hasNextWorkflow()) {
//            System.out.println(wp.get_next_workflow().getName());
//        }
//        List<Workflow> wf_list = wp.getWorkflowList();
//        Workflow w = wf_list.get(0);
//
//        w.setDeadline(1000);
//
//        DeadlineDistributionSimpleUpwardRank deadline_strategy = new DeadlineDistributionSimpleUpwardRank();
//        deadline_strategy.setWorkflow(w);
//        deadline_strategy.run();
//        System.out.println(wf_list.size());



//        List<Task> l1 = new ArrayList<>();
//        changeList(l1);
//        System.out.println(l1);
//    }
//    public static void changeList(List<Task> l1){
//        l1.add(new Task(1,1,1,1));
//        l1.add(new Task(2,2,2,2));
//        l1.add(new Task(3,3,3,3));


//        ReplicaCatalog.FileSystem file_system = ReplicaCatalog.FileSystem.SHARED;
//        ReplicaCatalog.init(file_system);
//
//        Parameters.init("E:\\term12\\Dataset\\test workload");
//
//        List <Task> rady_list = new ArrayList<>();
//        WorkflowParser wp = new WorkflowParser(1);
//        if(wp.hasNextWorkflow()){
//            Workflow wf = wp.get_next_workflow();
//
//            System.out.println(wf.getTaskList().size());
//            List <Task> list = wf.getTaskList();
//
//
////            rady_list.add(list.get(0));
////            list.remove(0);
////            rady_list.get(0).setMemory(55555);
////            System.out.println(wf.getTaskList().size());
//            List list2 = new ArrayList<>();
//            list2.addAll(list);
//            list.clear();
//
////            list.clear();
////            list = new ArrayList<>();
//            System.out.println(list2.size());
//        }



//        int num_user = 1; // number of cloud users
//        Calendar calendar = Calendar.getInstance(); // Calendar whose fields have been initialized with the current date and time.
//        boolean trace_flag = false; // trace events
//        CloudSim.init(num_user, calendar, trace_flag);
//
//        ReplicaCatalog.FileSystem file_system = ReplicaCatalog.FileSystem.SHARED;
//        ReplicaCatalog.init(file_system);
//
//        Parameters.init("E:\\term12\\Dataset\\test workload");
//
//        List <Task> rady_list = new ArrayList<>();
//
//        WorkflowParser wp = new WorkflowParser(1);
//        WorkflowEngine we = new WorkflowEngine("workflow_engine");
//        we.setWorkflowParser(wp);
//
//        if(wp.hasNextWorkflow()){
//            Workflow wf = wp.get_next_workflow();
//
//            List <Task> list = wf.getTaskList();
//            System.out.println(list.size());
//            we.processDatastaging(wf);
//            System.out.println(wf.getTaskList().size());
//
//        }


        int num_user = 1; // number of cloud users
        Calendar calendar = Calendar.getInstance(); // Calendar whose fields have been initialized with the current date and time.
        boolean trace_flag = false; // trace events
        CloudSim.init(num_user, calendar, trace_flag);

        ReplicaCatalog.FileSystem file_system = ReplicaCatalog.FileSystem.SHARED;
        ReplicaCatalog.init(file_system);

        Parameters.init("E:\\term12\\Dataset\\test workload");

        List <Task> rady_list = new ArrayList<>();

        WorkflowParser wp = new WorkflowParser(1);
        WorkflowEngine we = new WorkflowEngine("workflow_engine");
        we.setWorkflowParser(wp);

        if(wp.hasNextWorkflow()){
            Workflow wf = wp.get_next_workflow();

            List <Task> list = wf.getTaskList();
//            Task t1 = list.get(0);
            for (ContainerCloudlet c: wf.getTaskList()){
                Task t = (Task) c;
                t.setSubDeadline(500);
                c.setVmId(1);
            }

            System.out.println(wf.getTaskList().size());

        }

    }

}
