package com.actio.dpsystem;

import com.actio.DataSet;
import com.actio.DataSourceREST;
import com.actio.Task;
import com.actio.TaskService;

import java.util.List;

/**
 * Created by jim on 8/03/2016.
 */

/*
    Responsible for Engine for executing a pipeline and maintaining
    any state and services associtated with a pipeline instance

 */

public class DPSystemRuntime extends DPSystemConfigurable {

    private final DPFnNode root = new DPFnNode("root", "root");
    // Collection of Instantiated Pipelines within the System
    private DPSystemConfig sysconf;

    public DPSystemConfig getSysconf() {
        return sysconf;
    }

    public void setRuntimeConfig(DPSystemConfig sysConf) throws Exception
    {

        this.sysconf = sysConf;
        setConfig(sysconf.getConfig(), sysconf.getMasterConfig());
    }

    public void execute() throws Exception
    {
        execute(null);
    }

    public void execute(String execName) throws Exception
    {
        execute(execName, null);
    }

    public void execute(String execName, DataSet ds) throws Exception {
        // by default execute the pipe in exec
        List<DPFnNode> execpipes = sysconf.getExecutables(execName);

        // use factory to instantiate the runtime components

        if (execpipes.size() == 0) {
            logger.warn("No pipelines found to execute");
            return;
        }

        logger.info("***** RUN = '" + getRunID() + "' InstancedID='" + getInstanceID() + "'");

        // These are all parallel pipes for execution
        for (DPFnNode n : execpipes){

            // processing pipe

            logger.info("====== execute Pipeline '"+n.name+"'");
            n.dump();

            // add to the root node for tracking
            root.add(n);
            events.info("", "", "START", "Started DataPipes Runtime", "run", "", 0);
            // Instantiate the Node Function
            Task t = DPSystemFactory.newTask(sysconf,n);
            t.dataSet = ds;
            t.execute();
            events.info("", "", "FINISH", "Ending DataPipes Runtime", "run", "", 0);
            n.dump();
        }

        //events.dump();

    }


    public void sendEvents() throws Exception {
        // going to instantiate a new factory & runtime
        // based upon the systemConfig to send the events out of the existing run

        DPSystemFactory eventFactory = new DPSystemFactory();
        // locate filename
        String sysconfigfilename = getSysconf().getSystemConfig("eventConfigName").toString();
        logger.info("trying to load systemconfig" + sysconfigfilename);
        eventFactory.loadConfig(sysconfigfilename);
        DPSystemRuntime eventRunTime = eventFactory.newRuntime();

        // Generate the required DataSets

        // 1.

        DPEventPublisher dppub = new DPEventPublisher(this);
        DataSet ds = dppub.getRun();

        eventRunTime.execute("create-run", ds);

        DataSet dspipe = dppub.getPipeline();

        eventRunTime.execute("p-create-run-pipelines", dspipe);

        DataSet dstask = dppub.getTasks();

        eventRunTime.execute("p-create-run-tasks", dstask);

        DataSet dsevents = dppub.GetEvents();

        eventRunTime.execute("p-load-events", dsevents);

    }



    // run as a service
    public void service() throws Exception
    {

        logger.info("====== Starting SERVICE ===== ");

        Task service = DPSystemFactory.newService(sysconf,this);

        service.execute();
    }

    public void dump()
    {

    }





}
