package com.actio.dpsystem;

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
    public DPEventAggregator events = null;
    // Collection of Instantiated Pipelines within the System
    private DPSystemConfig sysconf;

    public void setRuntimeConfig(DPSystemConfig sysConf)
    {

        this.sysconf = sysConf;
    }

    public void execute() throws Exception
    {
        execute(null);
    }

    public void execute(String execName) throws Exception
    {
        // set the guid for this runtime
        setRunID(getUUID());
        sysconf.setRunID(getRunID());

        events = new DPEventAggregator(getRunID());
        events.setConfig(getConfig(), getMasterConfig());

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
            events.addEvent(getInstanceID(), "START", "Started DataPipes Runtime", n.getName());
            // Instantiate the Node Function
            Task t = DPSystemFactory.newTask(sysconf,n);

            t.execute();
            events.addEvent(getInstanceID(), "END", "Ending DataPipes Runtime", n.getName());
        }


        events.dump();
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
