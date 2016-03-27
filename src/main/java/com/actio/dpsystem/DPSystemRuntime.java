package com.actio.dpsystem;

import com.actio.Configurable;
import com.actio.Task;

import java.util.List;

/**
 * Created by jim on 8/03/2016.
 */

/*
    Responsible for Engine for executing a pipeline and maintaining
    any state and services associtated with a pipeline instance

 */

public class DPSystemRuntime extends Configurable {

    // Collection of Instantiated Pipelines within the System
    private DPSystemConfig sysconf;
    private final DPFnNode root = new DPFnNode("root","root");


    public void setRuntimeConfig(DPSystemConfig sysConf)
    {
        this.sysconf = sysConf;
    }

    public void execute() throws Exception
    {
        execute(null);

    }

    private void execute(String execName) throws Exception
    {

        // by default execute the pipe in exec
        List<DPFnNode> execpipes = sysconf.getExecutables(execName);

        // use factory to instantiate the runtime components

        if (execpipes.size() == 0) {
            logger.warn("No pipelines found to execute");
            return;
        }


        // These are all parallel pipes for execution
        for (DPFnNode n : execpipes){

            // processing pipe

            logger.info("====== execute Pipeline '"+n.name+"'");
            n.dump();

            // add to the root node for tracking
            root.add(n);

            // Instantiate the Node Function
            Task t = DPSystemFactory.newTask(sysconf,n);

            t.execute();

        }
        
    }

    public void dump()
    {

    }

}
