package com.actio;

import com.actio.dpsystem.DPFnNode;
import com.actio.dpsystem.DPSystemConfig;
import com.actio.dpsystem.DPSystemConfigurable;
import com.actio.dpsystem.DPEventAggregator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by jim on 7/8/2015.
 */
public abstract class Task extends DPSystemConfigurable {

    protected static final Logger logger = LoggerFactory.getLogger(Task.class);
    public Integer returnValue = 0;
    public DPFnNode node;
    public DPSystemConfig sysconf;

    public abstract void execute() throws Exception;

    public abstract void extract() throws Exception;

    public abstract void load() throws Exception;

    public void execute(String label)throws Exception
    {
        execute();
    }

    public void setNode(DPFnNode _node, DPSystemConfig _sysconf) throws Exception
    {

        sysconf = _sysconf;
        super.setConfig(sysconf.getConfig(),sysconf.getMasterConfig());
        node = _node;

        // create the instanceID for this task
        setInstanceID(getUUID());
        logger.info("---RUNID=" + sysconf.events.runId() + "----INSTANCEID=" + getInstanceID() + ".");
    }

}
