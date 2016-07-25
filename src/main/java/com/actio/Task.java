package com.actio;

import com.actio.dpsystem.DPFnNode;
import com.actio.dpsystem.DPSystemConfig;
import com.actio.dpsystem.DPSystemConfigurable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by jim on 7/8/2015.
 */
public abstract class Task extends DPSystemConfigurable {

    protected static final Logger logger = LoggerFactory.getLogger(Task.class);
    DPFnNode node;
    DPSystemConfig sysconf;
    private String runID;
    private String instanceID;

    public String getRunID() {
        return runID;
    }

    public void setRunID(String runID) {
        this.runID = runID;
    }

    public String getInstanceID() {
        return instanceID;
    }

    public void setInstanceID(String instanceID) {
        this.instanceID = instanceID;
    }

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
        setRunID(_sysconf.getRunID());
        setInstanceID(getUUID());

        logger.info("---RUNID=" + getRunID() + "----INSTANCEID=" + getInstanceID() + ".");
    }

}
