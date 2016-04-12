package com.actio;

import com.actio.dpsystem.DPFnNode;
import com.actio.dpsystem.DPSystemConfig;
import com.actio.dpsystem.DPSystemConfigurable;

/**
 * Created by jim on 7/8/2015.
 */
public abstract class Task extends DPSystemConfigurable {

    public abstract void execute() throws Exception;
    public abstract void extract() throws Exception;
    public abstract void load() throws Exception;

    DPFnNode node;
    DPSystemConfig sysconf;

    public void execute(String label)throws Exception
    {
        execute();
    }

    public void setNode(DPFnNode _node, DPSystemConfig _sysconf) throws Exception
    {
        sysconf = _sysconf;
        super.setConfig(sysconf.getConfig(),sysconf.getMasterConfig());
        node = _node;
    }


}
