package com.actio;

import com.actio.dpsystem.DPFnNode;
import com.actio.dpsystem.DPSystemConfig;
import com.actio.dpsystem.DPSystemFactory;
import com.typesafe.config.Config;


/**
 * Created by jim on 7/8/2015.
 *
 * @voice33
 */

public class TaskExtract extends Task {

    private DataSource dataSource;
    private String type;
    private String behaviour;
    protected DataSet dataSet;

    public TaskExtract() {

    }

    public void extract() throws Exception
    {
        logger.info("Processes for READER");

        dataSource.extract();

        setDataSet(dataSource.getDataSet());

        //getData().dump();

    }


    public void execute() throws Exception {
        // for extractor default to extract
        extract();
    }


    public void load() throws Exception {
       throw new Exception(FUNCTION_UNIMPLEMENTED_MSG);
    }


    // ==========================================================================

    // ==========================================================================
    public void setNode(DPFnNode _node, DPSystemConfig _sysconf) throws Exception
    {


        sysconf = _sysconf;
        super.setConfig(sysconf.getTaskConfig(_node.getName()).toConfig(),sysconf.getMasterConfig());
        node = _node;
        logger.debug("setNode::"+node.getName());

        // for mandatory fields we will throw the exception if not defined
        type = config.getString("type");

        // for some mandatory fields it's ok to throw an exception if not set
        if (config.hasPath(BEHAVIOR_LABEL))
            behaviour = config.getString(BEHAVIOR_LABEL);

        // set the destination datasource
        if (config.hasPath(DATASOURCE_LABEL))
            dataSource = DPSystemFactory.newDataSource(config.getConfig(DATASOURCE_LABEL),masterConfig);
    }

}


