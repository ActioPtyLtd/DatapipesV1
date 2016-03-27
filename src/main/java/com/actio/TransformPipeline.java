package com.actio;

/**
 * Created by jim on 7/13/2015.
 */

import com.actio.dpsystem.DPFnNode;
import com.actio.dpsystem.DPSystemFactory;
import com.typesafe.config.*;

import java.util.List;

public class TransformPipeline extends TaskPipeline {

    static final String TRANSFORMS_LABEL = "transforms";

    public List<List<String>> getRows() {
        return rows;
    }

    public void setRows(List<List<String>> rows) {
        this.rows = rows;
    }

    private List<List<String>> rows = null;


    public TransformPipeline() {
    }


    public DataSet evokeTask(DPFnNode fnode, DataSet initDataSet) throws Exception {
        try {
            //initialise Task - at somepoint replace with a Factory
            TaskTransform t;
            logger.info("TransformPipeline:evokeTask:"+fnode.getName());

            t = DPSystemFactory.newTransform(fnode, sysconf);

            t.setDataSet(initDataSet);
            t.execute();

             //setDataSet( t.getDataSet() );

            return t.getDataSet();

        } catch (Exception e) {
            logger.info("Exception:"+e.toString());
        }

        return new DataSetTabular();
    }


    public void execute() throws Exception {
        processPipeLine(config, masterConfig);
    }


    private void processPipeLine(Config tasksConf, Config conf) throws Exception {

        /*
        String pipeLineString = null;
        if (tasksConf.hasPath(TRANSFORMS_LABEL) == true)
            pipeLineString = tasksConf.getString(TRANSFORMS_LABEL);
        else {
            logger.info("Label '"+TRANSFORMS_LABEL+"' not defined");
            return;
        }

        List<String> tasksInPipeline = GetSortedKeys(pipes.entrySet());
        DataSet resultSet = getDataSet();

        for (String key : tasksInPipeline) {
            logger.info("key=" + key);
            ConfigValue val = tasks.get(key);

            if (val.valueType() == ConfigValueType.OBJECT) {
                logger.info("Processing::" + val.render());

                resultSet = evokeTask( tasksConf, resultSet);
            }
        }

        setDataSet(resultSet);
        */
    }
}