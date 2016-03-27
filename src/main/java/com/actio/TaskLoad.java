package com.actio;

import com.actio.dpsystem.DPFnNode;
import com.actio.dpsystem.DPSystemConfig;
import com.actio.dpsystem.DPSystemFactory;
import com.typesafe.config.Config;


/**
 * Created by jim on 7/8/2015.
 */

public class TaskLoad extends Task {

    private String type;
    private String behaviour;

    private DataSource diffLog;
    private DataSource destination;

    private DiffSet diff;


    private Boolean diffProcessAll = true;
    private Boolean diffProcessAdd = false;
    private Boolean diffProcessDel = false;
    private Boolean diffProcessOrig = false;
    private Boolean diffProcessRev = false;

    // =======================================================================================
    //
    //
    //
    // =======================================================================================

    public void setNode(DPFnNode _node, DPSystemConfig _sysconf) throws Exception
    {

        sysconf = _sysconf;
        super.setConfig(sysconf.getTaskConfig(_node.getName()).toConfig(),sysconf.getMasterConfig());
        node = _node;
        logger.debug("setNode::"+node.getName());


        // for mandatory fields we will throw the exception if not defined
        type = config.getString("type");

        behaviour = config.getString(BEHAVIOR_LABEL);

        // for some mandatory fields it's ok to throw an exception if not set

        // set the destination datasource
        if (config.hasPath(DATASOURCE_LABEL))
            destination =
                    DPSystemFactory.newDataSource(config.getConfig(DATASOURCE_LABEL),masterConfig);

        // set the Logging Datasource
        if (config.hasPath(DATASOURCELOG_LABEL))
            diffLog =
                    DPSystemFactory.newDataSource(config.getConfig(DATASOURCELOG_LABEL)
                            , masterConfig);

        if (config.hasPath(DIFF_PROCESS_ALL_LABEL))
            diffProcessAll = config.getBoolean(DIFF_PROCESS_ALL_LABEL);

        if (config.hasPath(DIFF_PROCESS_ADD_LABEL))
            diffProcessAdd = config.getBoolean(DIFF_PROCESS_ADD_LABEL);

        if (config.hasPath(DIFF_PROCESS_DEL_LABEL))
            diffProcessDel = config.getBoolean(DIFF_PROCESS_DEL_LABEL);

        if (config.hasPath(DIFF_PROCESS_ORIG_LABEL))
            diffProcessOrig = config.getBoolean(DIFF_PROCESS_ORIG_LABEL);

        if (config.hasPath(DIFF_PROCESS_REVISED_LABEL))
            diffProcessRev = config.getBoolean(DIFF_PROCESS_REVISED_LABEL);

        diff = new DiffSet();
    }


    public TaskLoad() {}


    public void execute() throws Exception {
        // for extractor default to extract
        load();
    }


    public void extract() throws Exception {
        throw new Exception(FUNCTION_UNIMPLEMENTED_MSG);
    }



    public void load() throws Exception {
        logger.info("Processes for BehaviourType=" + behaviour);
        // determine which subtask to deploy - at this time not going to subclass this
        if (behaviour.equals(CHECKPOINT_DIFF_LABEL)) {
            CheckPointDiff();
        }
        else if (behaviour.equals(FULL_CHECKPOINT_DIFF_LABEL)){
            ProcessAllDiffs();
        } else
            BasicQueryWrite();
    }

    private void BasicQueryWrite() throws Exception {
        // execute QueryParser
        logger.info("Process Basic Load");

        destination.write(getDataSet());
    }

    private void CheckPointDiff() throws Exception {
        logger.info("Process CheckPointDiff");

        // Get the Last log entry and load it's dataSet
        DataSet previousSet = diffLog.getLastLoggedDataSet();

        // Compare the previous set to the current set
        diff.trackDiffs(previousSet, getDataSet(), 0);

        // Write out all the sets

        // 1. Write out the new log
        diffLog.write(getDataSet());

        // 2. Load out  the differential results
        saveDiffFiles(diff);
    }


    // REFACTOR - reprocess all diff']]]]ds within a directory
    private void ProcessAllDiffs() throws Exception {

        throw new Exception("Not implemented");
    }


    private void saveDiffFiles(DiffSet tracker) throws Exception
    {
        logger.info("Entered saveDiffFiles");


        if (diffProcessAll)
            destination.write(tracker.getChangedList(),"_all");

        if (diffProcessAdd)
            destination.write(tracker.getAddList(), "_add");

        if (diffProcessDel)
            destination.write(tracker.getDelList(), "_del");

        if (diffProcessOrig)
            destination.write(tracker.getModList(true), "_mod_rev");

        if (diffProcessRev)
            destination.write(tracker.getModList(false), "_mod_org");
    }

}


