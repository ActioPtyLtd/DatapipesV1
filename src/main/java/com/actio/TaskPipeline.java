package com.actio;

/**
 * Created by jim on 7/13/2015.
 */

import com.actio.dpsystem.DPFnNode;
import com.actio.dpsystem.DPSystemFactory;
import com.typesafe.config.ConfigObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.None$;

import java.io.InputStream;
import java.util.*;


public class TaskPipeline extends Task implements Runnable
{
    static final Logger logger = LoggerFactory.getLogger(TaskPipeline.class);
    protected ConfigObject pipes;
    protected ConfigObject tasks;
    String result = "";
    InputStream inputStream;


    //private String pipelineString;
    //private Map<String, Task> pipeline;
    private ArrayDeque<DataSet> pipelineResults = null;
    private List<String> sortedPipes;

    public TaskPipeline() {
        pipelineResults = new ArrayDeque<DataSet>();
    }

    public ArrayDeque<DataSet> getPipelineResults() {

        return pipelineResults;
    }

    public void setPipelineResults(ArrayDeque<DataSet> pipelineResults) {

        this.pipelineResults = pipelineResults;
    }

    public List<String> getSortedPipes() {
        return sortedPipes;
    }

    public void setSortedPipes(List<String> sortedPipes) {

        this.sortedPipes = sortedPipes;
    }

    public void extract() throws Exception {
        throw new Exception(FUNCTION_UNIMPLEMENTED_MSG);
    }


    public void load() throws Exception {
        throw new Exception(FUNCTION_UNIMPLEMENTED_MSG);
    }

    public void execute () throws Exception {
        processPipe();
    }


    private void processPipe() throws Exception {

        logger.info("Entered processPipe, name= '"+node.getName()+"'");
        sysconf.events.info(getInstanceID(), "", "START", "ProcessPipeline Started::", node.getName(), "", 0);
        try {
            // get the list of tasks for that pipeline
            LinkedList<DPFnNode> tasksInPipeline = node.getNodeList();

            // clear caches when the pipeline starts
            Cache.clear();
            DataSetCache.clear();

            if (dataSet != null)
                processPipeLineRE(tasksInPipeline, 0, dataSet);
            else
                processPipeLineRE(tasksInPipeline, 0, new DataSetTabular());

        } catch (Exception e) {
            logger.info("processPipeLine Exception::" + e.getMessage());
            sysconf.events.err(getInstanceID(), "", "ERROR", "ProcessPipeline Exception::" + e.getMessage(), node.getName(), "", 0);
        }
        sysconf.events.info(getInstanceID(), "", "FINISH", "ProcessPipeline Finished::", node.getName(), "", 0);
    }


    private void processPipeLineRE(LinkedList<DPFnNode> tasksInPipeline,
                                   int keyIndex,
                                   DataSet initDataSet) throws Exception {

        logger.info("Entered::processPipeLineRE:"+keyIndex);

        // check that there are tasks in the pipeline
        if (tasksInPipeline == null || tasksInPipeline.size() <= 0) {
            logger.debug("processPipeLineRE: No tasks in pipeline");
        }

        DPFnNode currentNode = tasksInPipeline.get(keyIndex++);

        logger.info("===== keyRE::" + currentNode.getName() +" ============== ");
        DataSet resultSet;

        if(currentNode.getType() == null)
            logger.error("Cannot retrieve task type, ensure the task exists and the type is specified");

        if (currentNode.getType().contains(PIPE_LABEL))
            resultSet = evokePipe(currentNode, initDataSet);
        else
            resultSet = evokeTask(currentNode, initDataSet);

        //dont think we need this. initalise straight awy resultSet.initBatch();

        logger.info("Calling Batch Recursively::"+currentNode.getName()+"("+keyIndex + "/"+tasksInPipeline.size()+")");
        if(resultSet.elems().hasNext()) {
            scala.collection.Iterator<DataSet> iterator = resultSet.elems();
            while (iterator.hasNext() && tasksInPipeline.size() > keyIndex) {
                DataSet subResultSet = iterator.next();
                // recursively traverse the rest of the pipeline batching up the ResultSet
                //subResultSet.dump();
                int reccount =  subResultSet.elems().size();
                logger.info("Processing::" + currentNode.getName() + "::size=" +reccount);
                sysconf.events.count(getInstanceID(), currentNode.getInstanceID(), "RecordCount", currentNode.getName(), "Records", reccount);
                for (DataSet iteratedDatSet : getSubDataSets(subResultSet, tasksInPipeline, keyIndex)) {
                    processPipeLineRE(tasksInPipeline, keyIndex, iteratedDatSet);

                    logger.info("Called Batch Recursively::" + currentNode.getName() + "::size=" + subResultSet.elems().size() +
                            "(" + keyIndex + "/" + tasksInPipeline.size() + ")");
                }
            }
        }
        else{
            if(tasksInPipeline.size() > keyIndex) {
                processPipeLineRE(tasksInPipeline, keyIndex, new DataSetTabular());
                logger.info("Called Batch Recursively::" + currentNode.getName() +
                            "(" + keyIndex + "/" + tasksInPipeline.size() + ")");
            }
        }

        logger.info("--------------------------------------Exit recursive "+
                currentNode.getName()+"  "+"---");
    }

    private List<DataSet> getSubDataSets(DataSet dataSet, LinkedList<DPFnNode> tasks, int keyIndex) throws Exception {
        return Collections.singletonList(dataSet);
    }

    private Boolean iterateOverDataSet(LinkedList<DPFnNode> tasks, int keyIndex) throws Exception {
        if(keyIndex <= tasks.size())
            return sysconf.getTask(tasks.get(keyIndex).getName()).toConfig().hasPath("iterate");

        return false;
    }

    //
    // ==============================================================================
    //
    private DataSet evokeTask(DPFnNode taskNode, DataSet initDataSet) throws Exception {


        try {
            //initialise Task - at somepoint replace with a Factory
            Task t = null;

            logger.info("TaskPipeline:evokeTask:");

            t = DPSystemFactory.newTask(sysconf, taskNode);

            // pass the prior results if there are any
            if (initDataSet != null)
                t.setDataSet(initDataSet);

            // ===========================================
            sysconf.events.info(getInstanceID(), t.getInstanceID(), "START", "Starting Task", t.node.getName(), "", 0);
            //events.info(t.getInstanceID(), "COUNT", "Processing Record Count", t.node.getName(), "Records", t.dataSet.elems().length());

            try {
                t.execute();
                //events.info(t.getInstanceID(), "COUNT", "Processing Record Count", t.node.getName(), "Records", t.dataSet.elems().length());
                sysconf.events.info(getInstanceID(), t.getInstanceID(), "FINISH", "Finishing Task", t.node.getName(), "", 0);

                return t.getDataSet();
            } catch (Exception e) {
                sysconf.events.err(getInstanceID(), t.getInstanceID(), e.toString(), e.getStackTrace()[0].toString(), t.node.getName(), "", 0);
                logger.error("evokeTask Exception:" + e.toString());
            }

        } catch (Exception e) {
            sysconf.events.err(getInstanceID(), null, e.toString(),e.getStackTrace()[0].toString(), node.getName(), "", 0);
            logger.error("evokeTask Exception:" + e.toString());
            // add system message
        }

        // return an empty set on an exception
        return new DataSetTabular();
    }


    private DataSet evokePipe(DPFnNode node, DataSet initDataSet) throws Exception
    {

        logger.info("Entered evokePipe name= '"+node.getName()+"'");

        // locate the actual compiled pipe this is just a stub.

        DPFnNode pipeNode = sysconf.resolveNode(node);

        try {
            Task t=null;
            // These are all parallel pipes for execution

            logger.info("====== execute Pipeline '"+
                    pipeNode.name+"'");
            //pipeNode.dump();

            // Instantiate the Node Function
            t = DPSystemFactory.newTask(sysconf,pipeNode);

            t.execute();

            return t.getDataSet();
        } catch (Exception e) {
            logger.info("processPipeLine Exception::" + e.getMessage());
        }

        // return empty dataset
        return new DataSetTabular();
    }


    //
    // ==============================================================================
    //

    public void run()
    {
        // enter Threaded class

        logger.info("=====Entered new thread ====================");

        // exit threaded class
        return;
    }

}