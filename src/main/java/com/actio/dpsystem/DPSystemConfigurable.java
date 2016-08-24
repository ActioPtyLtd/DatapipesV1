package com.actio.dpsystem;

import com.actio.DataSet;
import com.actio.DataSetTabular;
//import com.jcabi.aspects.Loggable;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;

/**
 * Created by dimitarpopov on 24/08/15.
 */

//@Loggable(Loggable.DEBUG)
public class DPSystemConfigurable {

    /** File separator. */
    public static final String FS = File.separator;

    // Basic Constants
    public static final String BOUNDARY_TOKENS = "@@";
    // Configuration Labels
    public static final String PIPELINE_LABEL = "pipelines";
    public static final String SCHEMA_LABEL = "schema";
    public static final String SERVICES_LABEL = "services";
    public static final String STARTUP_EXECS_LABEL = "startup";
    public static final String SCHEDULED_LABEL = "scheduled";
    public static final String EXEC_PIPENAME_LABEL = "exec";
    public static final String PIPE_LABEL = "pipe";
    public static final String TASKS_LABEL = "tasks";
    public static final String SCRIPT_LABEL = "script";
    public static final String TYPE_LABEL = "type";
    public static final String DATASOURCE_LABEL = "dataSource";
    public static final String DATASOURCELOG_LABEL = "dataSourceLog";
    public static final String DIRECTORY_LABEL = "directory";
    public static final String FILE_LABEL = "file";
    public static final String SQL_LABEL = "sql";
    public static final String REST_LABEL ="rest";
    public static final String FILENAME_LABEL = "filename";
    public static final String FILENAME_TEMPLATE_LABEL = "filenameTemplate";
    public static final String BEHAVIOR_LABEL = "behavior";
    public static final String QUERY_LABEL = "query";
    public static final String QUERY_TEMPLATE_LABEL = "queryTemplate";
    public static final String MERGE_TEMPLATE_LABEL="mergeTemplate";
    public static final String FILENAME_DATEDAY_OFFSET_LABEL = "dateDayOffset";
    public static final String DEFAULT_FILENAME_LABEL = "defaultLog_@datetime";
    public static final String TASK_EXTRACT_LABEL = "extract";
    public static final String TASK_LOAD_LABEL = "load";
    public static final String TASK_TRANSFORM_LABEL = "transform";
    public static final String TASK_PIPELINE_LABEL = "pipeline";
    public static final String ATTRIBUTE_LABEL = "attribute";
    // types of behavior
    public static final String CHECKPOINT_DIFF_LABEL="checkpointDiff";
    public static final String FULL_CHECKPOINT_DIFF_LABEL="fullCheckpointDiff";
    public static final String BASIC_LOAD_LABEL="basic";
    public static final String DIFF_PROCESS_ALL_LABEL="diffProcessAll";
    public static final String DIFF_PROCESS_ADD_LABEL="diffProcessAdd";
    public static final String DIFF_PROCESS_DEL_LABEL="diffProcessDel";
    public static final String DIFF_PROCESS_ORIG_LABEL="diffProcessOrig";
    public static final String DIFF_PROCESS_REVISED_LABEL="diffProcessRev";
    public static final String VALIDATE_PHONE_COLUMNS_LABEL = "validatePhoneColumns";
    public static final String DATEDAYOFFSET_LABEL = "dateDayOffset";
    public static final String READ_JSON_FROM_FILE = "readJsonFromFile";
    // QueryParser Substitution LABELS
    public static final String DATE_SUBSTITUTION_LABEL = "@yesterdaysDate";
    public static final String DATE_SUBSTITUTION_WITH_OFFSET_LABEL = "@yesterdaysDateWithOffset";
    public static final String FUNCTION_UNIMPLEMENTED_MSG = "Function Not Implemented";
    public static final String WEBSERVICE_LABEL = "webservice";
    public static final String JDBC_DRIVER_LABEL = "jdbcDriver";
    public static final String COLUMN_LABEL = "columns";
    public static final String BATCH_FUNCTIONS_LABEL = "batch";
    public static final String ROW_FUNCTIONS_LABEL = "row";
    public static final String BATCH_FUNCTIONS_KEY = "****batch****";
    public static final String ROW_FUNCTIONS_KEY = "****row****";
    protected static final Logger logger = LoggerFactory.getLogger(DPSystemConfigurable.class);
    /**
     * The base resource path.
     */
    public static String BASE_PATH = "src" + FS + "test" + FS + "resources";
    public DataSet dataSet;
    public Config config;
    public Config masterConfig;
    public DPEventAggregator events = null;
    private String runID;
    private String instanceID;

    static public boolean matchLabel(String value, String label) {

        /// comment

        if (value.toUpperCase().contains(label.toUpperCase()) == true)
            return true;
        else
            return false;
    }

    static public boolean isInteger(String s, int radix) {
        Scanner sc = new Scanner(s.trim());
        if (!sc.hasNextInt(radix)) return false;
        // we know it starts with a valid int, now make sure
        // there's nothing left!
        sc.nextInt(radix);
        return !sc.hasNext();
    }

    public static String getUUID() {
        return UUID.randomUUID().toString();
    }

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

    public DataSet getDataSet() {

        // return a valid empty dataset if null
        if (dataSet == null)
            return dataSet = new DataSetTabular();
        else
            return dataSet;
    }

    public void setDataSet(DataSet dataSet) {
        this.dataSet = dataSet;
    }

    public Config getConfig() {
        return config;
    }

    public void setConfig(Config config) {
        this.config = config;
    }

    public Config getMasterConfig() {
        return masterConfig;
    }

    public void setMasterConfig(Config masterConfig) {
        this.masterConfig = masterConfig;
    }

    public void setConfig(Config _conf, Config _master) throws Exception
    {
        config = _conf;
        masterConfig = _master;


        events = DPSoleAggregator.events;

        setRunID(events.runId());
        setInstanceID(getUUID());
        logger.info("***********---RUNID=" + getRunID() + "----INSTANCEID=" + getInstanceID() + ".");

    }

    // =======================================================================================
    // why is this function here - REFACTOR into standard library
    //
    //
    // =======================================================================================

    // @Loggable

    // This function is used to return the Pipeline Names in sorted order
    public List<String> GetSortedKeys(Set<Map.Entry<String, ConfigValue>> confSet) {
        List<String> keyList = new LinkedList<String>();
        for (Map.Entry<String, ConfigValue> data : confSet) {
            keyList.add(data.getKey());
        }
        //Collections.sort(keyList);

        keyList.sort((s1, s2) -> s1.compareTo(s2));
        keyList.forEach(s -> logger.info("sorted key=" + s));

        return keyList;
    }
}
