package com.actio.dpsystem;

/**
 * Created by jim on 7/13/2015.
 */

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import com.actio.*;
import com.typesafe.config.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/*

Used to bootstrap Task system and return Task Instances from Configuration
Should hide ALL the nasty Instantiation of specific Classes

*/

public class DPSystemFactory extends DPSystemConfigurable {

    private static final Logger logger = LoggerFactory.getLogger(DPSystemFactory.class);

    String result = "";
    InputStream inputStream;

    public List<String> getSortedConfigItems() {
        return sortedConfigItems;
    }

    public void setSortedConfigItems(List<String> sortedConfigItems) {
        this.sortedConfigItems = sortedConfigItems;
    }

    private List<String> sortedConfigItems;
    private DPSystemConfig sysconf;

    public DPSystemFactory() {
    }

    private void loadConfig() throws IOException
    {
        config = ConfigFactory.load();
        masterConfig = config;
    }

    public void loadConfig(String configFile) throws IOException {

        if (configFile == null){
            this.loadConfig();
            return;
        }

        File myConfigFile = new File(configFile);
        config = ConfigFactory.parseFile(myConfigFile);
        masterConfig = config;

    }

    private DPSystemConfig compileConfig() throws Exception {

        sysconf = new DPSystemConfig();

        sysconf.setConfig(config,masterConfig);

        sysconf.compile();

        // create top level task pipe
        return sysconf;
    }


    public DPSystemRuntime newRuntime() throws Exception
    {
        DPSystemConfig dpipeConfig = compileConfig();
        dpipeConfig.dump();

        DPSystemRuntime dprun = new DPSystemRuntime();
        dprun.setConfig(config, masterConfig);

        dprun.setRuntimeConfig(dpipeConfig);

        return dprun;
    }


    // =====================================================
    //

    static public Task newTask(DPSystemConfig sysconf, DPFnNode node) throws Exception
    {
        Task t;
        String name = node.getName();
        String type = sysconf.getType(node);

        logger.info(" newTask::  Factory Found Type=" + type);
        switch (type){
            case TASK_EXTRACT_LABEL :
                t = new TaskExtract();
                break;
            case TASK_LOAD_LABEL:
                t = new TaskLoad();
                break;
            case TASK_TRANSFORM_LABEL:
                t = new TaskTransform();
                break;
            case PIPE_LABEL:
                t = new TaskPipeline();
                break;
            default:
                throw new Exception("DPSystemFactory:Unknown Task Type "+type);
        }

        t.setNode(node, sysconf);

        return t;
    }



    // =====================================================

    static public DataSourceREST newDataSourceREST(String name, DPSystemConfig sysconf)
            throws Exception
    {
        logger.info("newDataSourceREST::name="+name);
        DataSourceREST dsr = new DataSourceREST();

        dsr.setConfig(sysconf.getServices(name).toConfig() ,sysconf.getMasterConfig());

        return dsr;
    }

    static public DataSource newDataSource(Config config, Config masterConfig) throws Exception
    {
        DataSource ds;
        String type;

        type = config.getString(TYPE_LABEL);
        ds = newDataSource(type);
        ds.setConfig(config, masterConfig);

        return ds;
    }

    private static DataSource newDataSource(String type) throws Exception
    {
        DataSource t = null;
        logger.info("   Factory Found Type=" + type);
        switch (type){
            case FILE_LABEL:
                t = new DataSourceFile();
                break;
            case SQL_LABEL:
                t = new DataSourceSQL();
                break;
            case REST_LABEL:
                t = new DataSourceREST();
                break;
            default:
                throw new Exception("DPSystemFactory:Unknown Task Type "+type);
        }
        return t;
    }

    // =====================================================


    // REFACTOR NOTE - currently deal with a single sqlquery
    // in the future will want to be able to deal with a collection of
    // queries, that fulfill an API definition

    static public QueryParser newQuery(String type, Config config, Config masterConfig)
            throws Exception
    {

        QueryParser t = null;

        if (config.hasPath(QUERY_LABEL)) {
            t = newQuery(type);
            t.setConfig(config.getConfig(QUERY_LABEL), masterConfig);
        }

        return t;
    }

    private static QueryParser newQuery(String type) throws Exception
    {
        QueryParser t = null;
        logger.info("   Factory Found Type=" + type);
        switch (type){
            case SQL_LABEL :
                t = new QueryParserSQL();
                break;
            case REST_LABEL:
                t = new QueryParserRest();
                break;
            case WEBSERVICE_LABEL:
                t = new QueryParserWebService();
                break;
            default:
                throw new Exception("Unknown Task Type "+type);
        }

        return t;
    }

    // =====================================================    //
    // Locate Task Configuration by taskLabelName
    //

    static public Task newTaskByName(Config config, Config masterConfig, String taskName)
            throws Exception
    {
        Task t = null;

        // ===============================================
        return t;
    }



    static public TaskTransform newTransform(DPFnNode node, DPSystemConfig sysconf)
            throws Exception
    {
        TaskTransform t = null;
        String type = "lineValidation";

        Config taskConfig = sysconf.getTask(node.name).toConfig();

        if (taskConfig.hasPath(BEHAVIOR_LABEL))
            type = taskConfig.getString(BEHAVIOR_LABEL);


        logger.info("   Factory Found Type=" + type);
        switch (type){
            case "mergeTemplate" :
                t = new TransformTemplate();
                break;
            case "lineValidation":
                t = new TaskTransform();
                break;
            case "replace":
                t = new TaskTransform();
                break;
            case "phoneformat_au":
                t = new TaskTransform();
                break;
            default:
                throw new Exception("Unknown Task Type "+type);
        }

        t.setNode(node,sysconf);
        return t;
    }

    // This is the binder from Token to actual Function
    // Refactor this into a proper Inversion of Control Bind

    public static String CallFunction(TransformFunction func, String param)
    {
        // switch statement of functions to call
        String newField = null;

        if (func.getName().contentEquals("getDate")){
            // call get date function with params

            newField = StaticUtilityFunctions.getDate(func);
        }
        else if (func.getName().contentEquals("replace")){

            newField = "**Replace**";

        }
        else {
            // unknown function

            newField = "*****unknown_fn*****";
        }

        return newField;
    }

    //
    // Construct New Function Set Structure from config input
    //

    public static CompiledTemplateFunctionSet newFunctionSet(Config config)
    {
        CompiledTemplateFunctionSet newFuncSet = new CompiledTemplateFunctionSet();

        // process for column functions
        if (config.hasPath(COLUMN_LABEL) == true) {
            // loop over columns creating compiled function structures
            Map<String, Object> theMap = config.getObject(COLUMN_LABEL).unwrapped();
            Config columnConfig = config.getObject(COLUMN_LABEL).toConfig();

            for (String key : theMap.keySet()) {
                logger.info("Processing for Key=" + key);

                // get an array of functions to be applied to that key

                List<String> rawFunctionList = columnConfig.getStringList(key);

                newFuncSet.addFunctions(key, buildTransformFunctionsFromList(rawFunctionList));
            }
        }

        // process for global functions add a custom entry for GLOBAL Functions using a custom label
        if (config.hasPath(GLOBAL_FUNCTIONS_LABEL) == true)
        {
            List<String> rawFunctionList = config.getStringList(GLOBAL_FUNCTIONS_LABEL);
            newFuncSet.addFunctions(GLOBAL_FUNCTIONS_KEY, buildTransformFunctionsFromList(rawFunctionList));
        }

        // process for row functions add a custom entry for LOCAL Functions using a custom label
        if (config.hasPath(ROW_FUNCTIONS_LABEL) == true)
        {
            List<String> rawFunctionList = config.getStringList(ROW_FUNCTIONS_LABEL);
            newFuncSet.addFunctions(ROW_FUNCTIONS_KEY, buildTransformFunctionsFromList(rawFunctionList));
        }

        newFuncSet.dump();

        return newFuncSet;
    }


    static private List<TransformFunction> buildTransformFunctionsFromList(List<String> rawFunctionList)
    {
        List<TransformFunction> tfl = new LinkedList<>();

        // following functions defined for given key
        StaticUtilityFunctions.DumpList(rawFunctionList);

        // create a FunctionList
        for (String raw : rawFunctionList) {
            TransformFunction func = new TransformFunction();
            func.initByRaw(raw);
            tfl.add(func);
        }

        return tfl;
    }

    static public TaskService newService(DPSystemConfig sysconf) throws Exception
    {
        // locate Service Configuration
        TaskService ts = new TaskService();
        ts.setNode(sysconf);

        return ts;
    }


}