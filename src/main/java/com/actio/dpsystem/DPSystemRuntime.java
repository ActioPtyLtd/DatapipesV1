package com.actio.dpsystem;

import com.actio.DataSet;
import com.actio.DataSourceREST;
import com.actio.Task;
import com.actio.TaskService;
import com.actio.Data2Json;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

/**
 * Created by jim on 8/03/2016.
 */

/*
    Responsible for Engine for executing a pipeline and maintaining
    any state and services associated with a pipeline instance

 */

public class DPSystemRuntime extends DPSystemConfigurable {

    String configFile;
    Properties properties;

    public boolean initialisedCheck = false;

    public DPSystemRuntime(String configFile, Properties properties){
        this.configFile = configFile;
        this.properties = properties;
    }

    public DPSystemRuntime(String configFile){
        this.configFile = configFile;
        this.properties = null;
    }

    private final DPFnNode root = new DPFnNode("root", "root");
    // Collection of Instantiated Pipelines within the System
    private DPSystemConfig sysconf;
    public DPEventAggregator events = null;

    public DPSystemConfig getSysconf() {
        return sysconf;
    }

    public String getRunID() {

        return sysconf.events.runId();
    }


    public void execute() throws Exception
    {
        execute(null);
    }

    public void execute(String execName) throws Exception
    {
        execute(execName, null);
    }

    public void execute(String execName, DataSet ds) throws Exception {

        // setup runtime if it's not been done before calling execute
        if (!initialisedCheck){
            initRuntime();
        }

        // by default execute the pipe in exec
        List<DPFnNode> execpipes = sysconf.getExecutables(execName);

        // use factory to instantiate the runtime components

        if (execpipes.size() == 0) {
            logger.warn("No pipelines found to execute");
            return;
        }

        logger.info("***** RUN = '" + getRunID() + "' InstancedID='" + getInstanceID() + "'");

        // These are all parallel pipes for execution
        for (DPFnNode n : execpipes){

            // processing pipe

            logger.info("====== execute Pipeline '"+n.name+"'");
            n.dump();

            // add to the root node for tracking
            root.add(n);
            events.info("", "", "START", "Started DataPipes Runtime", "run", "", 0);
            // Instantiate the Node Function
            Task t = DPSystemFactory.newTask(sysconf,n);
            t.dataSet = ds;
            t.execute();
            events.info("", "", "FINISH", "Ending DataPipes Runtime", "run", "", 0);
            n.dump();
        }

        events.dump();

    }

    private Config loadConfig() throws IOException {
        if (configFile != null ) {
            return loadConfig(configFile, properties);
        } else {
            return config = ConfigFactory.load();
        }
    }

    // This is the binder from Token to actual Function
    // Refactor this into a proper Inversion of Control Bind

    public Config loadConfig(String configFile) throws IOException {
        return loadConfig(configFile, null);
    }

    public Config loadConfig(String _configFile, Properties _properties) throws IOException {
        this.configFile = _configFile;
        this.properties = _properties;
        if (configFile == null) {
            return this.loadConfig();
        }

        File myConfigFile = new File(configFile);

        if (!myConfigFile.exists())
            logger.error("File " + configFile + " does not exist.");

        Config fallBack = ConfigFactory.parseFile(myConfigFile);

        if(properties!=null) {
            config = ConfigFactory.parseProperties(properties).withFallback(fallBack).resolve();
        }
        else {
            config = fallBack.resolve();
        }

        return config;
    }

    //
    // Construct New Function Set Structure from config input
    //

    public void loadConfig(Config _config) throws IOException {
        config = _config;
        masterConfig = config;
    }

    private DPSystemConfig compileConfig(Config config) throws Exception {

        DPSystemConfig sysconf = new DPSystemConfig();

        sysconf.setConfig(config, config);

        sysconf.compile();

        // create top level task pipe
        return sysconf;
    }

    public void initRuntime() throws Exception {

        Config _conf = loadConfig();
        setConfig(_conf, _conf);

        sysconf = compileConfig(config);
        sysconf.dump();

        this.events = sysconf.events;

        initialisedCheck = true;

        return;
    }

    public void sendEvents() throws Exception {
        // going to instantiate a new factory & runtime
        // based upon the systemConfig to send the events out of the existing run

        String sysconfigfilename;

        if (getSysconf().getSystemConfig(SYSTEM_CONFIG) == null)
            sysconfigfilename = SYSTEM_CONFIG_FILE;
        else
            sysconfigfilename = getSysconf().getSystemConfig(SYSTEM_CONFIG).toString();

        logger.info("Sending Messages");

        DPSystemRuntime eventRunTime = new DPSystemRuntime(sysconfigfilename);
        eventRunTime.initRuntime();
        // dont have the event transmission process generate more events
        eventRunTime.events.disableEvents();

        // Generate the required DataSets
        DPEventPublisher dppub = new DPEventPublisher(this);

        DataSet ds = dppub.getRun();
        logger.info(Data2Json.toJsonString(ds));
        eventRunTime.execute(SYS_PIPE_CREATE_RUN, ds);

        DataSet dspipe = dppub.getPipeline();
        logger.info(Data2Json.toJsonString(dspipe));
        eventRunTime.execute(SYS_PIPE_CREATE_RUN_PIPE, dspipe);

        DataSet dstask = dppub.getTasks();
        logger.info(Data2Json.toJsonString(dstask));
        eventRunTime.execute(SYS_PIPE_CREATE_RUN_TASKS, dstask);

        DataSet dsevents = dppub.GetEvents();
        logger.info(Data2Json.toJsonString(dsevents));
        eventRunTime.execute(SYS_PIPE_LOAD_TASKS, dsevents);

    }

    // run as a service
    public void service() throws Exception
    {

        logger.info("====== Starting SERVICE ===== ");

        Task service = DPSystemFactory.newService(sysconf,this);

        service.execute();
    }

    public void dump()
    {

    }





}
