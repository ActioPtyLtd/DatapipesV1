package com.actio.dpsystem;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigObject;
import com.typesafe.config.ConfigValue;
import com.typesafe.config.ConfigValueType;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Created by jim on 3/03/2016.
 */


/*
    DPSystem : Data Pipe System class is responsible for

     0. The System Configurations
     1. holding the Compiled Data Pipe Definition

 */

public class DPSystemConfig extends DPSystemConfigurable {

    //  pipeline name mapping to compiled parse tree
    public final Map<String, DPFnNode> pipelinesMap = new HashMap<>();
    private final Map<String, DPFnNode> servicesMap = new HashMap<>();
    private ConfigObject script;
    private ConfigObject schema;
    private ConfigObject pipes;
    private ConfigObject tasks;
    private ConfigObject services ;
    private ConfigObject execs;
    private ConfigObject scheduled;
    private ConfigObject system;

    public DPEventAggregator events = null;

    //  task name mapping to compiled task - TBD when we compileConfig tasks

    private ConfigObject setconfigsection(ConfigObject conf, String label, Boolean mandatory) throws Exception{
        ConfigObject co;

        if (conf.toConfig().hasPath(label) == true )
            co  = conf.toConfig().getObject(label);
        else
            co = null;

        if (co == null)
            if (mandatory == true)
                throw new Exception("Config does not contain a valid definition for::"+label);
            else
                logger.info("Warning: Config does not contain a valid definition for::"+label);

        return co;
    }

    public void setConfig(Config conf, Config master) throws Exception
    {
        setInstanceID(getUUID());
        logger.info("----INSTANCEID=" + getInstanceID() + ".");
        events = new DPEventAggregator(getInstanceID());
        super.setConfig(conf,master);
        script  = setconfigsection(master.root(),SCRIPT_LABEL,true);
        schema = setconfigsection(script,SCHEMA_LABEL,false);
        pipes = setconfigsection(script,PIPELINE_LABEL,true);
        tasks = setconfigsection(script,TASKS_LABEL,true);
        services = setconfigsection(script,SERVICES_LABEL,false);
        execs = setconfigsection(script, STARTUP_EXECS_LABEL,false);
        scheduled = setconfigsection(script,SCHEDULED_LABEL,false);
        system = setconfigsection(script, SYSTEM_LABEL, false);
    }

    public String getConfigName() {

        try {
            // check system section for a config name
            String configName = getSystemConfig(CONFIG_NAME);

            if (configName != null) {
                // otherwise use filename
                return configName;
            }
        } catch (Exception e){
            logger.warn("getSystemConfig Failed="+CONFIG_NAME);
        }
        return null;
    }

    public ConfigObject getTaskConfig(String name) throws Exception {
        if (tasks.containsKey(name))
        {
            return tasks.toConfig().getObject(name);
        }
        else {
            logger.warn("getTaskConfig:: Task not found:" + name);
            return null;
        }
    }

    public String getSystemConfig(String name) throws Exception {
        if (system == null)
            return null;
        else if (system.containsKey(name)) {
            return system.toConfig().getString(name);
        } else {
            logger.warn("getSystemConfig:: System item not found:" + name);
            return null;
        }
    }

    public ConfigObject getPipeConfig(String name) throws Exception {
        if (pipes.containsKey(name))
        {
            return pipes.toConfig().getObject(name);
        }
        else {
            logger.warn("getPipeConfig:: Pipe not found:" + name);
            return null;
        }
    }

    public ConfigObject getServices() throws Exception
    {
        if (services != null)
            return services;


        throw new Exception("getServices::SERVICES Section undefined in Config");

    }

    public ConfigObject getServices(String name) throws Exception
    {
        if (services == null || !services.containsKey(name))
            throw new Exception("getServices::SERVICES Section undefined in Config");

        return services.toConfig().getObject(name);
    }


    public String getType(String name) throws Exception
    {
        // look up first to see if its a task label
        ConfigObject conf = getTaskConfig(name);

        if (conf != null) {
            if (conf.containsKey(TYPE_LABEL))
                return conf.toConfig().getString(TYPE_LABEL);
            else
                logger.warn("getType:: Mandatory type label not found" + name);
        }else {
            // otherwise look to see if it's a pipeline label
            conf = getPipeConfig(name);
            if (conf!=null)
                return DPSystemConfigurable.PIPE_LABEL;
        }
        return null;
    }

    public String getType(DPFnNode node) throws Exception
    {
        // special case pipe task doesnt have a config
        if (node.getType().contains(PIPE_LABEL))
            return PIPE_LABEL;

        // look up the task
        ConfigObject taskConf = getTaskConfig(node.getName());

        if (taskConf.containsKey(TYPE_LABEL))
            return taskConf.toConfig().getString(TYPE_LABEL);
        else
            logger.warn("getType:: Mandatory type label not found"+node.getName());
        return null;
    }

    public ConfigObject getNodeConfig(String name, String type)
    {
        try {
            if (type.contains(DPSystemConfigurable.PIPE_LABEL))
                return getPipeConfig(name);
            else
                return getTaskConfig(name);
        } catch (Exception e){
            // very bad could not find a config definition for name label combo
            logger.error("Could Not find a Config Definiton for ="+name+" of type ="+type);
        }
        return null;
    }

    //
    // Compile all pipelines into a parse tree
    //

    public void compilePipes() throws Exception{
        // sort the collection
        List<String> sortedPipes = GetSortedKeys(pipes.entrySet());
        for (String key : sortedPipes) {
            try {
                ConfigValue val = pipes.get(key);
                String pipeLineString = null;

                if (val.valueType() == ConfigValueType.OBJECT) {
                    logger.info("Pipeline (" + key + ") ");
                    // extended attributes for pipeline object

                    Config tasksConf = ((ConfigObject) val).toConfig();

                    if (tasksConf.hasPath(PIPE_LABEL) != true) {
                        logger.info("Label '" + PIPE_LABEL +
                                "' not defined for pipe==" + key);
                        continue;
                    }
                    pipeLineString = tasksConf.getString(PIPE_LABEL);
                } else {
                    pipeLineString = pipes.toConfig().getString(key);
                }

                DPFnNode node = compileDataPipe(key,pipeLineString);
                // set the root names node to the name of the pipeline, not 'pipe' as set by the parser
                //node.name = key;
                // Check for any other Pipeline variables to be added to the node
                pipelinesMap.put(key,node);

            } catch (Exception e) {
                logger.info("processPipeLine '"+key+
                        "' Exception::" + e.getMessage());
            }
        }


    }


    public void compile() throws Exception {
        logger.info("Entered full compileConfig");
        // for now pipes need to be precompiled, loop over pipe section
        compilePipes();
        compileServices();
        compileTasks();
    }


    public void compileServices() throws Exception
    {
        // TODO:build the services node -


        return;
    }

    public void compileTasks()
    {
        // no op
        return;
    }


    // ==================================================================================================

    private DPFnNode compileDataPipe(String name, String dpiperaw) throws Exception
    {
        DPLangTokens lt = new DPLangTokens();
        //lt.setConfig(config, masterConfig);

        lt.tokeniseBrute(dpiperaw);

        return DPLangParser.parse(name, lt, this);
    }

    // ================================================================


    public void execute() throws Exception
    {
        compile();
    }

     // ================================================================

    // Rules for locating an executable Pipeline by name
    // or Default

    public List<DPFnNode> getExecutables(String name)
    {
        List<DPFnNode> list = new LinkedList<>();
        String pipeName = name;

        if (execs != null && name == null) {
            // locate exec section if default pipe specified
            pipeName = execs.toConfig().getString(EXEC_PIPENAME_LABEL);
        }

        if (pipeName != null)
        {
            // locate the pipeline, lookup
            if (pipelinesMap.containsKey(pipeName))
                list.add(pipelinesMap.get(pipeName));
        }
        else {
            // NO Op list remains empty
        }

        return list;
    }


    // Rules for locating an executable Pipeline by name
    // 1. node is either a named STUB referencing a compiled Pipeline
    // 2. OR node is an anonymous PIPELINE which is already compiled

    public DPFnNode resolveNode(DPFnNode node)
    {
        // if anonymous pipeline then just return the node
        if (node.getName().contains(PIPE_LABEL) && node.getType().contains(PIPE_LABEL))
            return node;

        // locate the actual compiled node

        return pipelinesMap.get(node.getName());
    }


    // ================================================================


    public ConfigObject getTaskList() throws Exception
    {
        return tasks;
    }


    // ================================================================


    public ConfigObject getTask(String name) throws Exception
    {
        // locate task by name-
        ConfigValue val = tasks.get(name);
        if (val.valueType() == ConfigValueType.OBJECT) {
            return (ConfigObject) val;
        }
        else
        {
            logger.warn("task name not found ='"+name+"'");
        }

        return null;
    }

    public List<String> getServicesNames(){
       return GetSortedKeys(services.entrySet());
    }


    // ================================================================

    public void dump()
    {

        logger.info("Total Compiled Pipelines == "+
                pipelinesMap.size());

        for (String key : pipelinesMap.keySet())
        {
            // dump compiled map
            System.out.println("\n---------------------------------------------------------");

            String cfg = pipes.toConfig().getObject(key).toConfig().getString(PIPE_LABEL);

            System.out.println("dpipe script '"+key+" : "+cfg+"'");
            System.out.print("json => '");
            pipelinesMap.get(key).dump();
            System.out.println("'");
        }


    }

}
