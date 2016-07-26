package com.actio;

import com.actio.dpsystem.DPSystemFactory;
import com.actio.dpsystem.DPSystemRuntime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;
import com.actio.dpsystem.DPFnNode;
import com.actio.dpsystem.DPSystemConfig;
import spark.Spark.*;
import spark.Spark;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by jim on 7/8/2015.
 */

// Service is an Async version of Pipeline

public class TaskService extends TaskPipeline {

    static final Logger logger = LoggerFactory.getLogger(TaskService.class);

    private DPSystemRuntime sysruntime;

    private Map<String, DataSourceREST> pathList = new HashMap<>();


    public void extract() throws Exception
    {
        execute();
    }

    public void load() throws Exception
    {
        execute();
    }

    public void execute() throws Exception
    {
        execute(null);
    }


    public void execute(String label)throws Exception
    {
        // start up a service

        registerPaths();
        startService();

    }

    public void setNode(DPFnNode _node, DPSystemConfig _sysconf) throws Exception
    {
        sysconf = _sysconf;
        super.setConfig(sysconf.getTaskConfig(_node.getName()).toConfig(),sysconf.getMasterConfig());
        node = _node;
        logger.debug("setNode::"+node.getName());

        // locate service Configuration
        setInstanceID(getUUID());
        logger.info("---RUNID=" + getRunID() + "----INSTANCEID=" + getInstanceID() + ".");
    }

    public void setNode(DPSystemConfig _sysconf, DPSystemRuntime runtime) throws Exception
    {
        sysconf = _sysconf;
        sysruntime = runtime;
        super.setConfig(sysconf.getServices().toConfig(),sysconf.getMasterConfig());

        logger.debug("setNode::Services");

        // locate service Configuration
        setInstanceID(getUUID());
        logger.info("---RUNID=" + getRunID() + "----INSTANCEID=" + getInstanceID() + ".");
    }

    // call back switches

    public void registerPaths() throws Exception
    {
        // iterate over services
        List<String> services = sysconf.getServicesNames();


        for (String serviceName : services){

            DataSourceREST datars = DPSystemFactory.newDataSourceREST(serviceName,sysconf);
            // Register datars
            pathList.put(datars.route(), datars);

            registerPathsByFunction(datars);
        }
    }

    private void registerPathsByFunction(DataSourceREST datars) throws Exception
    {
        // setup a generic get function
        if (datars.getfn() != null) {
            logger.debug(" --- Registering Route::get = "+datars.route());
            Spark.get(datars.route(), (request, response) -> {
                return getHandler(request, response);
            });
        }

        if (datars.postfn() != null) {
            logger.debug(" --- Registering Route::post = "+datars.route());
            Spark.post(datars.route(), (request, response) -> {
                return getHandler(request, response);
            });
        }

        if (datars.putfn() != null) {
            logger.debug(" --- Registering Route::get = "+datars.route());
            Spark.put(datars.route(), (request, response) -> {
                return getHandler(request, response);
            });
        }

    }

    public void startService()
    {
        logger.info("=========== Start Server =========");

    }

    public String getHandler(Request request, Response response) throws Exception {
        logger.info("getHandler Called a get ON::"+request.pathInfo());
        logger.info("Called a get ON::"+request.url());

        // pass body as a DS


        // access to pathlist
        DataSourceREST datars = pathList.get(request.pathInfo());

        sysruntime.execute(datars.getfn());

        return "getHandler-- Calling::"+datars.getfn();
    }

    public String postHandler(Request request, Response response)  throws Exception{
        logger.info("postHandler Called a post ON::"+request.pathInfo());
        logger.info("Called a post ON::"+request.url());
        logger.info("dump::"+request.raw());
        // access to pathlist
        DataSourceREST datars = pathList.get(request.pathInfo());
        return "Hello World -- Calling::"+datars.postfn();

    }

    public String putHandler(Request request, Response response)  throws Exception {
        logger.info("Called a put ON::"+request.pathInfo());
        logger.info("Called a put ON::"+request.url());
        logger.info("dump::"+request.raw());
        // access to pathlist
        DataSourceREST datars = pathList.get(request.pathInfo());
        return "Hello World -- Calling::"+datars.putfn();
    }

    public void deleteHandler(Request request, Response response)  throws Exception{

    }

    public void headHandler(Request request, Response response)  throws Exception{

    }

    public void traceHandler(Request request, Response response)  throws Exception{

    }

    public void optionsHandler(Request request, Response response)  throws Exception{

    }

    public void connectHandler(Request request, Response response)  throws Exception{

    }


    public void patchHandler(Request request, Response response)  throws Exception{

    }
}
