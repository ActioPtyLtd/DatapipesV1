package com.actio;

import com.actio.dpsystem.DPSystemConfig;
import com.actio.dpsystem.DPSystemFactory;
import com.actio.dpsystem.DPSystemRuntime;

import java.net.URL;
import java.net.URLClassLoader;

import com.jcabi.aspects.Loggable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class Main {


    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        
        System.out.println("=====First line in main");

        logger.info("======First logging.info");

        // get("/hello", (req, res) -> "Hello World");


        String configFile = null;
        String pipelineName = null;

        // Instantiate the Task Factory
        if (args.length > 0) {
            // check for a config file
            if (args[0].contains(".json") || args[0].contains(".conf")){
                configFile = args[0];
            }
            if (args.length > 1) {
                pipelineName = args[1];
            }
            logger.info("Params("+args.length+")="+configFile
                    +":'"+args[0]+"'  Pipeline=" + pipelineName) ;
        }
        else
            logger.info("Params("+args.length+")="+configFile
                    +": Pipeline=" + pipelineName) ;
        debug();



        DPSystemFactory tf = new DPSystemFactory();
        tf.loadConfig(configFile);

        DPSystemRuntime dprun = tf.newRuntime();

        dprun.execute();

        // dump out the runtime state
        dprun.dump();
    }


    private static void debug()
    {
        ClassLoader cl = ClassLoader.getSystemClassLoader();

        URL[] urls = ((URLClassLoader)cl).getURLs();

        for(URL url: urls){
            logger.info(url.getFile());
        }

    }


}