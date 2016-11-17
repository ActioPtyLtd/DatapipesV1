package com.actio;


import com.actio.dpsystem.DPSystemFactory;
import com.actio.dpsystem.DPSystemRuntime;

import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Enumeration;
import java.util.Properties;

//import com.jcabi.aspects.Loggable;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;

import static java.lang.System.exit;

public class Main {

    public static void main(String[] args) throws Exception {

        Path currentRelativePath = Paths.get("");
        String s = currentRelativePath.toAbsolutePath().toString();
        System.out.println("Current relative path is: " + s);

        CommandLineParser parser = new DefaultParser();
        Options options = setOptions();
        String configFile = null;
        String pipelineName = null;
        Boolean runService = false;
        Properties properties = null;
        Boolean loadConfigIntoAdmin = false;

        Logger logger =  null;

        try {
            // parse the command line arguments
            CommandLine line = parser.parse( options, args );
            String appendLogName = "application";

            // Check the options set
            if( line.hasOption( "c" ) ) {
                // print the value of config
                configFile = line.getOptionValue('c');
                appendLogName = new File(configFile).getName().replaceFirst("[.][^.]+$", "");
            }

            System.setProperty("log.configname", appendLogName);
            logger = LoggerFactory.getLogger(Main.class);
            logger.info("======First logging.info ***");

            if (line.hasOption('p')) {
                pipelineName = line.getOptionValue('p');
                logger.info( pipelineName );
            }
            if (line.hasOption('s')){
                runService = true;
                logger.info("============ RUN AS SERVICE ==========");
            }
            if (line.hasOption('D')) {
                properties = line.getOptionProperties("D");
                Enumeration e = properties.propertyNames();

                logger.info("Command line parameters:");

                while (e.hasMoreElements()) {
                    String key = (String) e.nextElement();
                    logger.info(key + " = " + properties.getProperty(key));
                }
            }
            if (line.hasOption('L')) {
                logger.info("Load Config into Admin Server");
                loadConfigIntoAdmin = true;
            }

            logger.info("loadingConfigFile=" + configFile);

            debug(logger);

            //DPSystemFactory tf = new DPSystemFactory();
            //tf.loadConfig(configFile, properties);

            DPSystemRuntime dprun = new DPSystemRuntime(configFile,properties);

            try
            {
                if (loadConfigIntoAdmin){

                    dprun.uploadConfig();
                }
                else  if (!runService) {
                    if (pipelineName == null)
                        dprun.execute();
                    else
                        dprun.execute(pipelineName);

                    ///==========
                    dprun.sendEvents();
                    logger.info("Execution completed.");
                }
                else {
                  dprun.service();
              }

            }
            catch(Exception exp) {
              logger.error("Unexpected exception:" + exp.toString());
              logger.info("Execution stopped.");
            }
            // dump out the runtime state
            dprun.dump();
        }
        catch( ParseException exp ) {
            System.out.print( "Unexpected exception:" + exp.getMessage() );

            exit(-1);
        }
    }


    private static Options setOptions()
    {
        /*
        Command line options
        -s  : run as service according to Service
        -c  : config
        -p  : run pipeline
         */
        // create the Options
        Options options = new Options();
        options.addOption( "c", "config", true, "config" );
        options.addOption( "p", "pipe", true, "run named pipeline .." );
        options.addOption( "s", "service", false, "run as Service, as configured in Services section");
        options.addOption( "L", "load config file into Admin Server");
        options.addOption( Option.builder("D").argName( "property=value" )
                .hasArgs()
                .valueSeparator('=')
                .build());

        return options;
    }

    private static void debug(Logger logger)
    {
        ClassLoader cl = ClassLoader.getSystemClassLoader();

        URL[] urls = ((URLClassLoader)cl).getURLs();

        for(URL url: urls){
            logger.info(url.getFile());
        }

    }
}
