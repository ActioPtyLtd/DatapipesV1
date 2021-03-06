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
        Boolean suppressEvents = false;
        Boolean returnLastProcessedCount = false;
        Integer exitCode = 0;
        Logger logger =  null;
        Boolean BatchMode = true;

        try {
            // parse the command line arguments
            CommandLine line = parser.parse( options, args );
            String appendLogName = "application";

            // Check the options set
            if ( line.hasOption( "help" ) ) {
                printUsage(options);
                exit(0);
            }

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
            if (line.hasOption('n')){
                returnLastProcessedCount = true;
                logger.info("============ USE PROCESSED RECORD COUNT OF LAST TASK AS EXIT CODE ============");
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
            if (line.hasOption('S')){
                logger.info("Suppressing event streaming");
                suppressEvents = true;
            }

            debug(logger);

            logger.info("loadingConfigFile=" + configFile);


            DPSystemRuntime dprun = new DPSystemRuntime(configFile,properties);

            try
            {
                if (loadConfigIntoAdmin){

                    dprun.uploadConfig();
                }
                else  if (!runService) {
                    if (suppressEvents)
                        dprun.disableEvents();
                    if (pipelineName == null)
                        dprun.execute();
                    else
                        dprun.execute(pipelineName);
                    if(returnLastProcessedCount)
                        exitCode = dprun.lastTotal;

                    ///==========
                    if (!suppressEvents)
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

        exit (exitCode);
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
        options.addOption( "n", "return number of records processed in final task as the exit code");
        options.addOption( "help", "print this help message");
        options.addOption( "L", "load config file into Admin Server");
        options.addOption( "S", "Supress event streaming to Admin Server");
        options.addOption( Option.builder("D").argName( "property=value" )
                .hasArgs()
                .valueSeparator('=')
                .build());

        return options;
    }

    private static void printUsage(Options options)
    {
        final HelpFormatter helpFormatter = new HelpFormatter();
        helpFormatter.printHelp( " ", options );
    }

    private static void debug(Logger logger)
    {
        ClassLoader cl = ClassLoader.getSystemClassLoader();

        URL[] urls = ((URLClassLoader)cl).getURLs();
        logger.info("============================= Dump loaded classes ================= ");
        for(URL url: urls){
            logger.info(url.toString());
        }

    }
}
