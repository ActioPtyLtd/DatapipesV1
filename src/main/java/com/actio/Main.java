package com.actio;

import com.actio.dpsystem.DPSystemConfig;
import com.actio.dpsystem.DPSystemFactory;
import com.actio.dpsystem.DPSystemRuntime;

import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.jcabi.aspects.Loggable;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.System.exit;

class Main {


    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {

        logger.info("======First logging.info");
        Path currentRelativePath = Paths.get("");
        String s = currentRelativePath.toAbsolutePath().toString();
        System.out.println("Current relative path is: " + s);

        CommandLineParser parser = new DefaultParser();
        Options options = setOptions();
        String configFile = null;
        String pipelineName = null;
        Boolean runService = false;


        try {
            // parse the command line arguments
            CommandLine line = parser.parse( options, args );

            // Check the options set
            if( line.hasOption( "c" ) ) {
                // print the value of config
                configFile = line.getOptionValue('c');
                logger.info( configFile );
            }
            if (line.hasOption('p')) {
                configFile = line.getOptionValue('p');
                logger.info( pipelineName );
            }
            if (line.hasOption('s')){
                runService = true;
                logger.info("============ RUN AS SERVICE ==========");
            }

        }
        catch( ParseException exp ) {
            logger.error( "Unexpected exception:" + exp.getMessage() );

            exit(-1);
        }

        debug();

        DPSystemFactory tf = new DPSystemFactory();
        tf.loadConfig(configFile);

        DPSystemRuntime dprun = tf.newRuntime();

        if (!runService) {
            if (pipelineName == null)
                dprun.execute();
            else
                dprun.execute(pipelineName);
        }
        else {
            dprun.service();
        }
        // dump out the runtime state
        dprun.dump();
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

        return options;
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