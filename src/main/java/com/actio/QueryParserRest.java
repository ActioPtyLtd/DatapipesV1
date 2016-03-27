package com.actio;

import com.typesafe.config.Config;

/**
 * Created by jim on 7/10/2015.
 */
public class QueryParserRest extends QueryParser {

    // sqlquery
    private String query;

    public String getQuery(){
        return query;
    }

    //
    public  void execute() throws Exception
    {
    }

    public void setConfig(Config _conf, Config _master) throws Exception {
        super.setConfig(_conf,_master);

        // initialise sqlquery parameters

    }

}
