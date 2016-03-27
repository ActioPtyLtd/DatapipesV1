package com.actio;

import com.typesafe.config.Config;

/**
 * Created by jim on 7/8/2015.
 */
public class QueryParserSQL extends QueryParser {

    @Override
    public void setConfig(Config _conf, Config _master) throws Exception
    {
        super.setConfig(_conf, _master);

        logger.info("QueryParserSQL:setConfig:"+getQueryTemplate()+" : ");
    }


}
