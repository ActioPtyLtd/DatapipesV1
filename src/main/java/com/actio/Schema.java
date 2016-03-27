package com.actio;

import com.typesafe.config.Config;

/**
 * Created by jim on 7/1/2015.
 */

import java.util.List;


public abstract class Schema extends Configurable {

    protected List<Node> nodes;
    protected String header;

    public abstract void execute() throws Exception;

    public void setConfig(Config _conf, Config _master) throws Exception {
        super.setConfig(_conf,_master);
    }



}
