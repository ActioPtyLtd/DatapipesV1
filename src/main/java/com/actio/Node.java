package com.actio;

import com.typesafe.config.Config;

/**
 * Created by jim on 7/1/2015.
 */

/*

 CLASS QUERY implements specific API interfaces
 to handle the semantics of accessing an interface

 */

public abstract class Node extends Configurable {

    public abstract void execute() throws Exception;

    public void setConfig(Config _conf, Config _master) throws Exception {
        super.setConfig(_conf,_master);
    }

    protected String type;
    protected String name;
    protected int position;
    private final String[] aliases;
    private final String[] displayNames;

    public Node(){
        aliases = new String[1];
        displayNames = new String[1];
    }

}
