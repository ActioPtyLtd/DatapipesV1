package com.actio;

import com.actio.dpsystem.DPSystemConfigurable;
import com.typesafe.config.Config;

/**
 * Created by jim on 7/8/2015.
 */
public abstract class DataSource extends DPSystemConfigurable {

    public abstract void execute() throws Exception;
    public abstract void extract() throws Exception;
    public abstract void load() throws Exception;

    String getConnectStr() throws Exception {
        return connectStr;
    }

    private void setConnectStr(String connectStr) {
        this.connectStr = connectStr;
    }

    public abstract void write(DataSet data)  throws Exception;
    public abstract void write(DataSet data, String suffix)  throws Exception;

    public abstract DataSet read(QueryParser queryParser)  throws Exception;

    public abstract DataSet getLastLoggedDataSet() throws Exception;
    public abstract void LogNextDataSet(DataSet theSet) throws Exception;

    private String connectStr;

    @Override
    public void setConfig(Config _conf, Config _master) throws Exception {
        super.setConfig(_conf,_master);

        if (config.hasPath("connect"))
            setConnectStr(config.getString("connect"));
    }



}
