package com.actio;

import com.actio.dpsystem.DPSystemConfigurable;
import com.typesafe.config.Config;

/**
 * Created by jim on 7/8/2015.
 */

public abstract class DataSource extends DPSystemConfigurable {

    public abstract void execute() throws Exception;
    public void execute(DataSet dataSet) throws Exception { execute(); }
    public abstract void extract() throws Exception;
    public void extract(DataSet dataSet) throws Exception { extract(); }
    public abstract void load() throws Exception;
    public void load(DataSet dataSet) throws Exception { load(); }

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

    public void create(DataSet ds) throws Exception {
        executeLabel(ds, "create");
    }

    public DataSet read(DataSet ds) throws Exception {
        return executeQueryLabel(ds, "read");
    }

    public void update(DataSet ds) throws Exception {
        executeLabel(ds, "update");
    }

    public void delete(DataSet ds) throws Exception {
        executeLabel(ds, "delete");
    }

    public void executeLabel(DataSet ds, String label) throws Exception {
        execute(ds, label);
    }

    public DataSet executeQueryLabel(DataSet ds, String label) throws Exception {
        return executeQuery(ds, label);
    }

    public abstract void execute(DataSet ds, String query) throws Exception;

    public abstract DataSet executeQuery(DataSet ds, String query) throws Exception;

}
