package com.actio;

import com.actio.dpsystem.DPSystemFactory;
import com.typesafe.config.Config;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLWarning;
import java.util.stream.Collectors;

/**
 * Created by jim on 7/8/2015.
 */
public class DataSourceSQL extends DataSource {


    public DataSourceSQL()
    {
        /* no op */
    }

    // DataSet results;
    private QueryParser sqlquery;

    @Override
    public void setConfig(Config _conf, Config _master) throws Exception
    {
        super.setConfig(_conf, _master);

        // instantiate the correct jdbc driver class
        if (config.hasPath(JDBC_DRIVER_LABEL)) {
            try {
                Class.forName(config.getString(JDBC_DRIVER_LABEL));
            } catch (ClassNotFoundException var1) {
                var1.printStackTrace();
            }
        }

        sqlquery = DPSystemFactory.newQuery(SQL_LABEL,config,masterConfig);
    }

    public void execute() throws Exception {
            extract();
    }


    public void load() throws Exception {

        Connection cn = null;

        try {
            cn = DriverManager.getConnection(getConnectStr());
            //cn.setNetworkTimeout(null,160000);
            // Get the warnings
            for (SQLWarning warn = cn.getWarnings(); warn != null; warn = warn
                    .getNextWarning()) {
                // Note: Printing to Standard Out and putting messages in
                // Pop-Up.
                logger.info("Connection Warning:");
                logger.info("State  : " + warn.getSQLState());
                logger.info("Message: " + warn.getMessage());
                logger.info("Error  : " + warn.getErrorCode());
                warn = warn.getNextWarning();
            }
            logger.info("Connected");

            String parameters = "(" + String.join(",", dataSet.getColumnHeader().stream().map(c -> "?").collect(Collectors.toList())) + ")";
            String tablename = getConfig().getString("Table");
            Boolean truncate = !getConfig().atKey("Truncate").isEmpty();

            if(truncate) {
                logger.info("Truncating table...");
                cn.prepareStatement("truncate table " + tablename).execute();
                logger.info("Table truncated");
            }

            PreparedStatement stmt = cn.prepareStatement("insert into " + tablename + "values " + parameters);

            while(dataSet.isNextBatch()) {
                for (java.util.List<String> row : dataSet.getAsListOfColumns()) {
                    int i = 1;

                    for (String value : row)
                        stmt.setString(i++, value);

                    stmt.addBatch();
                }
                logger.info("Executing SQL Statement...");
                stmt.executeBatch();
                logger.info("Executed SQL Statement.");
            }
            cn.close();

        } catch (Exception e)
        {
            logger.info("Exception "+e.getMessage());
        }
    }

    @Override
    public void extract() throws Exception {
        // default to an extract behavior for SQL
        // execute the sqlquery

        logger.info("SqlQuery: " + QueryParser.processTemplate(sqlquery.getQueryTemplate()));

        Connection cn = null;

        try {
            cn = DriverManager.getConnection(getConnectStr());
            //cn.setNetworkTimeout(null,160000);
            // Get the warnings
            for (SQLWarning warn = cn.getWarnings(); warn != null; warn = warn
                    .getNextWarning()) {
                // Note: Printing to Standard Out and putting messages in
                // Pop-Up.
                logger.info("Connection Warning:");
                logger.info("State  : " + warn.getSQLState());
                logger.info("Message: " + warn.getMessage());
                logger.info("Error  : " + warn.getErrorCode());
                warn = warn.getNextWarning();
            }
            logger.info("Connected");

            // Get QueryParser
            PreparedStatement stmt = cn.prepareStatement(
                    QueryParser.processTemplate(sqlquery.getQueryTemplate()));

            // store the results
            dataSet = new DataSetRS();

            dataSet.setConfig(config, masterConfig);
            dataSet.set(stmt.executeQuery());

            logger.info("Executed SQL Statement :");

        } catch (Exception e)
        {
            logger.info("Exception "+e.getMessage());
        }
    }

    @Override
    public DataSet getDataSet() {
        return dataSet;
    }

    @Override
    public void setDataSet(DataSet set){
        dataSet = set;
    }

    @Override
    public DataSet getLastLoggedDataSet() throws Exception
    {
        throw new Exception(FUNCTION_UNIMPLEMENTED_MSG);
    }

    @Override
    public void LogNextDataSet(DataSet set) throws Exception
    {
        throw new Exception(FUNCTION_UNIMPLEMENTED_MSG);
    }

    @Override
    public void write(DataSet data)  throws Exception
    {
        throw new Exception(FUNCTION_UNIMPLEMENTED_MSG);
    }

    @Override
    public void write(DataSet data, String suffix)  throws Exception
    {
        throw new Exception(FUNCTION_UNIMPLEMENTED_MSG);
    }

    @Override
    public DataSet read(QueryParser queryParser)  throws Exception
    {
        throw new Exception(FUNCTION_UNIMPLEMENTED_MSG);
    }




}
