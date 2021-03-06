package com.actio;

import com.actio.dpsystem.DPSystemFactory;
import com.typesafe.config.Config;

import java.sql.*;

/**
 * Created by jim on 7/8/2015.
 */
public class DataSourceSQLJava extends DataSource {


    public DataSourceSQLJava()
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
        throw new Exception(FUNCTION_UNIMPLEMENTED_MSG);
    }

    @Override
    public void extract() throws Exception {
        executeQuery(dataSet, QueryParser.processTemplate(sqlquery.getQueryTemplate()));
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
        create(data);
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

    @Override
    public void execute(DataSet data, String statement) throws Exception {
        DataSetTableScala nds = TransformsDataSet.prepare4statement(DataSetTableScala$.MODULE$.apply(data.schema(),data.headOption().get()), statement);   // assuming one batch again
        statement = statement.replaceAll("@(?<name>[-_a-zA-Z0-9]+)","?");

        logger.info("Connecting to database...");

        try(Connection cn = DriverManager.getConnection(getConnectStr())) {
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

            //logger.info("CREATE TABLE T1 (" + String.join(",", nds.getColumnHeader().stream().map(f -> f + " text").collect(Collectors.toList())) + ");");

            PreparedStatement stmt = cn.prepareStatement(statement);

            for (java.util.List<String> row : nds.getAsListOfColumns()) {
                int i = 1;

                for (String value : row) {
                    stmt.setString(i++, value);
                }

                stmt.addBatch();
            }
            logger.info("Executing SQL batch statement...");
            stmt.executeBatch();
            logger.info("Successfully executed statement.");
        }
        catch(BatchUpdateException e) {
            SQLException se = e.getNextException();
            if(se!=null)
                logger.error("Exception "+se.getMessage());
        }
        catch (Exception e)
        {
            logger.error("Exception "+e.getMessage());
            Throwable c = e.getCause();
            if(c!=null)
                logger.error(c.getMessage());
        }
    }

    @Override
    public DataSet executeQuery(DataSet ds, String query) throws Exception {
        // default to an extract behavior for SQL
        // execute the sqlquery

        logger.info("Connecting to database...");

        try {
            //cn.setNetworkTimeout(null,160000);
            // Get the warnings
            Connection cn = DriverManager.getConnection(getConnectStr());   //TODO: close connection on query execute exception

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
            String sqlQuery = query;
            //PreparedStatement stmt = cn.prepareStatement(QueryParser.processTemplate(sqlquery.getQueryTemplate()));

            Statement st = cn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);

            // store the results
            //dataSet = new DataSetRS();

            ////dataSet.setConfig(config, masterConfig);
            //// dataSet.set(stmt.executeQuery());

            logger.info("Executing Query: " + sqlQuery);

            //dataSet.set(st.executeQuery(sqlQuery));

            DataSetDBStream stream = new DataSetDBStream(st.executeQuery(sqlQuery), 100);
            dataSet = stream;


            logger.info("Successfully executed statement.");
        }
        catch (Exception e) {
            logger.error("Exception "+e.getMessage());
        }
        return dataSet;
    }


}
