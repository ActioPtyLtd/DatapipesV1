package com.actio;

import com.actio.dpsystem.DPSystemFactory;
import com.typesafe.config.Config;
import org.apache.commons.lang.StringUtils;

import java.math.BigDecimal;
import java.util.stream.Collectors;
import java.sql.*;

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
        DataSet nds = DataSetTransforms.prepare4statement(data, statement);
        statement = statement.replaceAll("@(?<name>[-a-zA-Z0-9]+)","?");

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

            logger.info("CREATE TABLE T1 (" + String.join(",", nds.getColumnHeader().stream().map(f -> f + " text").collect(Collectors.toList())) + ");");

            PreparedStatement stmt = cn.prepareStatement(statement);

            for (java.util.List<String> row : nds.getAsListOfColumns()) {
                int i = 1;

                for (String value : row) {
                    stmt.setString(i++, value);
                }

                stmt.addBatch();
            }
            logger.info("Executing SQL Statement...");
            stmt.executeBatch();
            logger.info("Executed SQL Statement.");
        } catch (Exception e)
        {
            logger.info("Exception "+e.getMessage());
        }
        finally {
            if(cn !=null)
                cn.close();
        }
    }

    @Override
    public DataSet executeQuery(DataSet ds, String query) throws Exception {
        // default to an extract behavior for SQL
        // execute the sqlquery

        logger.info("SqlQuery: " + query);

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
            String sqlQuery = query;
            //PreparedStatement stmt = cn.prepareStatement(QueryParser.processTemplate(sqlquery.getQueryTemplate()));

            Statement st = cn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,ResultSet.CONCUR_READ_ONLY);

            // store the results
            //dataSet = new DataSetRS();

            ////dataSet.setConfig(config, masterConfig);
            //// dataSet.set(stmt.executeQuery());

            logger.info("Executing Query="+sqlQuery);

            //dataSet.set(st.executeQuery(sqlQuery));

            dataSet = new DataSetDBStream(st.executeQuery(sqlQuery));

            logger.info("Executed SQL Statement :");
        } catch (Exception e)
        {
            logger.info("Exception "+e.getMessage());
        }
        finally {
            if(cn !=null)
                cn.close();
        }
        return dataSet;
    }


}
