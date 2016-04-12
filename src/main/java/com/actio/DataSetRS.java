package com.actio;

import com.typesafe.config.Config;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang3.StringEscapeUtils.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by jim on 7/8/2015.
 */

public class DataSetRS extends DataSetTabular {

    private static final Logger logger = LoggerFactory.getLogger(DataSetRS.class);

    private ResultSet dataRS;
    private int currentIndex = 0;
    private int currentIndexCache  = 0;

    private List<List<String>> currentBatchCache = new LinkedList<List<String>>();

    public String[] GetRow()
    {
        return null;
    }

    public boolean NextRow()
    {
        return false;
    }

    @Override
    public void set(ResultSet _result){

        dataRS = _result;
    }

    @Override
    public void set(List<String> _result){
        return;
    }

    @Override
    public ResultSet getResultSet(){

        return dataRS;
    }

    @Override
    public List<List<String>> getAsListOfColumns() throws Exception {
        initBatch();


        // side effect currentBatchCache is set -1 means, load the entire dataset
        getAsListOfColumnsBatch(-1);
        currentIndex = -1;

        return currentBatchCache;
    }

    @Override
    public void initBatch() throws Exception{


        dataRS.beforeFirst();
        currentIndex = 0;
        currentIndexCache = 0;
        resultSetCursorState = dataRS.next();
    }

    private boolean resultSetCursorState = false;


    @Override
    public boolean isNextBatch(){
        return resultSetCursorState;
    }

    @Override
    public DataSet getNextBatch() throws Exception
    {
        List<List<String>> batch = getAsListOfColumnsBatch(batchSize);

        DataSetTabular batchDS = new DataSetTabular();
        batchDS.setRsc(batch);

        logger.info("getNextBatch:"+batchDS.size());

        return batchDS;
    }

    private List<List<String>> getAsListOfColumnsCachedBatch(int batchLen)
            throws Exception {
        logger.info("getAsListOfColumnsCachedBatch::"+currentIndexCache);

        List<List<String>> subBatch;

        int upper = (currentIndexCache+batchLen < currentBatchCache.size())?
                currentIndexCache+batchLen : currentBatchCache.size() - 1;
        logger.info("getAsListOfColumnsCachedBatch::"+currentIndexCache+","+upper);
        subBatch = currentBatchCache.subList(currentIndexCache,upper);
        currentIndexCache+=batchLen;
        logger.info("Exit getAsListOfColumnsCachedBatch::"+subBatch.size());
        return subBatch;
    }

    @Override
    public List<List<String>> getAsListOfColumnsBatch(int batchLen)
            throws Exception {

        logger.info("getAsListOfColumnsBatch::"+currentIndex);

        // check that the FULL set has not already been cached
        if (currentIndex == -1)
            return getAsListOfColumnsCachedBatch(batchLen);

        ResultSetMetaData meta = dataRS.getMetaData();
        final int columnCount = meta.getColumnCount();
        int count = 0;
        List<List<String>> rowList = new LinkedList<List<String>>();

        while (resultSetCursorState && (count ++ < batchLen || batchLen == -1)) {
            List<String> columnList = new LinkedList<String>();
            rowList.add(columnList);

            for (int column = 1; column <= columnCount; column++) {
                Object value = dataRS.getObject(column);
                if (value != null) {
                    columnList.add(
                            StringEscapeUtils.escapeCsv(StaticUtilityFunctions.stripNonAscii(value.toString())));
                } else {
                    columnList.add(""); // you need this to keep your columns in sync.
                }
            }
            resultSetCursorState = dataRS.next();
        }
         if (currentIndex >=0) currentIndex += count;
        currentBatchCache = rowList;
        logger.info("exit getAsListOfColumnsBatch::"+currentIndex+",batch="+rowList.size());
        return currentBatchCache;
    }




    @Override
    public List<String> getAsList() throws Exception {

        List<List<String>> listSet = getAsListOfColumns();
        List<String> outList = new LinkedList<String>();

        // FLATTEN THE COLUMNS
        for (List<String> columns : listSet){
            outList.add(columnsToRow(columns,outputDelimiter));
        }

        setRs(outList);

        return getAsList();
    }

    @Override
    public List<String> getColumnHeader() throws Exception
    {
        // customHeader overrides
        if (customHeader != null) {
            List<String> columnHeader = Arrays.asList(customHeader.split(","));
            return columnHeader;
        }
        else {
            ResultSetMetaData meta = dataRS.getMetaData();
            final int columnCount = meta.getColumnCount();

            List<String> columnHeader = new LinkedList<String>();
            for (int column = 1; column <= columnCount; column++) {
                columnHeader.add(meta.getColumnName(column));
            }
            return columnHeader;
        }
    }

    @Override
    public String getColumnHeaderStr() throws Exception
    {
        return columnsToRow(getColumnHeader(),outputDelimiter);
    }


    @Override
    public void setConfig(Config _conf, Config _master) throws Exception {

        // call the parent to initialise configs
        super.setConfig(_conf,_master);

              /*
        config = _conf;
        masterConfig = _master;

        if (config.hasPath("customHeader"))
            customHeader = config.getString("customHeader");

        if (config.hasPath("outputDelimiter"))
            outputDelimiter = config.getString("outputDelimiter");
*/
    }



    // ********* WARNING - dont call dump on a large data set as it will bring it all into memory!!
    @Override
    public void dump() throws Exception {

        logger.info("===============dump datasetRS ============================");
        List<List<String>> rowList = getAsListOfColumns();

        // print out headings
        logger.info(getColumnHeaderStr());
        // print out the dataSource
        for (List<String> columns : rowList) {
            StringBuffer buff = new StringBuffer();
            for (String field : columns)
                buff.append(field + ',');
            logger.info(buff.toString());
        }
    }

}
