package com.actio;

/**
 * Created by jim on 7/8/2015.
 */
import com.actio.dpsystem.DPSystemConfigurable;
import com.typesafe.config.Config;

import java.sql.*;
import java.util.List;
import java.util.LinkedList;

public abstract class DataSet extends DPSystemConfigurable {

    public DataSetKey key = new DataSetKey();
    public void setKey(DataSetKey _key){
        key = _key;
    }
    public void setChunk(int chunkStart, int chunkEnd, int maxChunk){
        key.chunkStart = chunkStart;
        key.chunkEnd = chunkEnd;
        key.maxChunk = maxChunk;
    }

    protected String customHeader;

    public abstract int size() throws Exception;

    public Schema getSchema() {
        return schema;
    }

    public void setSchema(Schema schema) {
        this.schema = schema;
    }

    private Schema schema;

    String getOutputDelimiter() {
        return outputDelimiter;
    }

    public void setOutputDelimiter(String outputDelimiter) {
        this.outputDelimiter = outputDelimiter;
    }

    public String getCustomHeader() {
        return customHeader;
    }

    public void setCustomHeader(String customHeader) {
        this.customHeader = customHeader;
    }

    String outputDelimiter = ",";

    int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    int batchSize = 500;

    public abstract String[] GetRow()  throws Exception;
    public abstract boolean NextRow()  throws Exception;
    public abstract void set(ResultSet _results)  throws Exception;

    public abstract void set(List<String> _results)  throws Exception;
    public abstract void setWithFields(List<List<String>> _results) throws Exception;

    public abstract ResultSet getResultSet()  throws Exception;
    public abstract List<String> getAsList() throws Exception;
    public abstract List<List<String>> getAsListOfColumns() throws Exception;

    public abstract void initBatch() throws Exception;
    public abstract boolean isNextBatch() throws Exception;
    public abstract DataSet getNextBatch() throws Exception;

    public abstract List<List<String>> getAsListOfColumnsBatch(int batchLen) throws Exception;

    public abstract List<String> getColumnHeader() throws Exception;
    public abstract String getColumnHeaderStr() throws Exception;

    // Iterator for Rows and Columns by Index or by string
    public abstract String FromRowGetField(int rowIndex, String label) throws Exception;
    public abstract String FromRowGetField(int rowIndex, int label) throws Exception;

    public void setConfig(Config _conf, Config _master) throws Exception {
        super.setConfig(_conf,_master);
        if (config.hasPath("customHeader"))
            customHeader = config.getString("customHeader");

        if (config.hasPath("outputDelimiter"))
            outputDelimiter = config.getString("outputDelimiter");
    }

    // REFACTOR - Performance Improvement
    // Need to Add a DataSetLine class to abstract a basic line


    public static List<String> flattenRows(List<List<String>> rows, String _outputDelimiter)
    {
        List<String> newRows = new LinkedList<String>();

        for (List<String> columns : rows)
            newRows.add(columnsToRow(columns,_outputDelimiter));

        return newRows;
    }

    static String columnsToRow(List<String> columns, String _outputDelimiter) {
        String row = "";
        int count = 1;
        int maxCols = columns.size();

        for (String field : columns) {
            row = row + field;
            if (count++ < maxCols)
                row = row + _outputDelimiter;

        }
        return row;
    }

    public abstract void dump() throws Exception;

}
