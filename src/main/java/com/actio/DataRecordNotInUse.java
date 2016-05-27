package com.actio;

/**
 * Created by jim on 7/8/2015.
 */

import com.actio.dpsystem.DPSystemConfigurable;
import com.typesafe.config.Config;

import java.util.List;

public abstract class DataRecordNotInUse extends DPSystemConfigurable {
    private String customHeader;

    public List<String> getRow() {
        return row;
    }

    public void setRow(List<String> row) {
        this.row = row;
    }

    public String getCustomHeader() {
        return customHeader;
    }

    private List<String> row;

    public Schema getSchema() {
        return schema;
    }

    public void setSchema(Schema schema) {
        this.schema = schema;
    }

    private Schema schema;

    public String getOutputDelimiter() {
        return outputDelimiter;
    }

    public void setOutputDelimiter(String outputDelimiter) {
        this.outputDelimiter = outputDelimiter;
    }

    private String outputDelimiter = "\t";

    public List<String> GetAsListOfColumns() throws Exception
    {
        return row;
    }

    public void setConfig(Config _conf, Config _master) throws Exception {
        super.setConfig(_conf,_master);
        if (config.hasPath("customHeader"))
            customHeader = config.getString("customHeader");

        if (config.hasPath("outputDelimiter"))
            outputDelimiter = config.getString("outputDelimiter");
    }

    public void dump() throws Exception
    {
        for (String col : row){
            logger.info(col+',');
        }
    }
}
