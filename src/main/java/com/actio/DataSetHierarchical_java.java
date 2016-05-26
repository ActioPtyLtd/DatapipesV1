package com.actio;

/**
 * Created by jim on 7/8/2015.
 *
 * This class is used to abstract both XML and JSON data and allow interchange with tabular formats
 *
 * jackson library is used for json manipulation
 *
 */

import java.sql.ResultSet;
import java.util.List;

public abstract class DataSetHierarchical_java extends DataSet {

    // tab

    public void set(ResultSet _results) throws Exception
    {
        throw new Exception("Unimplemented");
    }

    public void set(List<String> _results) throws Exception
    {
        throw new Exception("Unimplemented");
    }

    public  ResultSet getResultSet() throws Exception
    {
        throw new Exception("Unimplemented");
    }

    public List<String> getAsList() throws Exception
    {
        throw new Exception("Unimplemented");
    }

    public List<List<String>> getAsListOfColumns()  throws Exception
    {
        throw new Exception("Unimplemented");
    }

    public List<String> getColumnHeader()  throws Exception
    {
        throw new Exception("Unimplemented");
    }

    public String getColumnHeaderStr() throws Exception
    {
        throw new Exception("Unimplemented");
    }


}
