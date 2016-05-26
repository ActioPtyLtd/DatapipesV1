package com.actio;

/**
 * Created by jim on 7/8/2015.
 */

import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class DataSetTabular extends DataSet {

    private static final Logger logger = LoggerFactory.getLogger(DataSetTabular.class);

    public DataSetTabular(List<String> _rs)
    {
        set(_rs);
    }


    // Create an empty valid set
    public DataSetTabular()
    {
        rs =  new LinkedList<>();
        rsc = new  LinkedList<List<String>> ();

    }

    @Override
    public int sizeOfBatch() throws Exception {
        return getRs().size();
    }

    //protected String inputDelimiter = ",";
    private String inputDelimiter = "\t";

    private List<List<String>> getRsc() throws Exception {

        // if rsc is not set, set it from dataSource
        if (rsc == null)
            getAsListOfColumns();

        return rsc;
    }

    void setRsc(List<List<String>> rsc) {
        this.rsc = rsc;
    }

    private List<String> getRs() throws Exception {

        if (rs != null && rsc != null)
            if (rs.size() == 0 && rsc.size() > 0)
                rs = getAsList();
            else if (rs.size() != rsc.size() && rsc.size() != 0)
                // ASSERTION TEST ERROR if they don't match
                logger.error("getRs: ASSERTION ERROR rs.size()="+rs.size()+"  rsc.size()="+rsc.size()+" DO NOT MATCH");

        return rs;
    }

    public void setRs(List<String> rs) {
        this.rs = rs;
    }

    private List<String> rs = null;
    private List<List<String>> rsc = null;

    private int getCurrentColumnIndex() {
        return currentColumnIndex;
    }

    public void setCurrentColumnIndex(int currentColumnIndex) {
        this.currentColumnIndex = currentColumnIndex;
    }

    public String getInputDelimiter() {
        return inputDelimiter;
    }

    public void setInputDelimiter(String inputDelimiter) {
        this.inputDelimiter = inputDelimiter;
    }

    private int currentColumnIndex = 0;

    public void set(ResultSet _rs) {
        return;
    }

    private int rowNumber = 0;

    private boolean emptySet = true;

    @Override
    public void set(List<String> _rs) {
        rs = _rs;
    }

    public void setWithFields(List<List<String>> _rsc)
    {
        rsc = _rsc;
    }

    @Override
    public List<String> getAsList() throws Exception {
        if (rs != null && rs.size() != 0)
            return rs;

        // it's empty so return an empty list
        if (rsc == null)
            return new LinkedList<String>();

        List<String> rowList = new LinkedList<String>();

        for (List<String> columnRow : rsc) {
            StringBuilder flattenedRow = new StringBuilder();

            for (String column : columnRow){
                if (flattenedRow.length() > 0)
                     flattenedRow.append(getOutputDelimiter());

                flattenedRow.append(column);
            }
            rowList.add(flattenedRow.toString());
        }
        rs = rowList;
        return rs;
    }

    @Override
    public List<List<String>> getAsListOfColumns() throws Exception {

        // if it's already been called and assigned get the cached version
        if (rsc != null)
            return rsc;

        List<List<String>> rowList = new LinkedList<List<String>>();

        // loading all this into memory is inefficient will need to revise in the future to stream and write
        for (String rows : getAsList()) {
            String[] columnList = rows.split(inputDelimiter);
            rowList.add(new ArrayList<String>(Arrays.asList(columnList)));
        }
        rsc = rowList;
        return rsc;
    }

    @Override
    public void initBatch() throws Exception{
        // Set row number
        rowNumber = 0;
        if (sizeOfBatch() == 0)
            emptySet = true;
        else
            emptySet = false;
    }

    public List<List<String>> getAsListOfColumnsBatch(int batchLen) throws Exception{
        throw new Exception(FUNCTION_UNIMPLEMENTED_MSG);
    }


    @Override
    public boolean hasNext()
    {
        int s = 0;
        try {
            s = sizeOfBatch();
        }
        catch(Exception e) {
            s = 0;
        }

        return (rowNumber < s || emptySet);
    }


    // REFACTOR NOTE
    // Could use a flywheel pattern such that each DataSet is a bounded index
    // over the underlying Master RS list, such that there is only ever one copy

    //@Override
    public DataSet getNextBatch() throws Exception{

        logger.info("Entered getNextBatch rowNumber"+rowNumber+", batchSize="+batchSize());

        DataSet newSet =  new DataSetTabular();

        if (emptySet) {
            emptySet = false;
            return newSet;
        }

        if (rowNumber >= 0 && rowNumber < sizeOfBatch()) {
            int index = rowNumber+batchSize();
            List<String> sublist = getAsList().subList(rowNumber,
                    index< sizeOfBatch()?index: sizeOfBatch());

            // increment the row number so it moves to next batch
            rowNumber += (rowNumber + batchSize() > sizeOfBatch())? rowNumber+batchSize(): sizeOfBatch();


            logger.info("Exiting getNextBatch new rowNumber"+rowNumber);
            newSet.set(sublist);
            return newSet;
        }

        return newSet;
    }

    @Override
    public List<String> getColumnHeader() throws Exception {
        // not supported
        return getAsListOfColumns().get(0);
    }

    @Override
    public String getColumnHeaderStr() throws Exception {
        if (getCustomHeader() == null) {
            // assume first line is header - piss poor assumption
            if (rs!= null)
                return rs.get(1);
            else
                return null;

        } else {

            return getCustomHeader();
        }

    }

    @Override
    public void setConfig(Config _conf, Config _master) throws Exception {
        // call the parent to initialise configs
        super.setConfig(_conf, _master);

        if (config.hasPath("inputDelimiter"))
            inputDelimiter = config.getString("inputDelimiter");

    }

    // Iterator for Rows and Columns by Index or by string
    public String FromRowGetField(int rowIndex, String label) throws Exception {
        return FromRowGetField(rowIndex, getColumnHeader().indexOf(label));
    }

    public String FromRowGetField(int rowIndex, int label) throws Exception {

        return getRsc().get(rowIndex).get(label);
    }

    // Iterator for Rows and Columns by Index or by string
    public String CurrentRowGetField(String label) throws Exception {
        return getRs().get(getCurrentColumnIndex());
    }

    public String CurrentRowGetField(int label) throws Exception {

        return getRsc().get(getCurrentColumnIndex()).get(label);
    }


    public String getColumnByLabel(String label){

        return null;
    }

    @Override
    public void dump() throws Exception
    {
        logger.info("=== dataSource set dump======");

        if (getAsList() == null)
            return ;

        for (String line: getRs())
            logger.info(line);

    }

    @Override
    public Data next() {
        return this.toData();
    }
}
