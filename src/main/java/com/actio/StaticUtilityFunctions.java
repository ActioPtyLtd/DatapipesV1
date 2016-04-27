package com.actio;

import com.actio.dpsystem.DPSystemConfigurable;
import com.typesafe.config.Config;
import org.joda.time.LocalDate;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by dimitarpopov on 24/08/15.
 */
public class StaticUtilityFunctions extends DPSystemConfigurable {

    /*

    Function used to map to transform functions

     */

    // execute functions at 3 levels
    // 1. Full DataSet
    // 2. At a Row
    // 3. At a Field level

    public static DataSet execute(DataSet theSet, CompiledTemplateFunctionSet fns) throws Exception
    {
        // iterate over the functions, processing the source
        DataSet  newSource = new DataSetTabular();
        List<List<String>> newRows = new LinkedList<List<String>>();

        // process for each line
        List<List<String>> ds = theSet.getAsListOfColumns();

        for (List<String> row : ds)
        {
            newRows.add(execute(row,fns));
        }

        // copy across into the new DataSet
        newSource.setWithFields(newRows);

        for (TransformFunction tf : fns.getFunctions().get(GLOBAL_FUNCTIONS_KEY)){
            newSource = execute(newSource, tf);
        }
        return newSource;
    }


    private static List<String> execute(List<String> row, CompiledTemplateFunctionSet fns) throws Exception
    {

        List<String> newRow = new ArrayList<>(row.size());

        for (int i = 0; i < row.size(); i++){
            // index starts from 0 but columns start from 1 so add + 1
            newRow.add(i,execute(row.get(i),fns.getFunctions(i+1)));
        }

        for (TransformFunction fn : fns.getFunctions().get(ROW_FUNCTIONS_KEY)) {
            newRow = execute(newRow,fn);
        }
        return newRow;
    }

    public static String execute(String  columns, CompiledTemplateFunctionSet fns) throws Exception
    {
        // process row by row

        return null;
    }

    private static String execute(String source, List<TransformFunction> fns) throws Exception
    {
        // iterate over the functions, processing the source
        String  newSource = source;

        if (fns == null) return newSource;

        for (TransformFunction tf : fns){
            newSource = execute(newSource,tf);
        }
        return newSource;
    }

    private static String execute(String source, TransformFunction fn) throws Exception {
        // iterate over the functions, processing the source
        String newSource = source;


        logger.info("execute:fn="+fn.getName()+"=="+source);

        switch (fn.getName()) {

            case "setDefaults" :
                newSource = setDefaults(source,fn);
                break;
            case "formatForPhone" :
                newSource = formatForPhone(source, fn);
                break;
            case "setValueByRegexpForColumns" :
                //newSource = setValueByRegexpForColumns(source, fn);
                break;
            case "setDateFormat" :

                break;
            case "getOffsetDate" :

                break;
            case "trimWhiteSpace" :

                break;
            default :
                logger.info("Unknown Function in StaticUtilityFunctions::"+fn.getName());
        }

        return newSource;
    }

    private static DataSet execute(DataSet set, TransformFunction fn) throws Exception {

        // iterate over the functions, processing the source
        DataSet newset = set;

        switch (fn.getName()) {

            case "Sort" :

                break;
            case "RemoveDuplicate" :

                break;
            case "GroupBy" :

                break;
            case "header" :
                newset = DataSetTableScala.toDataSetTableScala(set).transformFirstRowAsHeader();
                break;
            case "addheader":
                newset = DataSetTableScala.toDataSetTableScala(set).transformAddHeader(fn);
                break;
            case "split2cols" :
                newset = DataSetTableScala.toDataSetTableScala(set).transformSplitToColumns(fn);
                break;
            case "split2rows":
                newset = DataSetTableScala.toDataSetTableScala(set).transformSplitToRows(fn);
                break;
            case "keep":
                newset = DataSetTableScala.toDataSetTableScala(set).transformSelect(fn);
                break;
            case "keepregex":
                newset = DataSetTableScala.toDataSetTableScala(set).transformSelectRegex(fn);
                break;
            case "constant":
                newset = DataSetTableScala.toDataSetTableScala(set).transformAddConstant(fn);
                break;
            case "rename":
                newset = DataSetTableScala.toDataSetTableScala(set).transformRename(fn);
                break;
            case "concat":
                newset = DataSetTableScala.toDataSetTableScala(set).transformConcat(fn);
                break;
            case "drop":
                newset = DataSetTableScala.toDataSetTableScala(set).transformDrop(fn);
                break;
            case "match2cols":
                newset = DataSetTableScala.toDataSetTableScala(set).transformMatchToColumns(fn);
                break;
            case "splitcol2rows":
                newset = DataSetTableScala.toDataSetTableScala(set).transformSplitColToRows(fn);
                break;
            default :
                logger.info("Unknown Function in StaticUtilityFunctions::"+fn.getName());
        }

        return newset;
    }

    private static List<String> execute(List<String> source, TransformFunction fn) throws Exception {
        // iterate over the functions, processing the source
        List<String> newSource = source;

        switch (fn.getName()) {

            case "DeleteColumn" :

                break;
            case "CombineColumn" :

                break;
            default :
                logger.info("Unknown Function in StaticUtilityFunctions::"+fn.getName());
        }

        return newSource;
    }


    private static String setDefaults(String col, TransformFunction fn) {

        String[] params = fn.getParameters();

        if (params.length <= 0)
            return col;

        if (col.matches("") || col == null)
            return params[1];
        else
            return col;
    }

    // REFACTOR make this a callable transform
    public static List<List<String>> RemoveDuplicates(List<List<String>> inList)
    {
        List<List<String>> newList = inList.stream()
                .map(WrapColumns::new)
                .distinct()
                .map(WrapColumns::getColumns)
                .collect(Collectors.toList());

        return newList;
    }

    public static DataSet templateMerge(DataSet rows) throws Exception
    {
        throw new Exception(FUNCTION_UNIMPLEMENTED_MSG);
    }

    private static String formatForPhone(String newphone, TransformFunction fn)
    {
        try {

            newphone = newphone.replaceAll("[^\\d\\+]", "");

            // if not correct format blank it out
            if (newphone.matches("^\\+61\\d{9}|04\\d{8}") == false) {
                newphone = "-1";
            } else if (newphone.matches("^04\\d{8}") == true) {
                // need to truncate 0 and replace with +61
                newphone = newphone.replaceAll("^04", "\\+61 4");
            }

        }catch(Exception e) {
            // bad index
            logger.info("formatForPhone::error:: "+e.toString());
        }
        logger.info("formatForPhone="+newphone);
        return newphone;
    }

    // =====================================================================

    public static List<String> setValueByRegexpForColumns(String row, TransformFunction fn){
/*
        // REFACTOR: should move these to instance variables so they don't get recreated on every single loop
<<<<<<< HEAD
        //List<Integer> rows = config.getIntList("setValueByRegexp_Columns");
        //String replaceRegExp = config.getString("setValueByRegexp_Regexp");
        //String replaceValue = config.getString("setValueByRegexp_Value");

        //try {

        //    for (int idx : rows) {
        //        idx = idx - 1; // adjust for array starting at 0
        //        String sashVetName = rows.get(idx);

        //        if (sashVetName != null) {
        //            sashVetName = sashVetName.replaceFirst(replaceRegExp, replaceValue).trim();
        //            rows.set(idx, sashVetName);
        //        }
        //    }
        //} catch (Exception e) {
        //    logger.info("TaskTransform::setValueByRegexpForColumns::"+e);
        //}
        //return rows;
=======
        List<Integer> rows = config.getIntList("setValueByRegexp_Columns");
        String replaceRegExp = config.getString("setValueByRegexp_Regexp");
        String replaceValue = config.getString("setValueByRegexp_Value");

        try {

            for (int idx : rows) {
                idx = idx - 1; // adjust for array starting at 0
                String sashVetName = row.get(idx);

                if (sashVetName != null) {
                    sashVetName = sashVetName.replaceFirst(replaceRegExp, replaceValue).trim();
                    row.set(idx, sashVetName);
                }
            }
        } catch (Exception e) {
            logger.info("TaskTransform::setValueByRegexpForColumns::"+e);
        }
        return row;


>>>>>>> 142ce3a10e7d9e97c95bab4fabee6945c2badaec */
        return null;
    }


    public static List<String> SetDateFormat(List<String> row, Config config){

        // REFACTOR: should move these to instance variables so they don't get recreated on every single loop
        List<Integer> rows = config.getIntList("SetDateFormat_Columns");

        String inFormat = config.getString("SetDateFormat_InputFormat");
        String outFormat = config.getString("SetDateFormat_OutputFormat");

        try {
            for (int idx : rows) {

                idx = idx - 1; // adjust for array starting at 0
                String value = row.get(idx); // array counts from 0

                if (value != null) {
                    SimpleDateFormat inSdf = new SimpleDateFormat(inFormat);
                    SimpleDateFormat sdf = new SimpleDateFormat(outFormat);

                        Date date = inSdf.parse(value);

                        value = sdf.format(date);

                        row.set(idx, value);

                }
            }

        } catch (ParseException e) {
            logger.info("setDateFormat::"+e);
        }


        return row;
    }


    public static Date getOffsetDate(final int offset) {
        long offSetDate = LocalDate.now().plusDays(offset).toDate().getTime();
        return new Date(offSetDate);
    }


    public static Date getTodayDate() {
        long today = LocalDate.now().toDate().getTime();
        return new Date(today);
    }

    public static int currentTimeInMinutes() {
        long today = LocalDate.now().toDate().getTime();
        long currentTimeInMillis = System.currentTimeMillis();
        return (int) (((currentTimeInMillis - today) / 1000) / 60);
    }


    /*====================================================


     */

    public static String getDate(TransformFunction func) {

        String[] params = func.getParameters();

        // extract for each param
        int dateOffset = 0;
        String unitOffset = "dd";
        String dateTemplateFormat = "yyyyMMdd_HHmmss";

        if (params.length >= 2)
            dateOffset = Integer.parseInt(params[1]);

        if (params.length >=3)
            dateTemplateFormat = params[2];

        if (params.length >= 4)
            unitOffset = params[3];

        DateFormat dateFormat;
        Calendar cal = Calendar.getInstance();

        // IGNORING UNIT OFFSET RIGHT NOW - REFACTOR
        cal.add(Calendar.DATE, dateOffset);

        dateFormat = new SimpleDateFormat(dateTemplateFormat);

        return dateFormat.format(cal.getTime());
    }

    public static void DumpList(List<String> s)
    {
        for (String f : s){
            System.out.print(" "+f+",");
        }
        logger.info("");
    }

    public static String stripNonAscii(String buffer) {
        StringBuffer out = new StringBuffer();
        int j = 0;
        for (int i = 0, n = buffer.length(); i < n; ++i) {
            char c = buffer.charAt(i);
            //
            if (c <= '\u007F' && (c == '\015'  || c == '\012' || c >= '\u0020'))
                out.append(c);
            else
                if (c == 0)
                    logger.info("Bad Char=="+String.format("%04x", (int) c));
        }
        return  out.toString();
    }

}

