package com.actio;

import com.actio.dpsystem.DPSystemFactory;
import com.typesafe.config.Config;

import java.util.*;
import org.apache.commons.lang.*;

/**
 * Created by jim on 7/1/2015.
 */

/*

 CLASS QUERY implements specific API interfaces
 to handle the semantics of accessing an interface

 */

public abstract class QueryParser extends Configurable {

    private String queryTemplate=null;
    private String formattedQuery = null;
    private int dateDayOffset=-1;

    public void setConfig(Config _conf, Config _master) throws Exception {
        super.setConfig(_conf,_master);

        queryTemplate = config.getString(QUERY_TEMPLATE_LABEL);

        if (config.hasPath(DATEDAYOFFSET_LABEL) == true)
            dateDayOffset = config.getInt(DATEDAYOFFSET_LABEL);

    }

    public void resetQuery()
    {
        formattedQuery = null;
    }

    public String getQueryTemplate()
    {
        return queryTemplate;
    }


    public void execute() throws Exception {
        formattedQuery =  processTemplate(getQueryTemplate());
    }


    private String formatQuery(String inQuery)
    {
        String newQuery=inQuery;

        // currently only support date substitution
        // check for substring @yesterdays date
        if (inQuery.contains(DATE_SUBSTITUTION_LABEL)){
            String yesterdaysDate = getAdjustedDate(0);
            String yesterdaysDateWithOffset = getAdjustedDate(dateDayOffset);

            newQuery=inQuery.replaceAll(DATE_SUBSTITUTION_WITH_OFFSET_LABEL, yesterdaysDateWithOffset);
            // replace date
            newQuery=newQuery.replaceAll(DATE_SUBSTITUTION_LABEL,yesterdaysDate);
        }
        logger.info("Executing SQL Statment:"+newQuery);

        return newQuery;
    }

    // REFACTOR
    // current default is to use sql Date format
    private String getAdjustedDate(int daysOffset) {
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DATE, daysOffset);
        logger.info("sqlquery: offset=" + daysOffset + " : cal=" + cal + " : ");
         cal.getTime();

        return new java.sql.Date(cal.getTime().getTime()).toString();
    }


    public static LinkedList<String> parse(String parseString)
    {
        // Simple flat parse for now, strictly sequential
        // refactor to full tree parse with nodes
        // e.g. ( A | B | C)
        LinkedList<String> taskList = new LinkedList<String>();
        List<String> tempList = Arrays.asList(parseString.trim().split("[|]"));

        for (int i=0; i < tempList.size(); i++)
            taskList.add(tempList.get(i).trim());

        return taskList;
    }


    // Tokenizer

    // Given a String tokenize it based upon
    // <functionStart>FunctionString<functioinEnd>
    // Default function Start & End  @@


    // not subclassing tokenisation all done within QueryParser

    private static List<TransformFunction> functionTokenize(String input, String endPointTokens)
    {
        String[] rawFuncs =  StringUtils.substringsBetween(input,endPointTokens,endPointTokens);

        List<TransformFunction> funcs = new LinkedList<>();
        // now create
        if (rawFuncs == null)
            return funcs;

        int index=0;
        for (String rawFunc : rawFuncs){
            // locate
            TransformFunction func = new TransformFunction();
            func.initByRaw(rawFunc);
            // locate position within original string -- these are redundant for replacement
            int startPos = StringUtils.indexOf(input,endPointTokens,index);
            index = startPos + endPointTokens.length();
            int endPos = StringUtils.indexOf(input, endPointTokens, index) + endPointTokens.length();
            func.setPosition(startPos);
            func.setEndPosition(endPos);
            index = endPos ;
            funcs.add(func);
        }
        return funcs;
    }


    // return a list of pramaters in the raw string

    public static String[] paramsTokenize(String rawInput)
    {
        String params[] = rawInput.split("\\s*,\\s*");

        return params;
    }

    private static CompiledTemplateFunctionSet compileTemplate(String rawTemplate) throws Exception
    {
        CompiledTemplateFunctionSet outTemplate = new CompiledTemplateFunctionSet(rawTemplate);


        List<TransformFunction> funcs = functionTokenize(rawTemplate,BOUNDARY_TOKENS);


        // build ordinal map, which should come from the rawTemplate
        // -- for now just use iterator sequences
        for (int i=0; i < funcs.size(); i++){
            List<TransformFunction> tflist = new ArrayList<>();
            tflist.add(funcs.get(i));
            outTemplate.addFunctions(i,tflist);
            logger.info("   compilingFn("+i+"):"+funcs.get(i).getName());
        }

        return outTemplate;
    }


    public static String processTemplate(String rawTemplate) throws Exception
    {

        //Need to compileConfig & parse the functions in the template for substitution
        //Compiled Template is only a container classes for the results
        //Actual compilation happens here.
        CompiledTemplateFunctionSet template = compileTemplate(rawTemplate);
        StringBuilder newRow = new StringBuilder(rawTemplate);

        // for each function in the rawTemplate, call it and replace it with the output
        for (List<TransformFunction> tfl: template.getFunctions().values()) {
            String resultString=null;

            // run all the functions over the field
            for (TransformFunction tf: tfl) {
                logger.info("replacing: filter=" + tf.getName());
                resultString = DPSystemFactory.CallFunction(tf, resultString);
            }
            newRow = replaceStatement(newRow, resultString, BOUNDARY_TOKENS, 0);
        }

        return newRow.toString();
    }


    public static DataSet processTemplateByDataSet (String rawTemplate, DataSet data) throws Exception
    {
        DataSet outSet = new DataSetTabular();

        //Need to compileConfig & parse the functions in the template for substitution
        //Compiled Template is only a container classes for the results
        //Actual compilation happens here.
        CompiledTemplateFunctionSet template = new CompiledTemplateFunctionSet(rawTemplate);

        if (data == null) {
            // process for a single set
            return outSet;
        }

        // Loop process the set
        List<List<String>> lines = data.getAsListOfColumns();

        for (List<String> columns : lines){

            String transformedLine = processTemplateByLine(template, columns);

        }

        return outSet;
    }

    private static String processTemplateByLine(CompiledTemplateFunctionSet functions, List<String> row){

        StringBuilder newRow = new StringBuilder(functions.template);
        int index =0;

        // 3. Loop over each column and positionally replace it
        for (int i = 0; i < row.size(); i++) {
            // get the matching function by ordinal
            List<TransformFunction> tfl = functions.functions.get(i);


            StringBuilder resultString = new StringBuilder(row.get(i));

            // run all the functions over the field
            for (TransformFunction tf: tfl) {
                logger.info("replacing: filter=" + tf.getName() + "," + row.get(i));
                resultString = replaceStatement(newRow,resultString.toString(),BOUNDARY_TOKENS,0);
            }

            newRow = resultString;
        }


        logger.info(" newRow="+newRow);

        return newRow.toString();

    }

    // replace string function
    private static StringBuilder replaceStatement(StringBuilder oldString,
                                                  String subString,
                                                  String endPointTokens,
                                                  int index)
    {
        int startPos = StringUtils.indexOf(oldString.toString(),endPointTokens,index);
        index = startPos + endPointTokens.length();
        int endPos = StringUtils.indexOf(oldString.toString(), endPointTokens, index) + endPointTokens.length();


        return oldString.replace(startPos, endPos, subString);
    }



}
