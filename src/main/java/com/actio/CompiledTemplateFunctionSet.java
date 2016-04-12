package com.actio;

import com.actio.dpsystem.DPSystemConfigurable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.List;

/**
 * Created by dimitarpopov on 26/10/2015.
 */

/*
 Request Functions by numeric Ordinal or Label


 */


public class CompiledTemplateFunctionSet extends DPSystemConfigurable {
    private static final Logger logger = LoggerFactory.getLogger(CompiledTemplateFunctionSet.class);

    public String getTemplate() {
        return template;
    }

    private void setTemplate(String template) {
        this.template = template;
    }

    public String template;

    public Map<String, List<TransformFunction>> getFunctions() {
        if (functions == null)
            functions = new HashMap<String, List<TransformFunction>>();
        return functions;
    }

    public void setFunctions(Map<String, List<TransformFunction>> functionsByLabel) {
        this.functions = functionsByLabel;
    }

    public Map<String, List<TransformFunction>> functions;

    public CompiledTemplateFunctionSet(String template)
    {
        setTemplate(template);
    }
    public CompiledTemplateFunctionSet() {}


    public void addFunctions(String key, List<TransformFunction> tfl){
        addFunctions(key,tfl,true);
    }

    private void addFunctions(String key, List<TransformFunction> tfl, boolean add){

        if (functions == null)
            functions = new HashMap<String, List<TransformFunction>>();

        if (functions.containsKey(key) && add == true){
            List<TransformFunction> tfstored = functions.get(key);

            tfstored.addAll(tfl);

            functions.put(key,tfstored);
        } else
            functions.put(key, tfl);
    }

    private void addFunctions(int key, List<TransformFunction> tfl, boolean add) throws Exception
    {
        String strKey = String.valueOf(key);
        addFunctions(strKey,tfl, add);
    }

    public void addFunctions(int key, List<TransformFunction> tfl) throws Exception {
        addFunctions(key,tfl,true);
    }

    public List<TransformFunction> getFunctions(int key) throws Exception {
        String strKey = String.valueOf(key);

        return functions.get(strKey);
    }

    public List<TransformFunction> getFunctions(String key) throws Exception {
        return functions.get(key);
    }

    public void dump()
    {
        logger.debug("----------------------------------------");
        for (String key : functions.keySet())
        {
            List<TransformFunction> tfl = functions.get(key);
            logger.debug("dump:: listing transform functions for key="+key);
            for (TransformFunction tf : tfl ){

                logger.debug(" tf.name="+tf.getName());

            }
        }

    }


}
