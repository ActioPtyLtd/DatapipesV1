package com.actio;

import com.typesafe.config.Config;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by dimitarpopov on 24/08/15.
 */

public class TransformTemplate extends TaskTransform {

    private String mergeTemplate;

    public void setConfig(Config _config, Config _master) throws Exception {
        super.setConfig(_config, _master);

        // Requires a template Definition

        mergeTemplate = config.getString(MERGE_TEMPLATE_LABEL);

    }


    // Default Execute
    @Override
    public void execute() throws Exception {

        // basic positional template merge

        mergeTemplateByPosition();
    }

    private void mergeTemplateByPosition() throws Exception
    {

        // 2. Loop over the dataSet
        List<List<String>> data = dataSet.getAsListOfColumns();
        List<String> newData = new ArrayList<>();

        for (List<String> row: data) {
            String newRow = mergeTemplate;

            // 3. Loop over each column and positionally replace it
            for (int i=0; i <row.size(); i++){
                String filter = String.format("@%03d",i);

                logger.info("replacing: filter="+filter+","+row.get(i));

                newRow = newRow.replaceAll(filter, row.get(i));
            }
            logger.info(newRow);
            newData.add(newRow);
        }

        // Create a New Data set Record to store the new results
        dataSet = new DataSetTabular();

        dataSet.setConfig(getConfig(),getMasterConfig());

        // Store the New Result in the DataSet!!!
        dataSet.set(newData);
    }




}

