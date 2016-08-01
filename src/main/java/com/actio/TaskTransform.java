package com.actio;

import com.actio.dpsystem.DPFnNode;
import com.actio.dpsystem.DPSystemConfig;
import com.actio.dpsystem.DPSystemFactory;

import java.util.List;
import java.util.Objects;

/**
 * Created by dimitarpopov on 24/08/15.
 */
public class TaskTransform extends Task {

    protected List<List<String>> rows = null;
    protected DataSet outputDataSet;
    private String transformBehavior= null;
    private String tranRegexp = null;
    private List<Integer> fields = null;
    private List<Integer> validColumns = null;

    public List<Integer> getValidColumns() {
        return validColumns;
    }

    private void setValidColumns(List<Integer> validColumns) {
        this.validColumns = validColumns;
    }

    public String getTranRegexp() {
        return tranRegexp;
    }

    public void setTranRegexp(String tranRegexp) {
        this.tranRegexp = tranRegexp;
    }

    public List<Integer> getFields() {
        return fields;
    }

    private void setFields(List<Integer> fields) {
        this.fields = fields;
    }

    public String getTransformBehavior() {
        return transformBehavior;
    }

    private void setTransformBehavior(String _behavior) {
        this.transformBehavior = _behavior;
    }

    public void setNode(DPFnNode _node, DPSystemConfig _sysconf) throws Exception
    {
        sysconf = _sysconf;
        super.setConfig(sysconf.getTaskConfig(_node.getName()).toConfig(),sysconf.getMasterConfig());
        node = _node;
        logger.debug("setNode::"+node.getName());

        if (config.hasPath(BEHAVIOR_LABEL))
            setTransformBehavior(config.getString(BEHAVIOR_LABEL));

        if (config.hasPath(VALIDATE_PHONE_COLUMNS_LABEL))
            setFields(config.getIntList("phoneColumns"));

        if (config.hasPath("validateColumnCount"))
            setValidColumns(config.getIntList("validateColumnCount"));
        setInstanceID(getUUID());
        logger.info("---RUNID=" + getRunID() + "----INSTANCEID=" + getInstanceID() + ".");
    }

    public void extract() throws Exception {
        throw new Exception(FUNCTION_UNIMPLEMENTED_MSG);
    }

    public void load() throws Exception {
        throw new Exception(FUNCTION_UNIMPLEMENTED_MSG);
    }

    // Default Execute
    @Override
    public void execute() throws Exception {
        // setDataSet(transform(getDataSet()));

        setDataSet(process_transforms(getDataSet()));

    }

    // =================================================================
    // transform according to configuration
    private DataSet process_transforms(DataSet inputDataSet) throws Exception {

        // Get the TaskTransform Node

        // compileConfig transform list by column Reference
        if (config.hasPath(COLUMN_LABEL) != true && config.hasPath(BATCH_FUNCTIONS_LABEL) != true &&
                config.hasPath(ROW_FUNCTIONS_LABEL) != true) {

            logger.warn("TASK has no valid Transforms");
            return null;
        }
        // process for columns

        CompiledTemplateFunctionSet fs = DPSystemFactory.newFunctionSet(config);
        fs.dump();

        // built compiled set of functions now run them
       return processTransforms(inputDataSet,fs);
    }

    private DataSet processTransforms(DataSet inputDS, CompiledTemplateFunctionSet fs)
            throws Exception
    {
        DataSet newDataSet = null;

        // iterate over the transforms and apply them to the matching key
        // in the dataset

        newDataSet = StaticUtilityFunctions.execute(inputDS,fs);

        return newDataSet;
    }


    // =================================================================

    class WrapColumns {
        private final int keyColumn = 1;
        private List<String> columns;

        public WrapColumns(List<String> columns) {
            this.columns = columns;
        }

        public List<String> getColumns() {
            return columns;
        }

        public void setColumns(List<String> columns) {
            this.columns = columns;
        }

        public List<String> unwrap() {
            return this.columns;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            WrapColumns that = (WrapColumns) o;
            return Objects.equals(columns.get(keyColumn), that.columns.get(keyColumn));
        }

        @Override
        public int hashCode() {
            return Objects.hash(columns.get(keyColumn));
        }
    }


}

