package com.actio;

/**
 * Created by jim on 7/8/2015.
 */

import com.actio.dpsystem.DPSystemConfigurable;
import com.typesafe.config.Config;
import difflib.Delta;
import difflib.DiffUtils;
import difflib.Patch;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

// REFACTOR - current get mechanism is extremely inefficient for mutiple reads
// need to cache the result

public class DiffSet extends DPSystemConfigurable {

    private final List<Delta<String>> addLines = new LinkedList<>();
    private final List<Delta<String>> modifyLines = new LinkedList<>();
    private final List<Delta<String>> deleteLines = new LinkedList<>();

    public DiffSet(){}

    private void ToAdd(Delta val) {
        addLines.add(val);
    }

    private void ToDelete(Delta val) {
        deleteLines.add(val);
    }

    private void ToModify(Delta val) {
        modifyLines.add(val);
    }


    public DataSet getChangedList() throws Exception
    {
        DataSet add = getAddList();
        DataSet mod = getModList(true);
        List<String> combinedList = add.getAsList();

        combinedList.addAll(mod.getAsList());

        return new DataSetTabular(combinedList);
    }

    public DataSet getAddList() {
        List<String> thelist = new LinkedList<>();
        for (Delta d : addLines) {
            for (Object line : d.getRevised().getLines())
                thelist.add((String) line);
        }
        return new DataSetTabular(thelist);
    }

    public DataSet getDelList() {
        List<String> thelist = new LinkedList<>();
        for (Delta d : deleteLines) {
            for (Object line : d.getOriginal().getLines())
                thelist.add((String) line);
        }
        return new DataSetTabular(thelist);
    }

    public DataSet getModList(boolean getRevised) {
        List<String> thelist = new LinkedList<>();
        for (Delta d : modifyLines) {
            List<Objects> objList;

            if (getRevised)
                objList = d.getRevised().getLines();
            else
                objList = d.getOriginal().getLines();

            for (Object line : objList)
                thelist.add((String) line);
        }
        return new DataSetTabular(thelist);
    }

    public void setConfig(Config _conf, Config _master) throws Exception {
        super.setConfig(_conf,_master);
    }

    public void dump() throws Exception
    {
        logger.info("dump addLines");
        for (Delta<String> line : addLines){
            logger.info("--"+line.getRevised());
        }

        logger.info("dump modifyLines");
        for (Delta<String> line : modifyLines){
            logger.info("--"+line.getRevised());
        }

        logger.info("dump deleteLines");
        for (Delta<String> line : deleteLines){
            logger.info("--"+line.getRevised());
        }
    }

    private Patch<String> diff(List<String> original, List<String> revised)
    {
        logger.info("DIFF Process : "+original.size()+" revised:"+revised.size());
        // Compute diff. Get the Patch object. Patch is the container for computed deltas.
        Patch<String>patch = DiffUtils.diff(original, revised);
        return patch;
    }

    public void trackDiffs(DataSet original, DataSet revised, int depth) throws Exception {

        Patch<String> patch = diff(original.getAsList(),revised.getAsList());

        logger.info(" depth=" + depth);

        if (depth > 5)
            throw new Exception("Too Deep");

        for(Delta<String>delta:patch.getDeltas()){
            int OrigLinesCount = delta.getOriginal().size();
            int NewLineCount = delta.getRevised().size();

            if (OrigLinesCount > 0 && NewLineCount == 0) {
                // lines were deleted from this chunk write to deteted
                logger.info("DELETED LINES:-("+OrigLinesCount+","+NewLineCount+")");

                ToDelete(delta);
            }
            else if (OrigLinesCount == 0 && NewLineCount > 0) {
                // lines were added
                logger.info("ADDED LINES:-("+OrigLinesCount+","+NewLineCount+")");

                ToAdd(delta);
            }
            else if (OrigLinesCount == NewLineCount){
                // lines were equal so just modified
                logger.info("DELTA:Original:("+OrigLinesCount+","+NewLineCount+")");
                ToModify(delta);
            } else {
                logger.info("Need sub DIFF to determine exact changes");

                logger.info("Modified:: "+
                        delta.getOriginal().getLines().size()+":"+
                        delta.getRevised().getLines().size());
                ToModify(delta);
                //tracker = trackDiffs(delta.getOriginal().getLines(),
                //        delta.getRevised().getLines(), tracker, depth+1);
            }
        }
    }

}
