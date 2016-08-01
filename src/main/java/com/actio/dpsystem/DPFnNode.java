package com.actio.dpsystem;

/**
 * Created by jim on 1/03/2016.
 */


//import com.jcabi.aspects.Loggable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class DPFnNode {

    private static final Logger logger = LoggerFactory.getLogger(DPFnNode.class);
    //private Map<String, DPFnNode> nodes = new HashMap<>();
    private final LinkedList<DPFnNode> nodes = new LinkedList<>();
    public String name = null;
    public boolean parallel = false;
    private String runID;
    private String instanceID;
    private String type;

    public DPFnNode(String name, String type)
    {
        this.name = name;
        this.type = type;
    }

    private DPFnNode() {}

    private static void dumpOLD(DPFnNode node, int depth, String indent)
    {
        logger.info(depth+indent+"::"+node.name+"::"+node.type+","+node.parallel);

        if (node.nodes.isEmpty())
            return;
        else
            logger.info(indent+"{");

        for (DPFnNode n : node.nodes)
        {

            logger.info(indent+" \""+n.name+"\" : " );

                dump(n,++depth,indent+"   ");
        }
        logger.info(indent+"}");
    }

    private static void dump(DPFnNode node, int depth, String indent)
    {
        System.out.print(indent + "{ \"" + node.name + "::" + node.getInstanceID() + "\" : ");

        if (node.nodes.size() == 0)
            return ;
        System.out.print("[");

        boolean first = true;
        for (DPFnNode n : node.nodes)
        {
            if (first)
                first = false;
            else
                System.out.print(", ");

            if (n.nodes.size() > 0)
                dump(n,++depth,indent+"   ");
            else
                System.out.print(" \"" + n.getName() + "::" + n.getInstanceID() + "\" ");

        }

        System.out.print("] }");
    }

    public String getRunID() {
        return runID;
    }

    public void setRunID(String runID) {
        this.runID = runID;
    }

    public String getInstanceID() {
        return instanceID;
    }

    public void setInstanceID(String instanceID) {
        this.instanceID = instanceID;
    }

    public LinkedList<DPFnNode> getNodeList() {
        return nodes;
    }

    public void add(DPFnNode n) {
        nodes.add(n);
    }

    public DPFnNode duplicate() {

        return new DPFnNode(getName(), getType());
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    public void dump() {

        System.out.print(" \"RUNID::" + getRunID() + "::\" ");
        dump(this, 0, "");
        System.out.println();
    }

}
