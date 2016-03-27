package com.actio;

import com.typesafe.config.Config;
import java.util.List;

/**
 * Created by jim on 7/1/2015.
 */

/*

 CLASS QUERY implements specific API interfaces
 to handle the semantics of accessing an interface

 */

public class TransformFunction {

    private String type;
    private String name;

    private int endPosition=-1;
    private int position=-1;

    public int getEndPosition() {
        return endPosition;
    }

    public void setEndPosition(int endPosition) {
        this.endPosition = endPosition;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getName() {
        return name;
    }

    private void setName(String name) {
        this.name = name;
    }

    public int getPosition() {
        return position;
    }

    public void setPosition(int position) {
        this.position = position;
    }

    public String[] getParameters() {
        return parameters;
    }

    private void setParameters(String[] parameters) {
        this.parameters = parameters;
    }

    private String getRaw() {
        return raw;
    }

    private void setRaw(String raw) {
        this.raw = raw;
    }

    public void initByRaw(String raw){
        setRaw(raw);

        String[] params = QueryParser.paramsTokenize(getRaw());
        setParameters(params);
        // the first param should always be the name of the function, set Name of function
        if (params.length >= 1)
            setName(params[0]);
    }

    private String[] parameters;
    private String raw;


}
