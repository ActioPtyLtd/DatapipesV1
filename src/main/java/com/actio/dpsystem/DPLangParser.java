package com.actio.dpsystem;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by jim on 4/03/2016.
 */
class DPLangParser {

    protected static final Logger logger = LoggerFactory.getLogger(DPLangParser.class);

    private static final int READ_PARAMS_STATE = 2;
    private static final int READ_FUNCTION_STATE = 1;
    private static final int END_READ_STATE = 3;

    public static DPFnNode parse(DPLangTokens tokens, DPSystemConfig sysconf) throws Exception
    {
        DPFnNode root = new DPFnNode(DPSystemConfigurable.PIPE_LABEL, DPSystemConfigurable.PIPE_LABEL);
        tokens.init();

        return parseNewNode(READ_PARAMS_STATE,root,tokens,0,true,sysconf);
    }


    private static DPFnNode parseNewNode(int currentState, DPFnNode parent, DPLangTokens tokens, int currentDepth, boolean isPipeMode, DPSystemConfig sysconf)
            throws Exception
    {
        // loop over the tokens

        // logger.info("Parse::Processing Tokens count="+tokens.max()+" currentToken="+tokens.getToken()+" depth="+currentDepth);

        if (currentDepth > tokens.max()) {
            logger.info("ran out of tokens to process");
            return parent;
        }
        // Begin Function
        while (tokens.getToken() != null && currentState != END_READ_STATE) {

            if (tokens.getToken().compareTo(String.valueOf(DPLangTokens.LeftFunctionStart)) == 0){
                // Create a new Node, with a pipeline function
                DPFnNode newChild = new DPFnNode(DPSystemConfigurable.PIPE_LABEL, DPSystemConfigurable.PIPE_LABEL, sysconf);

                currentState = READ_PARAMS_STATE;
                parent.add( parseNewNode(READ_FUNCTION_STATE,newChild,tokens.moveNextToken(),++currentDepth,false,sysconf));
                continue;
            }
            else if (tokens.getToken().compareTo(String.valueOf(DPLangTokens.RightFunctionStart)) == 0){
                // terminator for a pipeline function

                // assertion check - for balanced dpipe terminators
                if (currentDepth - 1 < 0)
                {
                    // too many braces throw exception
                    throw new Exception("unbalanced function braces: token Position= "+tokens.current());
                }
                tokens.increment();
                return parent;
            }
            else if (tokens.getToken().compareTo(String.valueOf(DPLangTokens.LeftPipeStart)) == 0){

                // Create a new Node, with a pipeline function
                DPFnNode newChild = new DPFnNode(DPSystemConfigurable.PIPE_LABEL, DPSystemConfigurable.PIPE_LABEL,sysconf);

                parent.add( parseNewNode(READ_PARAMS_STATE,newChild,tokens.moveNextToken(),currentDepth + 1,true,sysconf));
                continue;

            } else if (tokens.getToken().compareTo(String.valueOf(DPLangTokens.RightPipeStart)) == 0){
                // terminator for a pipeline function

                // assertion check - for balanced dpipe terminators
                if (currentDepth - 1 < 0)
                {
                    // too many braces throw exception
                    throw new Exception("unbalanced dpipe braces: token Position= "+tokens.current());
                }
                tokens.increment();
                return parent;
            }


            if (tokens.getToken().compareTo(String.valueOf(DPLangTokens.PipeJoin)) == 0) {
                parent.parallel = false;
                tokens.increment();
            }
            else if (tokens.getToken().compareTo(String.valueOf(DPLangTokens.PipeParallel)) == 0) {
                parent.parallel = true;
                tokens.increment();
            }else if (currentState == READ_FUNCTION_STATE)
            {
                // set the function name for parent node with the current token

                parent.name = tokens.getToken();
                tokens.increment();
                currentState = READ_PARAMS_STATE;
            }
            else if (currentState == READ_PARAMS_STATE)
            {
                // reading through function parameters
                String name = tokens.getToken();
                String type = tokens.getType();
                String nodeType = null;

                // TODO : need to check if literal or Task or Function Name or PipeName

                if (type == DPLangTokens.LiteralType)
                    nodeType = DPLangTokens.LiteralType;
                else {
                    nodeType = sysconf.getType(name);
                }
                tokens.increment();
                DPFnNode newChild = new DPFnNode(name, nodeType,sysconf);
                parent.add(newChild);
            }
            else
            {
                // unknown state

                // too many braces throw exception
                throw new Exception(" Unknown State :token Position= "+tokens.current());
            }

        }
        return parent;
    }




}
