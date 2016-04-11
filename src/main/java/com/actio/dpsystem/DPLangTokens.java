package com.actio.dpsystem;

/**
 * Created by jim on 3/03/2016.
 */

import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;
import java.util.regex.Pattern;

public class DPLangTokens extends DPSystemConfigurable {

    public static final char LeftFunctionStart ='(';
    public static final char RightFunctionStart =')';

    public static final char LeftPipeStart ='{';
    public static final char RightPipeStart ='}';

    public static final char PipeJoin = '|';
    public static final char PipeParallel = ',';

    private static final char EscapeChar = '\\';

    private static final char Quote = '\'';
    public static final char LeftQuote = '\'';
    public static final char RightQuote = '\'';
    public static final boolean identicalQuotes = true;

    public static final String LiteralType = "literal";
    private static final String LabelType = "label";
    private static final String SymbolType = "symbol";

    private String raw;
    private List<String> tokens;
    private List<String> types;

    private int currentPos;
    private int maxPos;

    private void add(String token, String type)
    {
        tokens.add(token);
        types.add(type);
    }

    public int current()
    {
        return currentPos;
    }

    public int max(){
        return maxPos;
    }

    public String getToken()
    {
        if (currentPos >= maxPos)
            return null;
        return tokens.get(currentPos);
    }

    public String getType()
    {
        if (currentPos >= maxPos)
            return null;
        return types.get(currentPos);
    }

    public int init()
    {
       return  currentPos = 0;
    }

    public String nextToken()
    {
        if (currentPos >= maxPos)
            return null;

        return tokens.get(currentPos++);
    }

    public int increment()
    {
        if (currentPos >= maxPos)
            return -1;
        else
            return ++currentPos;
    }

    public DPLangTokens moveNextToken()
    {
        increment();
        return this;
    }

    public int skip(int offset)
    {
        return currentPos = bound(currentPos + offset);
    }

    public String lookAhead(int offset)
    {
        int tempPos = bound(currentPos + offset);

        return tokens.get(tempPos);
    }

    public String look(int index)
    {
        return tokens.get(bound(index));
    }

    private int bound(int val){
        if (val < 0) return 0;
        if (val > maxPos) return maxPos;
        return val;
    }


    // TODO does not work - resolve regex issue
    public int tokenise(String parseString){
        tokens = new LinkedList<>();
        currentPos = 0;


        logger.info("tokenizing="+parseString);

        Pattern p =  Pattern.compile("[\\s]+|[(]|[a-z0-9\\_\\-]*|[)]|[{]|[}]|[|]|[,]");

        Scanner sc = new Scanner(parseString);

        while (sc.hasNext(p)) {
            String token = sc.next(p);
            // TODO : we want to handle escaping of quotesmand other characters in the token stream and return a single token

            logger.info(token);
            tokens.add(token);

        }
        maxPos = tokens.size();
        return maxPos;
    }


    public int tokeniseBrute(String parseString) throws Exception
    {
        tokens = new LinkedList<>();
        types = new LinkedList<>();

        int i = 0;
        raw = parseString;

        boolean escaping = false;
        boolean quoted = false;
        boolean literal = false;
        boolean nonConsumeTerminator = false;

        StringBuffer buff = new StringBuffer();

        while (i < parseString.length()){
            char c = parseString.charAt(i++);

            // === section 1 tests
            if (c == EscapeChar) {
                escaping = true;
                continue;
            }

            if (c == Quote) {
                quoted = true;
                continue;
            }

            //logger.info("processing c='"+c+"' current buff="+buff);
            // === section 2 tests
            if (literal == true || (quoted == true && literal == true)) {

                if (c == RightFunctionStart ||
                        c == LeftFunctionStart ||
                        c == RightPipeStart ||
                        c == LeftPipeStart ||
                        c == PipeJoin ||
                        c == PipeParallel){

                    nonConsumeTerminator = true;
                }
                if ((c == Quote && escaping == false) || ((c == ' ' || c == '\t') && quoted == false) || nonConsumeTerminator) {
                    quoted = false;
                    literal = false;
                    // save token


                    // quoted strings are typed literals all others are label
                    add(buff.toString(), (c==Quote)? LiteralType : LabelType);

                    buff = new StringBuffer();

                    if (nonConsumeTerminator == false)
                        continue;
                    else
                        nonConsumeTerminator = false;
                } else {
                    buff.append(c);
                    escaping = false;
                    continue;
                }
            }

            // section 3 tests
            if (c == ' ' || c == '\t')
                continue;
            else if (c == RightFunctionStart ||
                    c == LeftFunctionStart ||
                    c == RightPipeStart ||
                    c == LeftPipeStart ||
                    c == PipeJoin ||
                    c == PipeParallel)
            {
                add(String.valueOf(c), SymbolType);
            } else if (Character.isLetterOrDigit(c) || c == '_' || c=='-') {
                literal = true;
                buff.append(c);
            }
            else{
                logger.info("Unknown char=='"+c+"'");
            }
        }
        // check for last token
        if (literal == true || (quoted == true && literal == true)) {
            add(buff.toString(),(quoted == true)? LiteralType : LabelType);
        }
        dump();
        return maxPos=tokens.size();
    }

    private void dump()
    {   int i=0;
        logger.info("===================='"+raw+"'");
        for (String token : tokens){
            logger.info("    ("+ i++ +")"+"'"+token+"'");
        }
    }

}

