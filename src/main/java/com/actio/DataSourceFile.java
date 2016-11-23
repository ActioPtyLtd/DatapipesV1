package com.actio;

import com.typesafe.config.Config;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.UnmappableCharacterException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;

/**
 * Created by dimitarpopov on 24/08/15.
 */

public class DataSourceFile extends DataSource {

    private String type;
    private String directory;

    private int currentFileIncrement;
    private String lastFileName;
    private String generatedFilename;
    private String prefixDiffFile;
    private String filenameTemplate;
    private String readStrategy;

    private String getCompiledFilename() throws Exception {

        if (compiledFilename == null)
            compiledFilename = QueryParser.processTemplate(filenameTemplate);

        return compiledFilename;
    }

    public void setCompiledFilename(String compiledFilename) {
        this.compiledFilename = compiledFilename;
    }

    private String compiledFilename;

    //protected String filePrefix;
    //protected String filePostfix;

    protected String preamble;
    protected String header;
    private String behaviour;
    private String attribute;

    public void resetFilename() {
        generatedFilename = null;
    }

    @Override
    public void execute() throws Exception {
        extract();
    }

    @Override
    public void execute(DataSet dataSet) throws Exception {
        extract(dataSet);
    }

    public void extract() throws Exception {

        String fname = getFilename();

        // read the file into memory -- yeah yeah
        ArrayList<String> list = new ArrayList<String>();

        // execute the query
        logger.info("ReadFile: " + fname);
	if(behaviour.equals("DBF"))
            dataSet = new DataSetDBF(new FileInputStream(new File(fname)));
	else if(behaviour.equalsIgnoreCase(READ_JSON_FROM_FILE))
            dataSet = Data2Json.fromFileStream2Json2Data(attribute, new FileInputStream(new File(fname)));
        else if(behaviour.equals("dump"))
            dataSet = DataSetDumpFile$.MODULE$.apply(this.config.getString("directory"), this.config.getString("regex"));
        else
            dataSet = new DataSetFileStream(new FileInputStream(new File(fname)));

        // save the results

        //dataSet.setConfig(config, masterConfig);

        logger.info("done");
    }

    @Override
    public void extract(DataSet dataSet) throws Exception {
        //if(dataSet.sizeOfBatch()>0) {
        //    Data d = dataSet.next();
        //    directory = d.apply("data").apply(0).apply("directory").valueOption().get();
        //    compiledFilename = d.apply("data").apply(0).apply("filename").valueOption().get();
       // }
        extract();
    }

    public void load() throws Exception {
        throw new Exception(FUNCTION_UNIMPLEMENTED_MSG);
    }


    @Override
    public DataSet read(QueryParser queryParser) throws Exception {
        throw new Exception("Not implemented");
    }

    // Generate Filename from parameters
    @Override
    public void setConfig(Config _conf, Config _master) throws Exception {
        super.setConfig(_conf, _master);
        // Optional fields -- MUST SET A DEFAULT
        if (config.hasPath(BEHAVIOR_LABEL) == true)
            behaviour = config.getString(BEHAVIOR_LABEL);
        else
            behaviour = BASIC_LOAD_LABEL;

        //mandatory fields will throw exception if not found
        directory = config.getString(DIRECTORY_LABEL);
        type = config.getString(TYPE_LABEL);

        if (behaviour.equalsIgnoreCase("checkpointDiff"))
            prefixDiffFile = config.getString("prefixDiffFile");
        else
            filenameTemplate = config.getString(FILENAME_TEMPLATE_LABEL);


        if (config.hasPath(FILENAME_TEMPLATE_LABEL) == true)
            filenameTemplate = config.getString(FILENAME_TEMPLATE_LABEL);
        else
            filenameTemplate = DEFAULT_FILENAME_LABEL;


        if(config.hasPath("header"))
            header = config.getString("header");
        else
            header = "";

        if(config.hasPath(ATTRIBUTE_LABEL))
            attribute = config.getString(ATTRIBUTE_LABEL);
        else
            attribute = "";

        // General Init
        generatedFilename = null;
                /* no op */
        dataSet = new DataSetTabular() {
        };
    }

    @Override
    public void execute(DataSet ds, String query) throws Exception {

    }

    @Override
    public DataSet executeQuery(DataSet ds, String query) throws Exception {
        extract();
        return dataSet;
    }

    @Override
    public DataSet getLastLoggedDataSet() throws Exception {
        // from directory

        initialiseDeltaFiles();
        List<String> file = fileToLines(lastFileName);
        DataSetTabular returnSet = new DataSetTabular();
        returnSet.set(file);

        return returnSet;
    }

    @Override
    public void LogNextDataSet(DataSet set) throws Exception {

        // May need to change this to increment properly
        write(set);
    }

    @Override
    public void write(DataSet data) throws Exception {
        String outFileName = getFilename();

        if(!header.isEmpty())
            WriteWorker(outFileName, data, header, preamble);
        else
            WriteWorker(outFileName, data, DataSetTableScala$.MODULE$.apply(data).getColumnHeaderStr(), preamble);
    }

    @Override
    public void write(DataSet data, String qualifier) throws Exception {
        List<String> rowList = DataSetTableScala$.MODULE$.apply(data).getAsList();

        logger.info("Entered writeListSet " + qualifier);
        if (rowList.isEmpty()) {
            logger.info(" list is empty no file created: " + qualifier);
            return;
        }

        String outFileName = generateFilenameByLabel(qualifier);
        logger.info("   DiffOutputFile=" + outFileName);

        if(!header.isEmpty())
            WriteWorker(outFileName, data, header, preamble);
        else
            WriteWorker(outFileName, data, DataSetTableScala$.MODULE$.apply(data).getColumnHeaderStr(), preamble);

    }

    private void WriteWorker(String outFileName, DataSet data,
                             String header, String preamble) throws Exception {

        List<String> rowList = DataSetTableScala$.MODULE$.apply(data).getAsList();

        logger.info("Writing File=" + getFilename() + "No.Lines=" + rowList.size());

        // open to a file
        Charset charset = Charset.forName("UTF-8");
        logger.info(outFileName);
        Path datafile = Paths.get(outFileName);

        if (!Files.exists(datafile))
            Files.createFile(datafile);

        BufferedWriter writer = Files.newBufferedWriter(datafile, charset, StandardOpenOption.APPEND);

        // print out the dataSource
        try {
            // write preamble header
            if (preamble != null) {
                writer.write(preamble);
                writer.newLine();
            }
            // write preamble header
            if (header != null) {
                writer.write(header);
                writer.newLine();
            }

            for (String columns : rowList) {
                try {
                    writer.write(columns);
                    writer.newLine();
                } catch (UnmappableCharacterException e) {
                    logger.info("ERROR:unmappable character expressoion");
                }
            }
            writer.close();
        } catch (UnmappableCharacterException e) {
            logger.info("ERROR:unmappable character expressoion");
        }

    }

    @Override
    public String getConnectStr() throws Exception {
        return getFilename();
    }

    // =======================================================================================
    //
    //
    //
    // =======================================================================================


    private String getFilename() throws Exception {

        if (generatedFilename != null)
            return generatedFilename;

        // process file template to generate a filename
        // search through supported labels to determine format

        if (matchLabel(behaviour, CHECKPOINT_DIFF_LABEL))
            initialiseDeltaFiles();
        else
            generateFilenameByLabel(null);

        return generatedFilename;
    }

    private void initialiseDeltaFiles() throws IOException {
        // delta logfiles are of the form 'fixed'__0001__.csv
        // number should always increment and should get the last number in the sequence

        // set the following variables
        // CurrentFileIncrement
        // LastFileName
        // NextFileName

        // loop over applicable filenames looking for last one - also store previous at this point
        OptionalInt currentMaxFileNo = Files.list(Paths.get(directory)).mapToInt(filePath -> {
            int val = -1;
            if (Files.isRegularFile(filePath)) {

                String[] fileTokens = filePath.getFileName().toString().split("__");

                // get the number and check the prefixs match
                if (fileTokens.length < 2)
                    return 0;

                String filePrefix = fileTokens[0];

                if (filePrefix.equals(prefixDiffFile) && isInteger(fileTokens[1], 10)) {
                    val = Integer.parseInt(fileTokens[1]);
                }
            }
            return val;
        }).max();

        currentFileIncrement = currentMaxFileNo.isPresent() ? currentMaxFileNo.getAsInt() : 0;
        logger.info(" CurrentFileIncrement=" + currentFileIncrement);

        lastFileName = generateBaseFilename(currentFileIncrement);
        generatedFilename = generateBaseFilename(currentFileIncrement + 1);

        logger.info("*** last=" + lastFileName + " next=" + generatedFilename);
    }

    // =======================================================================================
    //
    //
    //
    // =======================================================================================

    private String generateBaseFilename(int increment) {
        // take out the @datetime if its there
        return directory + "/" + prefixDiffFile + "__" + String.format("%04d", increment) + "__.dat";
    }

    private String generateFilenameByLabel(String qualifier) throws Exception {

        generatedFilename = directory + FS + getCompiledFilename() +
                ((qualifier == null) ? "" : qualifier);

        return generatedFilename;
    }

    //=============================================================
    private static List<String> fileToLines(String filename) {
        List<String> lines = new LinkedList<String>();
        String line = "";
        BufferedReader in = null;
        try {
            in = new BufferedReader(new FileReader(filename));
            while ((line = in.readLine()) != null) {
                lines.add(line);
            }
        } catch (IOException e) {
            logger.info("File not found - first diff entry");
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    // ignore ... any errors should already have been
                    // reported via an IOException from the final flush.
                }
            }
        }
        return lines;
    }

}
