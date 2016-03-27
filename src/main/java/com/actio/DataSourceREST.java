package com.actio;

import com.actio.dpsystem.DPSystemFactory;
import com.typesafe.config.Config;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;

/**
 * Created by jim on 7/8/2015.
 */
public class DataSourceREST extends DataSource {

    // protected DataSet outputDataSet;

    private String getUrl() {
        return url;
    }

    private void setUrl(String url) {
        this.url = url;
    }

    private String url;
    private TaskTransform trans;
    private String user;
    private String password;

    private String getPassword() {
        return password;
    }

    private void setPassword(String password) {
        this.password = password;
    }

    private String getUser() {
        return user;
    }

    private void setUser(String user) {
        this.user = user;
    }

    private static final String CONTENT_TYPE = "application/json";

    @Override
    public void setConfig(Config _conf, Config _master) throws Exception {
        super.setConfig(_conf,_master);

        if (config.hasPath("url"))
            setUrl(config.getString("url"));

        // TODO ; fix this
        // ate a TransformTemplate if a template is declared
        if (config.hasPath(MERGE_TEMPLATE_LABEL))
            //trans = DPSystemFactory.newTransform(config,masterConfig);

        // Refactor to a credential class
        if (config.hasPath("credential"))
        {
            Config credConfig = config.getConfig("credential");

            setUser(credConfig.getString("user"));
            setPassword(credConfig.getString("password"));
        }

    }

    @Override
    public void extract() throws Exception {

        // trans.setDataSet(getDataSet());
        // trans.extract();
        // setDataSet(trans.getDataSet());
    }

    @Override
    public void load() throws Exception {

        // if transform has been defined run a transform over the dataset first
        if (trans != null) {
            trans.setDataSet(getDataSet());
            trans.execute();
            setDataSet(trans.getDataSet());
        }

        // do the load
        write(getDataSet());
    }

    @Override
    public void execute() throws Exception {

        // if transform has been defined run a transform over the dataset first
        if (trans != null) {
            trans.setDataSet(getDataSet());
            trans.execute();
            setDataSet(trans.getDataSet());
        }

        write(dataSet);
    }

    @Override
    public void write(DataSet data, String suffix)  throws Exception
    {
        //Process only for the Additional
        // case with the REST API for now -
        // this is protection to stop spamming
        if (suffix.contentEquals("_all"))
            write(data);
    }

    @Override
    public void write(DataSet data)  throws Exception
    {
        for (String line : data.getAsList()) {
            logger.info("Send:"+line);
            write(line);
        }
    }

    @Override
    public DataSet read(QueryParser queryParser)  throws Exception
    {
        throw new Exception(FUNCTION_UNIMPLEMENTED_MSG);
    }

    @Override
    public DataSet getDataSet() {
        return dataSet;
    }

    @Override
    public void setDataSet(DataSet set){
        dataSet = set;
    }

    @Override
    public DataSet getLastLoggedDataSet() throws Exception
    {
        throw new Exception(FUNCTION_UNIMPLEMENTED_MSG);
    }

    @Override
    public void LogNextDataSet(DataSet set) throws Exception
    {
        throw new Exception(FUNCTION_UNIMPLEMENTED_MSG);
    }


    private void write(final String data) {
        CloseableHttpClient httpClient = null;
        try {
            CredentialsProvider credentialsProvider = new BasicCredentialsProvider();

            credentialsProvider.setCredentials(AuthScope.ANY,
                    new UsernamePasswordCredentials(getUser(), getPassword()));

            httpClient = HttpClientBuilder.create()
                    .setDefaultCredentialsProvider(credentialsProvider).build();

            HttpPost postRequest = new HttpPost(getUrl());

            StringEntity input = null;
            input = new StringEntity(data);
            input.setContentType(CONTENT_TYPE);

            postRequest.setEntity(input);
            HttpResponse response = httpClient.execute(postRequest);

            logger.info("REST Service Returned:="+response.getStatusLine().getStatusCode());

        } catch (UnsupportedEncodingException e) {
            logger.info( "Unable to send data to xMatters" + e);
        } catch (ClientProtocolException e) {
            logger.info("Unable to send data to xMatters"+ e);
        } catch (IOException e) {
            logger.info("Unable to send data to xMatters"+ e);
        } finally {
            if (httpClient != null) {
                try {
                    httpClient.close();
                } catch (IOException e) {
                    logger.info("Unable close the Connection"+ e);
                }
            }
        }
    }


}
