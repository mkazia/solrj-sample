package com.cloudera.se;

import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.config.Lookup;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.cookie.CookieSpecProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.*;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;


/**
 * Hello world!
 *
 */
public class BasicAuthApp
{
    public static void main( String[] args ) throws IOException, SolrServerException {
        final String jaasConf = args[0]; // e.g. jaas-client.conf
        final String zkEnsemble = args[1]; // e.g. nightly6x-1.nightly6x.root.hwx.site:2181,nightly6x-2.nightly6x.root.hwx.site:2181,nightly6x-3.nightly6x.root.hwx.site:2181/solr
        final String solrCollection = args[2];
        final String searchQuery = args[3];
        final String searchFilter = args[4];
        final String username = args[5];
        final String password = args[6];

        String[] zk = zkEnsemble.split("/");
        List zkList = Arrays.asList(zk[0].split(","));
        String zkRoot = "/" + zk[1];

        //System.setProperty("java.security.auth.login.config", jaasConf);

        HttpClientUtil.setCookiePolicy(SolrPortAwareCookieSpecFactory.POLICY_NAME);
        HttpClientUtil.getHttpClientBuilder()
                .setDefaultCredentialsProvider(() -> {
                    CredentialsProvider credsProvider = new BasicCredentialsProvider();
                    credsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
                    return credsProvider;
                })
                .setCookieSpecRegistryProvider(() -> {
                    SolrPortAwareCookieSpecFactory cookieFactory = new SolrPortAwareCookieSpecFactory();
                    Lookup<CookieSpecProvider> cookieRegistry = RegistryBuilder.<CookieSpecProvider> create()
                            .register(SolrPortAwareCookieSpecFactory.POLICY_NAME, cookieFactory).build();

                    return cookieRegistry;
                });

        CloudSolrClient client = new CloudSolrClient.Builder(zkList, Optional.of(zkRoot)).build();


        client.setDefaultCollection(solrCollection);

        final SolrQuery query = new SolrQuery(searchQuery);
        query.addFilterQuery(searchFilter);
        query.setFields("id", "text");
        query.setStart(0);
        query.setRows(5);

        final QueryResponse response = client.query(solrCollection, query);
        final SolrDocumentList documents = response.getResults();

        System.out.println("Num docs found: " + documents.getNumFound());
        for(SolrDocument document : documents) {
            System.out.println("id: " + document.getFieldValue("id")
                    + " text: " + document.getFieldValue("text"));
        }
    }
}
