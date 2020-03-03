package com.cloudera.se;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.Krb5HttpClientBuilder;
import org.apache.solr.client.solrj.impl.LBHttpSolrClient;
import org.apache.solr.client.solrj.request.DelegationTokenRequest;
import org.apache.solr.client.solrj.response.DelegationTokenResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;

import java.io.IOException;
import java.util.*;


/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws IOException, SolrServerException {
        final String jaasConf = args[0]; // e.g. jaas-client.conf
        final String zkEnsemble = args[1]; // e.g. nightly6x-1.nightly6x.root.hwx.site:2181,nightly6x-2.nightly6x.root.hwx.site:2181,nightly6x-3.nightly6x.root.hwx.site:2181/solr
        final String solrCollection = args[2];
        final String searchQuery = args[3];
        final String searchFilter = args[4];

        String zk[] = zkEnsemble.split("/");
        List zkList = Arrays.asList(zk[0].split(","));
        String zkRoot = "/" + zk[1];

        System.setProperty("java.security.auth.login.config", jaasConf);
        HttpClientUtil.setHttpClientBuilder(new Krb5HttpClientBuilder().getHttpClientBuilder(Optional.empty()));

        CloudSolrClient client = new CloudSolrClient.Builder(zkList, Optional.of(zkRoot)).build();
        DelegationTokenRequest.Get getToken = new DelegationTokenRequest.Get();
        DelegationTokenResponse.Get getTokenResponse = getToken.process(client);
        final String token = getTokenResponse.getDelegationToken();

        /* Delegation token Auth Mech provides faster performance
         * Rebuild the client to use Delegation token Auth
         */
        client = new CloudSolrClient.Builder(zkList, Optional.of(zkRoot))
                .withLBHttpSolrClientBuilder(new LBHttpSolrClient.Builder()
                        .withResponseParser(client.getParser())
                        .withHttpSolrClientBuilder( new HttpSolrClient.Builder()
                                .withKerberosDelegationToken(token)))
                .build();
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
