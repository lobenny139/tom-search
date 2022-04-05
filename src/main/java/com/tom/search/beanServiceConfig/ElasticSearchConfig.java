package com.tom.search.beanServiceConfig;

import com.tom.search.service.IDocService;
import com.tom.search.service.IIndexService;
import com.tom.search.service.provider.DocService;
import com.tom.search.service.provider.IndexService;
import lombok.Getter;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@Getter
public class ElasticSearchConfig   {

    @Value("${elasticsearch.host}")
    private String ESHost;

    @Value("${elasticsearch.port}")
    private int ESPort;

    // ---------------------- 在這註冊你的資料表存取服務 -------------------

    @Bean(name = "restHighLevelClient", destroyMethod = "close")
//    public RestHighLevelClient restHighLevelClient() {
//        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(
//                //IP地址
//                new HttpHost(getESHost(), getESPort(), "http")
//        ));
//        return client;
//    }
    public RestHighLevelClient getRestHighLevelClient(){
        HttpHost http1 = new HttpHost(getESHost(), getESPort(), "http");
//        HttpHost http2 = new HttpHost("127.0.0.1", 9201, "http");
        RestClientBuilder restClientBuilder = RestClient.builder(http1);
//        RestClientBuilder restClientBuilder = RestClient.builder(http1, http2);
        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(restClientBuilder);
        return restHighLevelClient;
    }

    @Bean(name = "docService")
    public IDocService docService(){
        return new DocService();
    }

    @Bean(name = "indexService")
    public IIndexService searchService(){
        return new IndexService();
    }


}
