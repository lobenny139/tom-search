package com.tom.search.test;


import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetMappingsRequest;
import org.elasticsearch.client.indices.GetMappingsResponse;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = com.tom.search.TestApplication.class)
@TestPropertySource(locations = "/test-application.properties")
public class TestElasticSearchTemplate {

    @Autowired
    RestHighLevelClient restHighLevelClient;

    @Test
    public void indexExists() throws IOException {
        String indexName="magazine";

        GetIndexRequest request = new GetIndexRequest();
        request.indices(indexName);
            System.out.println( restHighLevelClient.indices().exists(request, RequestOptions.DEFAULT) );
//        } catch (IOException e) {
//            throw new ElasticsearchException("Error while for indexExists request: " + request.toString(), e);
//        }
    }

    @Test
    public void testCreateIndex() throws IOException {
        //http://127.0.0.1:9200/magazine/_mapping/field/column1,column2...
        Map<String, String> columnInfos = new HashMap<String, String>();
        columnInfos.put("createDate", "long");
        columnInfos.put("author", "text");
        columnInfos.put("title", "text");
        columnInfos.put("content", "text");

        String indexName="magazine";
        //

        CreateIndexRequest request = new CreateIndexRequest(indexName);
        request.settings(Settings.builder()
                // 分片数
                // .put("index.number_of_shards", 3)
                // 副本数
                // .put("index.number_of_replicas", 2)
                .put("analysis.analyzer.default.tokenizer", "standard") // 默认分词器
                .put("analysis.analyzer.default.filter", "cjk_bigram") // 默认分词器
        );

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.startObject("properties");
            {

                builder.startObject("createDate");
                {
                    builder.field("type", "long");
                }
                builder.endObject();

//                for (Object key : columnInfos.keySet()) {
//                    System.out.println(key + " : " + columnInfos.get(key));
//                    builder.startObject(key.toString());
//                    builder.field("type", columnInfos.get(key));
//                    builder.endObject();
//                }


                //
                builder.startObject("author");
                {
                    builder.field("type", "text");
                }
                builder.endObject();
                //
                builder.startObject("title");
                {
                    builder.field("type", "text");
                }
                builder.endObject();

                //
                builder.startObject("content");
                {
                    builder.field("type", "text");
                }
                builder.endObject();

            }
            builder.endObject();
        }
        builder.endObject();

        CreateIndexResponse response = restHighLevelClient.indices().create(request, RequestOptions.DEFAULT);
        System.out.println(response.isAcknowledged());
    }




    @Test
    public void testAddRecord() throws IOException {
        String indexName="magazine";
        XContentBuilder xcontent = XContentFactory.jsonBuilder().startObject()
                .field("content", "紐約大都會藝術博物館（Metropolitan Museum of Art，簡稱The Met）日前宣布，任命前總統馬英九女兒馬唯中為現當代藝術部首位亞洲藝術副策展人，將於2022年4月到任。對此，馬英九今（20）日受訪表示，這對女兒和大都會博物館來說都是正面發展，他樂觀其成。馬唯中2013年起任職於香港M+視覺文化博物館，擔任水墨策展人，今年4月將到任大都會藝術博物館現當代藝術部。大都會博物館方面日前對此表示，國際現當代藝術團隊有了馬唯中的加入，就能將東亞及東南亞藝術家非凡的創造力帶入館內，為本館造就令人拭目以待的良機。")
                .field("title", "馬唯中任大都會藝術博物館副策展人 馬英九：樂觀其成")
                .endObject();
        IndexRequest request = new IndexRequest(indexName).source(xcontent).id("1");
        IndexResponse response = restHighLevelClient.index( request, RequestOptions.DEFAULT);
        System.out.println(xcontent);
        System.out.println(request);
        System.out.println(response);
        System.out.println(response.getId());
        System.out.println(response.getIndex());
        if(response.getResult() == DocWriteResponse.Result.CREATED){
            System.out.println(DocWriteResponse.Result.CREATED);
        }else if(response.getResult() == DocWriteResponse.Result.UPDATED){
            System.out.println(DocWriteResponse.Result.UPDATED);
        }
    }

    @Test
    public void testGetIndexFieldsInfo() throws IOException {
        String indexName="magazine";
        //指定索引
        GetMappingsRequest getMappings = new GetMappingsRequest().indices(indexName);
        //调用获取
        GetMappingsResponse getMappingResponse = restHighLevelClient.indices().getMapping(getMappings, RequestOptions.DEFAULT);
        //处理数据
        Map<String, MappingMetaData> allMappings = getMappingResponse.mappings();
        List<Map<String, Object>> mapList = new ArrayList<>();
        for (Map.Entry<String, MappingMetaData> indexValue : allMappings.entrySet()) {
            Map<String, Object> mapping = indexValue.getValue().sourceAsMap();
            Iterator<Map.Entry<String, Object>> entries = mapping.entrySet().iterator();
            entries.forEachRemaining(stringObjectEntry -> {
                if (stringObjectEntry.getKey().equals("properties")) {
                    Map<String, Object> value = (Map<String, Object>) stringObjectEntry.getValue();
                    for (Map.Entry<String, Object> ObjectEntry : value.entrySet()) {
                        Map<String, Object> map = new HashMap<>();
                        String key = ObjectEntry.getKey();
                        Map<String, Object> value1 = (Map<String, Object>) ObjectEntry.getValue();
                        map.put(key, value1.get("type"));
                        mapList.add(map);
                    }
                }
            });
        }
        ObjectMapper objectMapper = new ObjectMapper();
        System.out.println(objectMapper.writeValueAsString(mapList));
    }

    @Test
    public void testShowAllIndex() throws IOException {
        //http://127.0.0.1:9200/_cat/indices
        GetIndexRequest getIndexRequest = new GetIndexRequest().indices("*");
        GetIndexResponse getIndexResponse = restHighLevelClient.indices().get(getIndexRequest, RequestOptions.DEFAULT);
        String[] indices = getIndexResponse.getIndices();
        for (int i = 0; i < indices.length; i++) {
            System.out.println(indices[i]);
        }
    }

    @Test
    public void testDropIndex() throws IOException {
        String indexName = "magazine";
        DeleteIndexRequest request = new DeleteIndexRequest(indexName);
        AcknowledgedResponse response = restHighLevelClient.indices().delete(request, RequestOptions.DEFAULT);
        System.out.println(response.isAcknowledged());
    }



    @Test
    public void testShowAllDoc() throws IOException {
        String indexName = "magazine";
        SearchRequest searchRequest = new SearchRequest(indexName);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.matchAllQuery());
        sourceBuilder.from(0);
        sourceBuilder.size(100);
        searchRequest.source(sourceBuilder);
        SearchResponse response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHit hits[] = response.getHits().getHits();
        for (SearchHit hit : hits) {
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            System.out.println(sourceAsMap);
        }
    }

    @Test
    public void testGetDoc() throws IOException {
        String indexName = "magazine";
        String docId = "1";
        GetRequest request = new GetRequest();
        request.index(indexName).id(docId);
        GetResponse response = restHighLevelClient.get(request, RequestOptions.DEFAULT);
        System.out.println(response);
    }

    @Test
    public void testDelDoc() throws IOException {
        String indexName = "magazine";
        String docId = "ldKiq38Bkr8qLtIopJ2E";
        DeleteRequest request = new DeleteRequest( );
        request.index(indexName).id(docId);
        DeleteResponse deleteResponse = restHighLevelClient.delete(request, RequestOptions.DEFAULT);
        System.out.println(deleteResponse);
        System.out.println(deleteResponse.getId());
    }

    @Test
    public void testGetCount() throws IOException {
        String indexName = "magazine";
        Request request = new Request("GET", indexName+"/_count");
        Response response = restHighLevelClient.getLowLevelClient().performRequest(request);
        System.out.println(EntityUtils.toString(response.getEntity()));
    }

    @Test
    public void fuzzy() throws IOException {
//        https://www.deathearth.com/724.html
        String indexName="magazine";
        String typeName = "_doc";
        BoolQueryBuilder qb = QueryBuilders.boolQuery();
//  match_all
//        qb.must(QueryBuilders.matchAllQuery()); //匹配所有记录68条
        //qb.must(QueryBuilders.commonTermsQuery("title", "馬英九")); //1条
        //qb.must(QueryBuilders.commonTermsQuery("content", "唯九")); //1条
        qb.must(QueryBuilders.queryStringQuery("馬九樂").field("content")); //1条, 分词

        SearchSourceBuilder ssb = new SearchSourceBuilder().query(qb).from(0).size(60);// 影响聚合查询
        try {
            SearchResponse response = getSearchResult(indexName, typeName, ssb);
            System.out.println(response.getHits().getTotalHits());
            SearchHit hits[] = response.getHits().getHits();
            for (SearchHit hit : hits) {
                Map<String, Object> sourceAsMap = hit.getSourceAsMap();
                System.out.println(sourceAsMap);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    @Test
    public void query() throws IOException {
        String indexName="magazine";

//        MatchQueryBuilder matchQueryBuilder = QueryBuilders
//                .matchQuery("name", "afred");

        MultiMatchQueryBuilder multiMatchQueryBuilder =
                QueryBuilders.multiMatchQuery("核稿", "title","author").minimumShouldMatch("100%").slop(0);

//        //查詢條件
//        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery()
//                .must(matchQueryBuilder)
//                .must(multiMatchQueryBuilder);


        //分頁
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
                .query(multiMatchQueryBuilder)
                .from(0)
                .size(100)
                .timeout(new TimeValue(10, TimeUnit.SECONDS));

        //查詢
        SearchRequest searchRequest = new SearchRequest()
                .allowPartialSearchResults(true)
                //在es7中使用_doc作為預設的type,並且es8中將會被移除
                //.types("doc")
                .indices(indexName)
                .source(sourceBuilder);

        SearchResponse response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println("結果總數："+ response.getHits().getHits().length);
        SearchHits hits = response.getHits();  //SearchHits提供有關所有匹配的全域性資訊，例如總命中數或最高分數：
        SearchHit[] searchHits = hits.getHits();
        for (SearchHit hit : searchHits) {
            // do something with the SearchHit
            System.out.println(hit.getId());
            System.out.println(hit.getSourceAsString());
        }
    }


    @Test
    public void fuzzy2() throws IOException {
        String indexName="magazine";

        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices(indexName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
//        String[] includeFields = new String[] {"user"};
//        String[] excludeFields = new String[] {""};
//        searchSourceBuilder.fetchSource(includeFields, excludeFields);
        searchSourceBuilder.query(QueryBuilders.multiMatchQuery("Met"));
//        searchSourceBuilder.query(new MultiMatchQueryBuilder("馬英","title"));
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = this.restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        SearchHit[] searchHits = hits.getHits();
        for (SearchHit hit : searchHits) {
            // do something with the SearchHit

            System.out.println(hit.getSourceAsString());
        }
    }


    SearchResponse getSearchResult(String indexName, String typeName,
                                          SearchSourceBuilder searchSourceBuilder) throws Exception {
        return restHighLevelClient.search(
                new SearchRequest().indices(indexName).types(typeName).source(searchSourceBuilder),
                RequestOptions.DEFAULT);
    }

    SearchResponse getSearchResult(String indexName, String typeName, Integer page,
                                          Integer pageSize, String sortDesc, QueryBuilder queryBuilder,
                                          AggregationBuilder aggregationBuilder) throws Exception {
        return restHighLevelClient.search(
                new SearchRequest().indices(indexName).types(typeName)
                        .source(new SearchSourceBuilder().query(queryBuilder).from(page * pageSize)
                                .size(pageSize).sort(sortDesc, SortOrder.DESC).aggregation(aggregationBuilder)),
                RequestOptions.DEFAULT);
    }


    @Test
    public void wildcardQuery() throws IOException {
        String indexName="magazine";

        // 1.获取查询请求
        SearchRequest goods = new SearchRequest(indexName);
        // 2.创建条件构建器
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        // 3.条件配置
        WildcardQueryBuilder wildcardQueryBuilder = QueryBuilders.wildcardQuery("title", "*藝術博*");//华后多个字符
        searchSourceBuilder.query(wildcardQueryBuilder);
        // 4.配置分页
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(3);
        // 5.向请求中注入条件
        goods.source(searchSourceBuilder);
        // 6.执行查询
        SearchResponse searchResponse = restHighLevelClient.search(goods, RequestOptions.DEFAULT);
        // 7.获取命中对象
        SearchHits hits = searchResponse.getHits();

        // 获取命中总记录数

        System.out.println(">>>>>>>>>>>>>total: " + hits.getHits().length);


    }
}
