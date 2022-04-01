package com.tom.search.service.provider;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tom.search.model.DataSet;
import com.tom.search.service.ISearchService;
import lombok.Getter;
import lombok.Setter;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetMappingsRequest;
import org.elasticsearch.client.indices.GetMappingsResponse;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.TimeUnit;

@Getter
@Setter
@Component
@Service
public class SearchService implements ISearchService {

    private static Logger logger = LoggerFactory.getLogger(SearchService.class);

    @Autowired(required = true)
    @Qualifier(value = "restHighLevelClient")
    protected RestHighLevelClient client;


    @Override
    public List<Map<String, Object>> getIndexFieldsInfo(String indexName) {
        logger.info("準備取得索引[" + indexName +"]欄位資訊.");
        try {
            //指定索引
            GetMappingsRequest getMappings = new GetMappingsRequest().indices(indexName);
            //调用获取
            GetMappingsResponse getMappingResponse = getClient().indices().getMapping(getMappings, RequestOptions.DEFAULT);
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
            logger.info("成功取得索引[" + indexName +"]欄位資訊.");
            logger.info("欄位資訊:" + new ObjectMapper().writeValueAsString(mapList));
            return mapList;
        }catch(Exception e){
            e.printStackTrace();
            throw new RuntimeException("無法取得索引[" + indexName + "]欄位資訊, cause by " + findExceptionRoorCause(e));
        }
    }

    @Override
    public boolean createIndex(String indexName, Map<String, String> columnsInfo) {
        if(existsIndex(indexName)){
            throw  new RuntimeException("索引[" + indexName+"]已經存在.");
        }
        logger.info("準備建立索引[" + indexName +"].");
        CreateIndexResponse response = null;
        try {
            CreateIndexRequest request = new CreateIndexRequest(indexName);
            request.settings(Settings.builder()
                    .put("analysis.analyzer.default.tokenizer", "standard") // 默认分词器
                    .put("analysis.analyzer.default.filter", "cjk_bigram") // 默认分词器
            );
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            {
                builder.startObject("properties");
                {
                    for (Object key : columnsInfo.keySet()) {
                        logger.info("索引欄位資訊["+ key + ":" + columnsInfo.get(key) + "].");
                        builder.startObject(key.toString());
                        {
                            builder.field("type", columnsInfo.get(key));
                        }
                        builder.endObject();
                    }

                }
                builder.endObject();
            }
            builder.endObject();

            request.mapping(builder.toString());
            response = getClient().indices().create(request, RequestOptions.DEFAULT);
            logger.info("成功建立索引[" + indexName + "].");
            return (response.isAcknowledged() );
        }catch(Exception e){
            e.printStackTrace();
            throw new RuntimeException("無法建立索引[" + indexName + "], cause by " + findExceptionRoorCause(e));
        }
    }

    @Override
    public boolean dropIndex(String indexName) {
        logger.info("準備刪除索引[" + indexName + "].");
        if(!existsIndex(indexName)){
            throw new RuntimeException("索引[" + indexName+"]不存在.");
        }
        AcknowledgedResponse response = null;
        try {
            DeleteIndexRequest request = new DeleteIndexRequest(indexName);
            response = getClient().indices().delete(request, RequestOptions.DEFAULT);
            logger.info("成功刪除索引[" + indexName + "].");
            return response.isAcknowledged();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("無法刪除索引[" + indexName + "], cause by " + findExceptionRoorCause(e));
        }
    }

    @Override
    public boolean existsIndex(String indexName) {
        try {
            GetIndexRequest request = new GetIndexRequest();
            request.indices(indexName);
            return (getClient().indices().exists(request, RequestOptions.DEFAULT));
        }catch (Exception e) {
            throw new RuntimeException( findExceptionRoorCause(e) );
        }
    }


    @Override
    public boolean addDoc(String indexName, String id, Map<String, Object> columnValue) {
        try{
            logger.info("準備寫入數據[id=" + id + "].");
            XContentBuilder xcontent = XContentFactory.jsonBuilder();
            xcontent.startObject();
            for (Object key : columnValue.keySet()) {
                xcontent.field(key.toString(), columnValue.get(key));
            }
            xcontent.endObject();
            IndexRequest request = new IndexRequest(indexName).source(xcontent).id(id);
            IndexResponse response = getClient().index( request, RequestOptions.DEFAULT);
            logger.info("成功寫入數據[id=" + id + "].");
            return (response.getResult() == DocWriteResponse.Result.CREATED);
        }catch(Exception e){
            e.printStackTrace();
            throw new RuntimeException("無法寫入數據[id=" + id + "], cause by " + findExceptionRoorCause(e));
        }
    }

    @Override
    public DataSet getDoc(String indexName, String id)  {
        try {
            logger.info("準備在索引[" + indexName + "]取得文件[" + id + "].");
            GetRequest request = new GetRequest(indexName, id);
            GetResponse response = getClient().get(request, RequestOptions.DEFAULT);
            if( response.getSourceAsMap() != null && response.getSourceAsMap().size() > 0){
                response.getSourceAsMap().put("id", response.getId());
                DataSet ds = new DataSet();
                ds.setHitsCount(1);
                List<Map<String, Object>> list = new ArrayList<>();
                list.add(response.getSourceAsMap());
                ds.setDatas(list);
                logger.info("成功在索引[" + indexName + "]取得文件[" + id+"].");
                return ds;
            }else{
                logger.info("無法在索引[" + indexName + "]取得文件[" + id+"].");
                return null;
            }
        }catch(Exception e){
            e.printStackTrace();
            throw new RuntimeException("無法在索引[" + indexName + "]取得文件[" + id + "], cause by " + findExceptionRoorCause(e));
        }
    }


    @Override
    public DataSet getAllDoc(String indexName, int start, int size)  {
        try {
            logger.info("準備在索引[" + indexName + "]取得所有文件, 從" + start + "筆開始, 取" + size + "筆." );
            SearchRequest searchRequest = new SearchRequest(indexName);
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            sourceBuilder.query(QueryBuilders.matchAllQuery());
            sourceBuilder.from(start);
            sourceBuilder.size(size);
            searchRequest.source(sourceBuilder);
            SearchResponse response = getClient().search(searchRequest, RequestOptions.DEFAULT);
            DataSet ds = convertResponse2DataSet(response);
            logger.info("成功在索引[" + indexName + "]取得所有文件, 從第" + start +"筆開始, 取" + size + "筆, 共獲得" + ds.getHitsCount() + "筆." );
            return ds;
        }catch(Exception e){
            e.printStackTrace();
            throw new RuntimeException("無法在索引[" + indexName + "]取得所有文件, 從第" + start + "筆開始, 取" + size + "筆, cause by " + findExceptionRoorCause(e));
        }
    }

    protected DataSet convertResponse2DataSet(SearchResponse response){
        DataSet resultSet = new DataSet();
        resultSet.setHitsCount( response.getHits().getHits().length );

        SearchHit hits[] = response.getHits().getHits();
        List<Map<String, Object>> results = new LinkedList<Map<String, Object>>();
        for (SearchHit hit : hits) {
            hit.getSourceAsMap();
            hit.getSourceAsMap().put("id", hit.getId());
            results.add( hit.getSourceAsMap() );
        }
        resultSet.setDatas(results);
        return resultSet;
    }

    @Override
    public DataSet search(String indexName, String sortColumn, int timeOutSeconds, int start, int size, String keyWord, int minimumShouldMatch, int slop, String... searchColumns  )  {
        try {
            MultiMatchQueryBuilder multiMatchQueryBuilder =
                    QueryBuilders.multiMatchQuery(keyWord, searchColumns).minimumShouldMatch( minimumShouldMatch+"%" ).slop(slop);

            ///高亮
            HighlightBuilder highlightBuilder = new HighlightBuilder();
            //设置高亮字段
//            highlightBuilder.field("filed");
            for(int i = 0; i < searchColumns.length; i++){
                highlightBuilder.field(searchColumns[i]);
            }
            //如果要多个字段高亮,这项要为false
            highlightBuilder.requireFieldMatch(true);
            highlightBuilder.preTags("<span style='color:red'>");
            highlightBuilder.postTags("</span>");

            //下面这两项,如果你要高亮如文字内容等有很多字的字段,必须配置,不然会导致高亮不全,文章内容缺失等
            highlightBuilder.fragmentSize(800000); //最大高亮分片数
            highlightBuilder.numOfFragments(0); //从第一个分片获取高亮片段


            //分頁
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
                    .query(multiMatchQueryBuilder)
                    .from(start)
                    .size(size).sort(sortColumn, SortOrder.DESC)
                    .timeout(new TimeValue(timeOutSeconds, TimeUnit.SECONDS))
                    .highlighter(highlightBuilder);

            //查詢
            SearchRequest searchRequest = new SearchRequest()
                    .allowPartialSearchResults(true)
                    .indices(indexName)
                    .source(sourceBuilder);
            SearchResponse response = getClient().search(searchRequest, RequestOptions.DEFAULT);

            //return convertResponse2DataSet( response );

            List<Map<String, Object>> list = new ArrayList<>();
            for (SearchHit hit : response.getHits().getHits()) {
                Map<String, Object> sourceAsMap = hit.getSourceAsMap();
                //解析高亮字段
                Map<String, HighlightField> highlightFields = hit.getHighlightFields();
                for(int i = 0; i < searchColumns.length; i++){
                    HighlightField field= highlightFields.get(searchColumns[i]);
                    if(field!= null){
                        Text[] fragments = field.fragments();
                        String n_field = "";
                        for (Text fragment : fragments) {
                            n_field += fragment;
                        }
                        //高亮标题覆盖原标题
                        sourceAsMap.put(searchColumns[i],n_field);
                    }
                }
                list.add(hit.getSourceAsMap());
            }

            DataSet ds = new DataSet();
            ds.setDatas(list);
            ds.setHitsCount(response.getHits().getHits().length);
            return ds;
        }catch(Exception e){
            throw new RuntimeException(findExceptionRoorCause(e));
        }
    }


    /*
     * 取得 exception root cause
     * Ref: https://www.baeldung.com/java-exception-root-cause
     * Find an Exception’s Root Cause
     */
    protected Throwable findExceptionRoorCause(Throwable throwable) {
        Objects.requireNonNull(throwable);
        Throwable rootCause = throwable;
        while (rootCause.getCause() != null && rootCause.getCause() != rootCause) {
            rootCause = rootCause.getCause();
        }
        return rootCause;
    }
}
