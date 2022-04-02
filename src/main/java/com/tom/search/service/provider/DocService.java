package com.tom.search.service.provider;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tom.search.model.DataSet;
import com.tom.search.service.IDocService;
import lombok.Getter;
import lombok.Setter;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.UpdateByQueryAction;
import org.elasticsearch.index.reindex.UpdateByQueryRequestBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.elasticsearch.core.query.UpdateQuery;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

@Getter
@Setter
@Component
@Service
public class DocService implements IDocService {

    private static Logger logger = LoggerFactory.getLogger(DocService.class);

    @Autowired(required = true)
    @Qualifier(value = "restHighLevelClient")
    protected RestHighLevelClient client;

    @Override
    public boolean addDoc(String indexName, String id, Map<String, Object> columnValue) {
        try{
            logger.info("準備寫入數據[id=" + id + "].");
            IndexRequest request = new IndexRequest(indexName).source(columnValue).id(id);
            IndexResponse response = getClient().index( request, RequestOptions.DEFAULT);
            logger.info("成功寫入數據[id=" + id + "].");
            return (response.getResult() == DocWriteResponse.Result.CREATED);
        }catch(Exception e){
            e.printStackTrace();
            throw new RuntimeException("無法寫入數據[id=" + id + "], cause by " + e.getMessage());
        }
    }

    @Override
    public boolean updateDoc(String indexName, String id, Map<String, Object> columnValue)  {
        //https://www.4k8k.xyz/article/u013034223/108607976
        try {
            logger.info("準備更新數據[id=" + id + "].");
            UpdateRequest updateRequest = new UpdateRequest();
            updateRequest.index(indexName).id(id);
            //指定更新的字段，map格式
            updateRequest.doc(columnValue);
            //或者指定更新的字段，json格式传递，同局部更新代码V1版，加上XContentType.JSON即可
            //updateRequest.doc(JSON.toJSONString(map),XContentType.JSON);
            //如果要更新的文档在更新操作的get和索引阶段之间被另一个操作更改，那么要重试多少次更新操作
            updateRequest.retryOnConflict(3);
            updateRequest.fetchSource(true);
            UpdateResponse response = getClient().update(updateRequest, RequestOptions.DEFAULT);
            logger.info("成功更新數據[id=" + id + "].");
            return (response.getResult() == DocWriteResponse.Result.UPDATED);
        }catch(Exception e){
            e.printStackTrace();
            throw new RuntimeException("無法更新數據[id=" + id + "], cause by " + e.getMessage());
        }
    }

    @Override
    public DataSet getDoc(String indexName, String id)  {
        try {
            logger.info("準備在索引[" + indexName + "]取得文件[" + id + "].");
            GetRequest request = new GetRequest();

            //设置请求参数 --- 表示要查询user索引中id为1001的文档内容
            request.index(indexName).id(id);

            GetResponse response = getClient().get(request, RequestOptions.DEFAULT);
            if( response.getSourceAsMap() != null && response.getSourceAsMap().size() > 0){
                response.getSourceAsMap().put("id", response.getId());
                DataSet ds = new DataSet();
                ds.setHitCounts(1);
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
            throw new RuntimeException("無法在索引[" + indexName + "]取得文件[" + id + "], cause by " + e.getMessage());
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
            DataSet ds = convertResponse2DataSet(response, null);
            logger.info("成功在索引[" + indexName + "]取得所有文件, 從第" + start +"筆開始, 取" + size + "筆, 共獲得" + ds.getHitCounts() + "筆." );
            return ds;
        }catch(Exception e){
            e.printStackTrace();
            throw new RuntimeException("無法在索引[" + indexName + "]取得所有文件, 從第" + start + "筆開始, 取" + size + "筆, cause by " + e.getMessage());
        }
    }

    protected DataSet convertResponse2DataSet(SearchResponse response, String[] searchColumns){
        DataSet resultSet = new DataSet();
        resultSet.setHitCounts( response.getHits().getHits().length );

        if(searchColumns == null) {
            SearchHit hits[] = response.getHits().getHits();
            List<Map<String, Object>> results = new LinkedList<Map<String, Object>>();
            for (SearchHit hit : hits) {
                hit.getSourceAsMap();
                hit.getSourceAsMap().put("id", hit.getId());
                results.add(hit.getSourceAsMap());
            }
            resultSet.setDatas(results);
        }else{
            List<Map<String, Object>> list = new ArrayList<>();
            for (SearchHit hit : response.getHits().getHits()) {
                Map<String, Object> sourceAsMap = hit.getSourceAsMap();
                sourceAsMap.put("id", hit.getId());
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
            resultSet.setDatas(list);
        }
        return resultSet;

    }




    @Override
    public DataSet searchDoc(   String indexName,
                                String keyWord,
                                Map<String, Integer> sortColumnInfo,
                                int start,
                                int size,
                                String... searchColumns  ) {
        return searchDoc(indexName, keyWord, sortColumnInfo,  10, start, size, 100, 0,  searchColumns  );
    }

    @Override
    public DataSet searchDoc(   String indexName,
                                String keyWord,
                                Map<String, Integer> sortColumnInfo,
                                int timeOutSeconds,
                                int start,
                                int size,
                                int minimumShouldMatch,
                                int slop,
                                String... searchColumns  )  {
        StringBuilder sb = new StringBuilder();
        for(String searchColumn : searchColumns){
            sb.append(searchColumn+ ",");
        }
        sb = new StringBuilder(sb.substring(0, sb.length()-1) );

        logger.info("準備以關健字[" + keyWord +"]在索引[" + indexName + "]中查詢, 查詢欄位[" + sb.toString() + "], 從" + start + "筆開始, 取" + size + "筆." );
        try {
            MultiMatchQueryBuilder multiMatchQueryBuilder = QueryBuilders.multiMatchQuery(keyWord, searchColumns);
            multiMatchQueryBuilder.minimumShouldMatch( minimumShouldMatch + "%" );
            multiMatchQueryBuilder.slop(slop);

            ///高亮
            HighlightBuilder highlightBuilder = new HighlightBuilder();
            //设置高亮字段
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
                    .timeout(new TimeValue(timeOutSeconds, TimeUnit.SECONDS))
                    .highlighter(highlightBuilder);
//                    .size(size).sort(sortColumn, SortOrder.DESC)

            StringBuilder sb2 = new StringBuilder();
            for(Map.Entry entry:sortColumnInfo.entrySet()){
                sb2.append(entry.getKey().toString() + ":" + ( entry.getValue().toString().equals("0")  ? "ASC" : "DESC") + ",");
                sourceBuilder.sort(entry.getKey().toString(), (entry.getValue().toString().equals("0")  ? SortOrder.ASC : SortOrder.DESC) );
            }
            sb2 = new StringBuilder(sb2.substring(0, sb2.length()-1) );


            //查詢
            SearchRequest searchRequest = new SearchRequest()
                    .allowPartialSearchResults(true)
                    .indices(indexName)
                    .source(sourceBuilder);
            SearchResponse response = getClient().search(searchRequest, RequestOptions.DEFAULT);

            logger.info("找到" + response.getHits().getHits().length + "筆記錄,以[" + sb2 + "]排序.");
            //return convertResponse2DataSet( response );

            List<Map<String, Object>> list = new ArrayList<>();
            for (SearchHit hit : response.getHits().getHits()) {
                Map<String, Object> sourceAsMap = hit.getSourceAsMap();
                sourceAsMap.put("id", hit.getId());
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
            ds.setHitCounts(response.getHits().getHits().length);
            return ds;
        }catch(Exception e){
            e.printStackTrace();
            throw new RuntimeException("無法以關健字[" + keyWord +"]在索引[" + indexName + "]中查詢, 查詢欄位[" + sb.toString() + "], 從" + start + "筆開始, 取" + size + "筆, cause by " + e.getMessage());
        }
    }


}
