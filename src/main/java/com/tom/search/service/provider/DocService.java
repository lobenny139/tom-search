package com.tom.search.service.provider;

import com.tom.search.model.DataSet;
import com.tom.search.service.IDocService;
import com.tom.search.service.IIndexService;
import lombok.Getter;
import lombok.Setter;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
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
import org.elasticsearch.index.query.BoolQueryBuilder;
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

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
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

    @Autowired(required=true)
    @Qualifier("indexService")
    protected IIndexService indexService;

    @Override
    public boolean delDoc(String indexName, String id) {
        try {
            logger.info("準備刪除文件[id=" + id + "].");
            DeleteRequest request = new DeleteRequest();
            request.index(indexName).id(id);
            DeleteResponse response = getClient().delete(request, RequestOptions.DEFAULT);
            logger.info("成功刪除文件[id=" + id + "].");
            return (response.getResult() == DocWriteResponse.Result.DELETED);
        }catch(Exception e){
            e.printStackTrace();
            throw new RuntimeException("無法刪除文件[id=" + id + "], cause by " + e.getMessage());
        }
    }


    @Override
    public boolean updateDocs(String indexName, List<Map<String, Object>> records) {
        logger.info("準備更新" + records.size() +"筆文件.");
        StringBuilder sb = null;
        try {
            BulkRequest request = new BulkRequest();
            sb = new StringBuilder();
            for(Map<String, Object> record : records){
                String id = record.get("id").toString();
                sb.append(id + ",");
                record.remove("id");
                request.add(
                    new UpdateRequest().index(indexName).id(id).doc(record).retryOnConflict(3).fetchSource(true)
                );
            }

            sb = new StringBuilder(sb.substring(0, sb.length() - 1));
            BulkResponse response = getClient().bulk(request, RequestOptions.DEFAULT);
            for (BulkItemResponse r : response.getItems()) {
                if (r.isFailed()) {
                    logger.error("{}\t{}", r.getId(), r.getFailureMessage());
                }
            }
            logger.info("成功更新" + response.getItems().length + "筆文件.");
            return !response.hasFailures();
        }catch(Exception e){
            e.printStackTrace();
            throw new RuntimeException("無法更新文件[id=" + sb + "], cause by " + e.getMessage());
        }
    }

    @Override
    public boolean addDocs(String indexName, List<Map<String, Object>> records) {
        logger.info("準備寫入" + records.size() +"筆文件.");
        StringBuilder sb = null;
        try {
            BulkRequest request = new BulkRequest();
            sb = new StringBuilder();
            for(Map<String, Object> record : records){
                String id = record.get("id").toString();
                sb.append(id + ",");
                record.remove("id");
                request.add(
                    new IndexRequest(indexName).id(id).source(record)
                );
            }

            sb = new StringBuilder(sb.substring(0, sb.length() - 1));
            BulkResponse response = getClient().bulk(request, RequestOptions.DEFAULT);
            for (BulkItemResponse r : response.getItems()) {
                if (r.isFailed()) {
                    logger.error("{}\t{}", r.getId(), r.getFailureMessage());
                }
            }
            logger.info("成功寫入" + response.getItems().length + "筆文件.");
            return !response.hasFailures();
        }catch(Exception e){
            e.printStackTrace();
            throw new RuntimeException("無法寫入文件[id=" + sb + "], cause by " + e.getMessage());
        }
    }




    @Override
    public boolean addDoc(String indexName, String id, Map<String, Object> record) {
        try{
            logger.info("準備寫入文件[id=" + id + "].");
            IndexRequest request = new IndexRequest(indexName).source(record).id(id);
            IndexResponse response = getClient().index( request, RequestOptions.DEFAULT);
            logger.info("成功寫入文件[id=" + id + "].");
            return (response.getResult() == DocWriteResponse.Result.CREATED);
        }catch(Exception e){
            e.printStackTrace();
            throw new RuntimeException("無法寫入文件[id=" + id + "], cause by " + e.getMessage());
        }
    }

    @Override
    public boolean updateDoc(String indexName, String id, Map<String, Object> updateColumns)  {
        try {
            logger.info("準備更新文件[id=" + id + "].");
            UpdateRequest updateRequest = new UpdateRequest();
            updateRequest.index(indexName).id(id);
            //指定更新的字段，map格式
            updateRequest.doc(updateColumns);
            //或者指定更新的字段，json格式传递，同局部更新代码V1版，加上XContentType.JSON即可
            //updateRequest.doc(JSON.toJSONString(map),XContentType.JSON);
            //如果要更新的文档在更新操作的get和索引阶段之间被另一个操作更改，那么要重试多少次更新操作
            updateRequest.retryOnConflict(3);
            updateRequest.fetchSource(true);
            UpdateResponse response = getClient().update(updateRequest, RequestOptions.DEFAULT);
            logger.info("成功更新文件[id=" + id + "].");
            return (response.getResult() == DocWriteResponse.Result.UPDATED);
        }catch(Exception e){
            e.printStackTrace();
            throw new RuntimeException("無法更新文件[id=" + id + "], cause by " + e.getMessage());
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
                                String keyWords,
                                Map<String, Integer> sortedColumn,
                                int timeOutSeconds,
                                int start,
                                int size,
                                int minimumShouldMatch,
                                int slop) {
        try {

            logger.info("準備以關健字[" + keyWords + "]在索引[" + indexName + "]中查詢, 從" + start + "筆開始, 取" + size + "筆.");
            BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();

            for (String keyword : keyWords.split(" ")) {
                if (keyword.startsWith("+")) {
                    String word = keyword.trim().replace("+", "");
                    boolQueryBuilder.must(
                            QueryBuilders.multiMatchQuery( word ).minimumShouldMatch(minimumShouldMatch+"%").slop(slop)
                    );
                } else if (keyword.startsWith("-")) {
                    String word = keyword.trim().replace("-", "");
                    boolQueryBuilder.mustNot(
                            QueryBuilders.multiMatchQuery( word ).minimumShouldMatch(minimumShouldMatch+"%").slop(slop)
                    );
                } else {
                    String word = keyword.trim();
                    boolQueryBuilder.should(
                            QueryBuilders.multiMatchQuery( word ).minimumShouldMatch(minimumShouldMatch+"%").slop(slop)
                    );
                }
            }

            logger.info("ES Search Condition:" + boolQueryBuilder.toString());

            String[] stringColumnsInIndex = this.convertStringColumnInIndex2Array(indexName);

            ///高亮
            HighlightBuilder highlightBuilder = new HighlightBuilder();
            //设置高亮字段
            for (String colimnInIndex : stringColumnsInIndex) {
                highlightBuilder.field(colimnInIndex);
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
                    .query(boolQueryBuilder)
                    .from(start)
                    .timeout(new TimeValue(timeOutSeconds, TimeUnit.SECONDS))
                    .highlighter(highlightBuilder);

            StringBuilder sb2 = new StringBuilder();
            for (Map.Entry entry : sortedColumn.entrySet()) {
                sb2.append(entry.getKey().toString() + ":" + (entry.getValue().toString().equals("0") ? "ASC" : "DESC") + ",");
                sourceBuilder.sort(entry.getKey().toString(), (entry.getValue().toString().equals("0") ? SortOrder.ASC : SortOrder.DESC));
            }
            sb2 = new StringBuilder(sb2.substring(0, sb2.length() - 1));

            //查詢
            SearchRequest searchRequest = new SearchRequest()
                    .allowPartialSearchResults(true)
                    .indices(indexName)
                    .source(sourceBuilder);
            SearchResponse response = getClient().search(searchRequest, RequestOptions.DEFAULT);

            logger.info("找到" + response.getHits().getHits().length + "筆記錄,以[" + sb2 + "]排序.");
            return convertResponse2DataSet(response, stringColumnsInIndex);
        }catch(Exception e){
            e.printStackTrace();
            throw new RuntimeException("無法以關健字[" + keyWords +"]在索引[" + indexName + "]中查詢, 從" + start + "筆開始, 取" + size + "筆, cause by " + e.getMessage());
        }
    }

    @Override
    public DataSet searchDocByColumn(  String indexName,
                                       String keyWord,
                                       Map<String, Integer> sortedColumn,
                                       int timeOutSeconds,
                                       int start,
                                       int size,
                                       int minimumShouldMatch,
                                       int slop,
                                       String... searchColumns )  {
        String expandSearchColumns = null;
        if(searchColumns != null && searchColumns.length > 0){
            expandSearchColumns = array2String(searchColumns);
        }
        if(expandSearchColumns != null){
            logger.info("準備以關健字[" + keyWord + "]在索引[" + indexName + "]中查詢, 查詢欄位[" + expandSearchColumns + "], 從" + start + "筆開始, 取" + size + "筆." );
        }else{
            logger.info("準備以關健字[" + keyWord + "]在索引[" + indexName + "]中查詢, 從" + start + "筆開始, 取" + size + "筆." );
        }
        try {
            MultiMatchQueryBuilder multiMatchQueryBuilder = null;
            if(searchColumns != null && searchColumns.length > 0){
                multiMatchQueryBuilder = QueryBuilders.multiMatchQuery(keyWord, searchColumns);
            }else{
                multiMatchQueryBuilder = QueryBuilders.multiMatchQuery(keyWord);
            }
            multiMatchQueryBuilder.minimumShouldMatch( minimumShouldMatch + "%" );
            multiMatchQueryBuilder.slop(slop);

            logger.info("ES Search Condition:" + multiMatchQueryBuilder.toString());

            String[] stringColumnsInIndex = this.convertStringColumnInIndex2Array(indexName);

            ///高亮
            HighlightBuilder highlightBuilder = new HighlightBuilder();
            //设置高亮字段
            for(String colimnInIndex:stringColumnsInIndex){
                highlightBuilder.field(colimnInIndex);
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

            StringBuilder sb2 = new StringBuilder();
            for(Map.Entry entry:sortedColumn.entrySet()){
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
            return convertResponse2DataSet( response, stringColumnsInIndex );

        }catch(Exception e){
            e.printStackTrace();
            if(expandSearchColumns != null && expandSearchColumns.length() > 0){
                throw new RuntimeException("無法以關健字[" + keyWord +"]在索引[" + indexName + "]中查詢, 查詢欄位[" + expandSearchColumns + "], 從" + start + "筆開始, 取" + size + "筆, cause by " + e.getMessage());
            }else{
                throw new RuntimeException("無法以關健字[" + keyWord +"]在索引[" + indexName + "]中查詢, 從" + start + "筆開始, 取" + size + "筆, cause by " + e.getMessage());
            }
        }
    }

    protected String map2String(Map<String, Object> map){
        StringBuilder sb = new StringBuilder();
        for(Map.Entry entry:map.entrySet()){
            sb.append(entry.getKey().toString() + ":" +  entry.getValue() + ",");
        }
        return (new StringBuilder(sb.substring(0, sb.length()-1) ) ).toString();
    }

    protected String array2String(String[] results){
        StringBuilder sb = new StringBuilder();
        for(String result : results){
            sb.append(result + ",");
        }
        sb = new StringBuilder(sb.substring(0, sb.length()-1) );
        return sb.toString();
    }

    protected String[] convertStringColumnInIndex2Array(String indexName){
        List<Map<String, Object>> fieldsInfo = getIndexService().getIndexFieldsInfo(indexName);
        List<String> stringColumnInIndex = new ArrayList<String>();
        for(Map<String,Object> field : fieldsInfo){
            for(Map.Entry entry:field.entrySet()){
                if(entry.getValue().toString().toLowerCase().equals("text")){
                    stringColumnInIndex.add(entry.getKey().toString());
                }
            }
        }
        return stringColumnInIndex.stream().toArray(String[]::new);
    }

}
