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
            logger.info("??????????????????[id=" + id + "].");
            DeleteRequest request = new DeleteRequest();
            request.index(indexName).id(id);
            DeleteResponse response = getClient().delete(request, RequestOptions.DEFAULT);
            logger.info("??????????????????[id=" + id + "].");
            return (response.getResult() == DocWriteResponse.Result.DELETED);
        }catch(Exception e){
            e.printStackTrace();
            throw new RuntimeException("??????????????????[id=" + id + "], cause by " + e.getMessage());
        }
    }


    @Override
    public boolean updateDocs(String indexName, List<Map<String, Object>> records) {
        logger.info("????????????" + records.size() +"?????????.");
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
            logger.info("????????????" + response.getItems().length + "?????????.");
            return !response.hasFailures();
        }catch(Exception e){
            e.printStackTrace();
            throw new RuntimeException("??????????????????[id=" + sb + "], cause by " + e.getMessage());
        }
    }

    @Override
    public boolean addDocs(String indexName, List<Map<String, Object>> records) {
        logger.info("????????????" + records.size() +"?????????.");
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
            logger.info("????????????" + response.getItems().length + "?????????.");
            return !response.hasFailures();
        }catch(Exception e){
            e.printStackTrace();
            throw new RuntimeException("??????????????????[id=" + sb + "], cause by " + e.getMessage());
        }
    }




    @Override
    public boolean addDoc(String indexName, String id, Map<String, Object> record) {
        try{
            logger.info("??????????????????[id=" + id + "].");
            IndexRequest request = new IndexRequest(indexName).source(record).id(id);
            IndexResponse response = getClient().index( request, RequestOptions.DEFAULT);
            logger.info("??????????????????[id=" + id + "].");
            return (response.getResult() == DocWriteResponse.Result.CREATED);
        }catch(Exception e){
            e.printStackTrace();
            throw new RuntimeException("??????????????????[id=" + id + "], cause by " + e.getMessage());
        }
    }

    @Override
    public boolean updateDoc(String indexName, String id, Map<String, Object> updateColumns)  {
        try {
            logger.info("??????????????????[id=" + id + "].");
            UpdateRequest updateRequest = new UpdateRequest();
            updateRequest.index(indexName).id(id);
            //????????????????????????map??????
            updateRequest.doc(updateColumns);
            //??????????????????????????????json????????????????????????????????????V1????????????XContentType.JSON??????
            //updateRequest.doc(JSON.toJSONString(map),XContentType.JSON);
            //??????????????????????????????????????????get????????????????????????????????????????????????????????????????????????????????????
            updateRequest.retryOnConflict(3);
            updateRequest.fetchSource(true);
            UpdateResponse response = getClient().update(updateRequest, RequestOptions.DEFAULT);
            logger.info("??????????????????[id=" + id + "].");
            return (response.getResult() == DocWriteResponse.Result.UPDATED);
        }catch(Exception e){
            e.printStackTrace();
            throw new RuntimeException("??????????????????[id=" + id + "], cause by " + e.getMessage());
        }
    }

    @Override
    public DataSet getDoc(String indexName, String id)  {
        try {
            logger.info("???????????????[" + indexName + "]????????????[" + id + "].");
            GetRequest request = new GetRequest();

            //?????????????????? --- ???????????????user?????????id???1001???????????????
            request.index(indexName).id(id);

            GetResponse response = getClient().get(request, RequestOptions.DEFAULT);
            if( response.getSourceAsMap() != null && response.getSourceAsMap().size() > 0){
                response.getSourceAsMap().put("id", response.getId());
                DataSet ds = new DataSet();
                ds.setHitCounts(1);
                List<Map<String, Object>> list = new ArrayList<>();
                list.add(response.getSourceAsMap());
                ds.setDatas(list);
                logger.info("???????????????[" + indexName + "]????????????[" + id+"].");
                return ds;
            }else{
                logger.info("???????????????[" + indexName + "]????????????[" + id+"].");
                return null;
            }
        }catch(Exception e){
            e.printStackTrace();
            throw new RuntimeException("???????????????[" + indexName + "]????????????[" + id + "], cause by " + e.getMessage());
        }
    }


    @Override
    public DataSet getAllDoc(String indexName, int start, int size)  {
        try {
            logger.info("???????????????[" + indexName + "]??????????????????, ???" + start + "?????????, ???" + size + "???." );
            SearchRequest searchRequest = new SearchRequest(indexName);
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            sourceBuilder.query(QueryBuilders.matchAllQuery());
            sourceBuilder.from(start);
            sourceBuilder.size(size);
            searchRequest.source(sourceBuilder);
            SearchResponse response = getClient().search(searchRequest, RequestOptions.DEFAULT);
            DataSet ds = convertResponse2DataSet(response, null);
            logger.info("???????????????[" + indexName + "]??????????????????, ??????" + start +"?????????, ???" + size + "???, ?????????" + ds.getHitCounts() + "???." );
            return ds;
        }catch(Exception e){
            e.printStackTrace();
            throw new RuntimeException("???????????????[" + indexName + "]??????????????????, ??????" + start + "?????????, ???" + size + "???, cause by " + e.getMessage());
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
                //??????????????????
                Map<String, HighlightField> highlightFields = hit.getHighlightFields();
                for(int i = 0; i < searchColumns.length; i++){
                    HighlightField field= highlightFields.get(searchColumns[i]);
                    if(field!= null){
                        Text[] fragments = field.fragments();
                        String n_field = "";
                        for (Text fragment : fragments) {
                            n_field += fragment;
                        }
                        //???????????????????????????
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

            logger.info("??????????????????[" + keyWords + "]?????????[" + indexName + "]?????????, ???" + start + "?????????, ???" + size + "???.");
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

            ///??????
            HighlightBuilder highlightBuilder = new HighlightBuilder();
            //??????????????????
            for (String colimnInIndex : stringColumnsInIndex) {
                highlightBuilder.field(colimnInIndex);
            }

            //???????????????????????????,????????????false
            highlightBuilder.requireFieldMatch(true);
            highlightBuilder.preTags("<span style='color:red'>");
            highlightBuilder.postTags("</span>");
            //???????????????,?????????????????????????????????????????????????????????,????????????,???????????????????????????,?????????????????????
            highlightBuilder.fragmentSize(800000); //?????????????????????
            highlightBuilder.numOfFragments(0); //????????????????????????????????????


            //??????
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

            //??????
            SearchRequest searchRequest = new SearchRequest()
                    .allowPartialSearchResults(true)
                    .indices(indexName)
                    .source(sourceBuilder);
            SearchResponse response = getClient().search(searchRequest, RequestOptions.DEFAULT);

            logger.info("??????" + response.getHits().getHits().length + "?????????,???[" + sb2 + "]??????.");
            return convertResponse2DataSet(response, stringColumnsInIndex);
        }catch(Exception e){
            e.printStackTrace();
            throw new RuntimeException("??????????????????[" + keyWords +"]?????????[" + indexName + "]?????????, ???" + start + "?????????, ???" + size + "???, cause by " + e.getMessage());
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
            logger.info("??????????????????[" + keyWord + "]?????????[" + indexName + "]?????????, ????????????[" + expandSearchColumns + "], ???" + start + "?????????, ???" + size + "???." );
        }else{
            logger.info("??????????????????[" + keyWord + "]?????????[" + indexName + "]?????????, ???" + start + "?????????, ???" + size + "???." );
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

            ///??????
            HighlightBuilder highlightBuilder = new HighlightBuilder();
            //??????????????????
            for(String colimnInIndex:stringColumnsInIndex){
                highlightBuilder.field(colimnInIndex);
            }

            //???????????????????????????,????????????false
            highlightBuilder.requireFieldMatch(true);
            highlightBuilder.preTags("<span style='color:red'>");
            highlightBuilder.postTags("</span>");
            //???????????????,?????????????????????????????????????????????????????????,????????????,???????????????????????????,?????????????????????
            highlightBuilder.fragmentSize(800000); //?????????????????????
            highlightBuilder.numOfFragments(0); //????????????????????????????????????

            //??????
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

            //??????
            SearchRequest searchRequest = new SearchRequest()
                    .allowPartialSearchResults(true)
                    .indices(indexName)
                    .source(sourceBuilder);
            SearchResponse response = getClient().search(searchRequest, RequestOptions.DEFAULT);

            logger.info("??????" + response.getHits().getHits().length + "?????????,???[" + sb2 + "]??????.");
            return convertResponse2DataSet( response, stringColumnsInIndex );

        }catch(Exception e){
            e.printStackTrace();
            if(expandSearchColumns != null && expandSearchColumns.length() > 0){
                throw new RuntimeException("??????????????????[" + keyWord +"]?????????[" + indexName + "]?????????, ????????????[" + expandSearchColumns + "], ???" + start + "?????????, ???" + size + "???, cause by " + e.getMessage());
            }else{
                throw new RuntimeException("??????????????????[" + keyWord +"]?????????[" + indexName + "]?????????, ???" + start + "?????????, ???" + size + "???, cause by " + e.getMessage());
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
