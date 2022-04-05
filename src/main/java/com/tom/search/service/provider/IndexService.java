package com.tom.search.service.provider;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tom.search.service.IIndexService;
import lombok.Getter;
import lombok.Setter;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetMappingsRequest;
import org.elasticsearch.client.indices.GetMappingsResponse;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import java.util.*;

@Getter
@Setter
@Component
@Service
public class IndexService implements IIndexService {

    private static Logger logger = LoggerFactory.getLogger(IndexService.class);

    @Autowired(required = true)
    @Qualifier(value = "restHighLevelClient")
    protected RestHighLevelClient client;

    @Override
    public List<Map<String, Object>> getIndexFieldsInfo(String indexName) {
        logger.info("準備取得索引[" + indexName + "]欄位資訊.");
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
            throw new RuntimeException("無法取得索引[" + indexName + "]欄位資訊, cause by " + e.getMessage());
        }
    }

    @Override
    public boolean createIndex(String indexName, Map<String, String> columnsInfo) {
        /*
        https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-cjk-bigram-tokenfilter.html
        curl -X GET "localhost:9200/_analyze?pretty" -H 'Content-Type: application/json' -d'
        {
            "tokenizer" : "standard",
                "filter" : ["cjk_bigram"],
            "text" : "東京都は、日本の首都であり"
        }'
        */

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
            throw new RuntimeException("無法建立索引[" + indexName + "], cause by " + e.getMessage());
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
            throw new RuntimeException("無法刪除索引[" + indexName + "], cause by " + e.getMessage());
        }
    }

    @Override
    public boolean existsIndex(String indexName) {
        try {
            GetIndexRequest request = new GetIndexRequest();
            request.indices(indexName);
            return (getClient().indices().exists(request, RequestOptions.DEFAULT));
        }catch (Exception e) {
            throw new RuntimeException( e.getMessage() );
        }
    }

}
