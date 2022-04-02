package com.tom.search.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tom.search.service.IIndexService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = com.tom.search.TestApplication.class)
@TestPropertySource(locations = "/test-application.properties")
public class TestIndexService {

    @Autowired(required=true)
    @Qualifier("indexService")
    IIndexService service;

    @Test
    public  void testDropIndex()  {
        String indexName="magazine";
        System.out.println(service.dropIndex(indexName));
    }

    @Test
    public void testCreateIndex() {
        Map<String, String> columnInfos = new HashMap<String, String>();
        columnInfos.put("createDate", "long");
        columnInfos.put("author", "text");
        columnInfos.put("title", "text");
        columnInfos.put("content", "text");
        String indexName="magazine";
        System.out.println(service.createIndex(indexName, columnInfos));
    }


    @Test
    public void testGetIndexFieldsInfo() throws IOException {
        String indexName="magazine1";
        List<Map<String, Object>> fieldsInfo = service.getIndexFieldsInfo(indexName);
        System.out.println(new ObjectMapper().writeValueAsString(fieldsInfo));
    }



}
