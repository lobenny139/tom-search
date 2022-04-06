package com.tom.search.test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tom.search.model.DataSet;
import com.tom.search.service.IDocService;
import org.apache.commons.io.FileUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.File;
import java.io.IOException;
import java.util.*;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = com.tom.search.TestApplication.class)
@TestPropertySource(locations = "/test-application.properties")
public class TestDocService {

    @Autowired(required=true)
    @Qualifier("docService")
    IDocService service;


    @Test
    public void testDelDoc(){
        String indexName="magazine";
        String id="2";
        System.out.println(service.delDoc(indexName, id));
    }

    @Test
    public void testAddDoc(){
        String indexName="magazine";
        String id="2";
        Map<String, Object> columnValues = new HashMap<String, Object>();
        columnValues.put("createDate", new Date().getTime() );
        columnValues.put("author", "孟浩然");
        columnValues.put("title", "春晓");
        columnValues.put("content", "春眠不觉晓，处处闻啼鸟。夜来风雨声，花落知多少。");
        System.out.println(service.addDoc(indexName, id, columnValues));
    }

    @Test
    public void testUpdateDoc() throws JsonProcessingException {
        String indexName="magazine";
        String id="2";
        Map<String, Object> columnValues = new HashMap<>();
        columnValues.put("content", "春眠不覺曉，處處聞啼鳥。夜來風雨聲，花落知多少。" );
        columnValues.put("title", "春曉" );
        System.out.println(service.updateDoc(indexName, id, columnValues));
    }

    @Test
    public void testGetDoc() throws JsonProcessingException {
        String indexName="magazine";
        String id="2";
        DataSet ds  = service.getDoc(indexName, id);
        System.out.println(new ObjectMapper().writeValueAsString(ds));
    }

    @Test
    public void testGetAllDoc() throws JsonProcessingException {
        String indexName="magazine";
        int start = 0;
        int size = 100;
        DataSet rs = service.getAllDoc(indexName, start,  size);
        System.out.println(new ObjectMapper().writeValueAsString(rs));
    }

    @Test
    public void testSearchDoc() throws JsonProcessingException {
        String indexName="magazine";
        String keyword = "刘景文";
        String searchColumn1 = "author";
        String searchColumn2 = "title";
        String searchColumn3 = "content";
        Map<String, Integer> sortColumn = new HashMap<>();
        sortColumn.put("createDate",0);
        int start = 0;
        int size = 100;
        int timeOutSeconds = 10;
        int minimumShouldMatch = 100;
        int slop = 0;
        DataSet rs = service.searchDoc(indexName, keyword, sortColumn, timeOutSeconds, start,  size, minimumShouldMatch, slop, searchColumn1,searchColumn2, searchColumn3);
        System.out.println(new ObjectMapper().writeValueAsString(rs));
    }

    @Test
    public void testSearchDoc2() throws JsonProcessingException {
        String indexName="magazine";
        String keyword = "杜甫";
        Map<String, Integer> sortColumn = new HashMap<>();
        sortColumn.put("createDate",0);
        int start = 0;
        int size = 100;
        DataSet rs = service.searchDoc(indexName, keyword, sortColumn, start,  size);
        System.out.println(new ObjectMapper().writeValueAsString(rs));
    }

    @Test
    public void testAddDocs() throws JsonProcessingException {
        String indexName="magazine";
        System.out.println(service.addDocs(indexName, readFileAsList("唐詩")));
    }

    @Test
    public void testUpdateDocs() throws JsonProcessingException {
        String indexName="magazine";
        System.out.println(service.updateDocs(indexName, readFileAsList("唐詩2")));
    }


    List<Map<String, Object>> readFileAsList(String fileName) throws JsonProcessingException {
    //public void readFileAsList() throws JsonProcessingException {
//        String fileName = "唐詩2";
        File file = new File("E:/work@tom/tom-search/src/test/resources/" + fileName);
        List<Map<String, Object>> records = null;
        try {
            List<String> contents = FileUtils.readLines(file, "UTF-8");
            records = new ArrayList<>();
            Map<String, Object> record = new HashMap<>();
            // Iterate the result to print each line of the file.
            for (int i = 0; i < contents.size(); i++) {
                System.out.println(contents.get(i));
                String[] fields = contents.get(i).split(":");
                record = new HashMap<>();
                record.put("id", fields[0]);
                record.put("author", fields[1]);
                record.put("title", fields[2]);
                record.put("content", fields[3]);
                record.put("createDate", new Date().getTime());
                records.add(record);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        //System.out.println(records);
        return (records);
    }
}
