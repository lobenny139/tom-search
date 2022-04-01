package com.tom.search.service;

import com.tom.search.model.DataSet;
import java.util.List;
import java.util.Map;

public interface ISearchService {

    public boolean createIndex(String indexName, Map<String, String> columns);

    public boolean dropIndex(String indexName);

    public boolean existsIndex(String indexName);

    public boolean addDoc(String indexName, String id, Map<String, Object> columnValue);

    public DataSet getDoc(String indexName, String id);

    public DataSet getAllDoc(String indexName, int start, int size);

    public DataSet search(String indexName,
                          String sortColumn,
                          int timeOutSeconds,
                          int start, int size,
                          String keyWord, int minimumShouldMatch, int slop,
                          String... searchColumns  );

    public List<Map<String, Object>> getIndexFieldsInfo(String indexName);
}
