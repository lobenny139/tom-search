package com.tom.search.service;

import java.util.List;
import java.util.Map;

public interface IIndexService {
    /**
     * 建立索引
     * @param indexName 索引名稱
     * @param columns 欄位名稱及屬性
     * @return
     */
    public boolean createIndex(String indexName, Map<String, String> columns);

    /**
     * 删除索引
     * @param indexName 索引名稱
     * @return
     */
    public boolean dropIndex(String indexName);

    /**
     * 判断索引是否存在
     * @param indexName 索引名稱
     * @return
     */
    public boolean existsIndex(String indexName);

    /**
     * 取得索引欄位資訊
     * @param indexName 索引名稱
     * @return
     */
    public List<Map<String, Object>> getIndexFieldsInfo(String indexName);
}
