package com.tom.search.service;

import com.tom.search.model.DataSet;

import java.util.Map;

public interface IDocService {

    /**
     * 在索引中索引加入文件
     * @param indexName 索引名稱
     * @param id 文件id
     * @param columnValue 欄位名稱及值
     * @return
     */
    public boolean addDoc(String indexName, String id, Map<String, Object> columnValue);

    /**
     * 以id取得文件
     * @param indexName 索引名稱
     * @param id 文件id
     * @return
     */
    public DataSet getDoc(String indexName, String id);

    /**
     * 取得所有文件
     * @param indexName 索引名稱
     * @param start 從那筆開始
     * @param size 共取幾筆
     * @return
     */
    public DataSet getAllDoc(String indexName,
                             int start,
                             int size);

    /**
     * 以關健字查詢
     * @param indexName 索引名稱
     * @param keyWord 關健字
     * @param sortColumnInfo 排序欄位
     * @param timeOutSeconds ES 服務器timeout時間
     * @param start 從那筆開始
     * @param size  共取幾筆
     * @param minimumShouldMatch 關健字切詞後命中率
     * @param slop 關健字切詞後跨度
     * @param searchColumns 在那些欄位查詢
     * @return
     */
    public DataSet searchDoc(   String indexName,
                                String keyWord,
                                Map<String, Integer> sortColumnInfo,
                                int timeOutSeconds,
                                int start,
                                int size,
                                int minimumShouldMatch,
                                int slop,
                                String... searchColumns  );

    /**
     * 以關健字查詢
     * @param indexName
     * @param keyWord
     * @param sortColumnInfo
     * @param start
     * @param size
     * @param searchColumns
     * @return
     */
    public DataSet searchDoc(   String indexName,
                                String keyWord,
                                Map<String, Integer> sortColumnInfo,
                                int start,
                                int size,
                                String... searchColumns  );


}
