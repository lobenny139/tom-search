package com.tom.search.service;

import com.tom.search.model.DataSet;

import java.util.List;
import java.util.Map;

public interface IDocService {

    /**
     * 刪除文件
     * @param indexName
     * @param id
     * @return
     */
    public boolean delDoc(String indexName, String id);

    /**
     * 批量寫入文件(如果存在，更新，否則寫入)
     * @param indexName 索引名稱
     * @param records 文件(完全的), 裡面必須有鍵
     * @return
     */
    public boolean updateOrInsertDocs(String indexName, List<Map<String, Object>> records);

    /**
     * 在索引中索引加入文件
     * @param indexName 索引名稱
     * @param id 文件id
     * @param record 文檔欄位名稱及值, eg:{"content":"Wellcome to ES"},{"title","Hello World"}
     * @return
     */
    public boolean addDoc(String indexName,
                          String id,
                          Map<String,
                          Object> record);


    /**
     * 更新文件
     * @param indexName
     * @param id
     * @param updateColumns 需要更新欄位
     * @return
     */
    public boolean updateDoc(String indexName, String id,
                             Map<String, Object> updateColumns) ;

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
     * 以關健字查詢文件
     * @param indexName 索引名稱
     * @param keyWord 關健字
     * @param sortedColumn 排序欄位
     * @param timeOutSeconds ES 服務器timeout時間
     * @param start 從那筆開始
     * @param size  共取幾筆
     * @param minimumShouldMatch 關健字切詞後命中率, 台大停電 -> 台大 大停 停電, 切了3個, 100=>全部命中, 70=3個*0.7=2.1,取整數=2 => 命中2個
     * @param slop 關健字切詞後跨度 0=> 全部一起, 5=> 移動5個(台大xxxx大停xxxxx停電)
     * @param searchColumns 在那些欄位查詢
     * @return
     */
    public DataSet searchDoc(   String indexName,
                                String keyWord,
                                Map<String, Integer> sortedColumn,
                                int timeOutSeconds,
                                int start,
                                int size,
                                int minimumShouldMatch,
                                int slop,
                                String... searchColumns  );

    /**
     * 以關健字查詢文件
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
