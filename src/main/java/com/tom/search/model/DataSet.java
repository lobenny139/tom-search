package com.tom.search.model;

import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Document.*;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

@Data
public class DataSet {

    private int hitsCount;

    private List<Map<String, Object>> datas = new ArrayList<Map<String, Object>>();


}
