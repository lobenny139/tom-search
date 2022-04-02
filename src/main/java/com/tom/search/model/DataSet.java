package com.tom.search.model;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Data
public class DataSet {

    private int hitCounts;

    private List<Map<String, Object>> datas = new ArrayList<Map<String, Object>>();


}
