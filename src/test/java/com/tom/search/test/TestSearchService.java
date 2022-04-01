package com.tom.search.test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tom.search.model.DataSet;
import com.tom.search.model.DataSet;
import com.tom.search.service.ISearchService;
import org.assertj.core.util.diff.Delta;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = com.tom.search.TestApplication.class)
@TestPropertySource(locations = "/test-application.properties")
public class TestSearchService {

    @Autowired(required=true)
    @Qualifier("searchService")
    ISearchService service;

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
    public void testAddDoc(){
        String indexName="magazine";
        String id="1";
        Map<String, Object> columnValues = new HashMap<String, Object>();
        StringBuilder sb = new StringBuilder();
        sb.append("興達發電廠3日因人為疏失造成全台大停電，行政院發言人羅秉成今天透過媒體群組指出，王美花已在下午4時赴行政院提呈「303停電事故檢討報告」，親向蘇貞昌就事故發生原因研判，以及未來改善對策進行調查結果報告。");
        sb.append("羅秉成表示，王美花再次向蘇貞昌表達願負政治責任，但蘇貞昌認為，王美花近年來戮力規畫執行包括能源轉型、三接工程、抗旱整備及應變、班班有冷氣、台商回台投資，以及國際經貿談判布局等重大政策，方方面面都盡心盡力，非常辛苦，且著有成績。");
        sb.append("蘇貞昌說，經濟部轄管國營事業包含中油、台電、台糖、台水等眾多公司，而這次事件經調查顯示，屬台電第一線操作人員技術操作錯誤及監督不周所致，經濟部長之於台電雖有督導責任，不過現階段更重要的任務是儘速盤整及加強全國電網韌性的改革工作。");
        sb.append("蘇貞昌表示，責成王美花務必在6個月內，督導台電提出強化電網韌性建設的完整計畫，包括推動分散式電網建設，使電網架構朝全國融通及區域韌性雙軌並進等精進作為，不再重蹈覆轍，這才是對民眾應有的負責態度。");
        sb.append("曾文生是蔡英文政府能源轉型政策的重要推手之一，從再生能源建置到第三天然氣接收站攻防，屢屢能看到曾文生的影子。");
        sb.append("曾文生畢業自台大土木學系，是民進黨重點栽培人才，曾任行政院青年輔導委員會專門委員、民進黨青年部主任，民國97年起進入高雄市政府，擔任都發局專員、高市府顧問、經發局專員、經發局長，期間經歷過高雄氣爆、大林蒲遷村、五輕更新等計畫，對能源議題相當熟悉。曾文生在107年4月隨著監察院長陳菊北上（當時陳菊升任為總統府秘書長），擔任經濟部政務次長，負責督導能源、國營事業以及水利署相關業務。");

        columnValues.put("createDate", new Date().getTime() );
        columnValues.put("author", "于維寧張佑之");
        columnValues.put("title", "303大停電 政院：曾文生代理台電董座 王美花留任督導強");
        columnValues.put("content", sb.toString());
        System.out.println(service.addDoc(indexName, id, columnValues));

        id = "2";
        sb = new StringBuilder();
        sb.append("（中央社莫斯科30日綜合外電報導）俄羅斯和烏克蘭談判代表在土耳其伊斯坦堡談判有所進展，升高外界對和談突破的希望，但俄國當局今天表示，俄方樂見烏克蘭以書面詳細列出終結衝突的條件，只是和談迄今並無突破跡象。");
        sb.append("法新社報導，克里姆林宮發言人培斯科夫（Dmitry Peskov）今天告訴媒體：「（和談）說不上很有希望或有任何突破。」");
        sb.append("「有待努力之處仍多。」");
        sb.append("培斯科夫表示，俄方對於烏克蘭以書面臚列要求，予以正面看待。");
        sb.append("他並說，談判中商討的問題「我們小心翼翼避免對外說明，因為我們認為，談判應該悄然進行」。");
        sb.append("在俄國主要談判代表形容為「有意義的」伊斯坦堡談判結束後，俄方表示，將大幅減少在烏克蘭首都基輔（Kyiv）和切爾尼戈夫（Chernigiv）周邊地區的軍事活動。");
        columnValues.put("createDate", new Date().getTime()  );
        columnValues.put("author", "劉淑琴核稿張佑之");
        columnValues.put("title", "俄烏談判傳獲進展 克宮稱尚無突破跡象");
        columnValues.put("content", sb.toString());

        System.out.println(service.addDoc(indexName, id, columnValues));


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
    public void testGetIndexFieldsInfo() throws IOException {
        String indexName="magazine";
        List<Map<String, Object>> fieldsInfo = service.getIndexFieldsInfo(indexName);
        System.out.println(new ObjectMapper().writeValueAsString(fieldsInfo));
    }

    @Test
    public void testSearch() throws JsonProcessingException {
        String indexName="magazine";
        String keyword = " 談判";
        String searchColumn1 = "author";
        String searchColumn2 = "title";
        String searchColumn3 = "content";
        String sortColumn = "createDate";
        int start = 0;
        int size = 100;
        int timeOutSeconds = 10;
        int minimumShouldMatch = 100;
        int slop = 0;
        DataSet rs = service.search(indexName, sortColumn, timeOutSeconds, start,  size, keyword, minimumShouldMatch, slop, searchColumn1,searchColumn2, searchColumn3);
        System.out.println(new ObjectMapper().writeValueAsString(rs));
    }

}