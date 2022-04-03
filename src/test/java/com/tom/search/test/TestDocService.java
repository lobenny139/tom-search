package com.tom.search.test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tom.search.model.DataSet;
import com.tom.search.service.IDocService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.*;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = com.tom.search.TestApplication.class)
@TestPropertySource(locations = "/test-application.properties")
public class TestDocService {

    @Autowired(required=true)
    @Qualifier("docService")
    IDocService service;

    @Test
    public void testAddDocs(){
        String indexName="magazine";
        Map<String, Object> columnValue = new HashMap<String, Object>();
        List<Map<String, Object>> records = new ArrayList<>();

        columnValue.put("createDate", new Date().getTime() );
        columnValue.put("id", "3");
        columnValue.put("author", "李白");
        columnValue.put("title", "將進酒");
        columnValue.put("content", "君不見黃河之水天上來 ， 奔流到海不復回!君不見高堂明鏡悲白髮 ， 朝如青絲暮成雪。人生得意須盡歡 ， 莫使金樽空對月。天生我材必有用 ， 千金散盡還復來。烹羊宰牛且為樂 ， 會須一飲三百杯。岑夫子 ， 丹丘生 ， 將進酒 ， 杯莫停。與君歌一曲 ， 請君為我傾耳聽。鐘鼓饌玉不足貴 ， 但願長醉不願醒。古來聖賢皆寂寞 ， 唯有飲者留其名。陳王昔時宴平樂 ， 斗酒十千恣歡謔。主人何為言少錢 ， 徑須沽取對君酌。五花馬 ， 千金裘 ，呼兒將出換美酒 ， 與爾同銷萬古愁。");
        records.add(columnValue);

        Map<String, Object> columnValue2 = new HashMap<String, Object>();
        columnValue2.put("id", "4");
        columnValue2.put("createDate", new Date().getTime() );
        columnValue2.put("author", "李白");
        columnValue2.put("title", "靜夜思");
        columnValue2.put("content", "床前明月光，疑是地上霜。举头望明月，低头思故乡。");
        records.add(columnValue2);

        System.out.println(service.updateOrInsertDocs(indexName, records));

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
    public void testUpdateDoc() throws JsonProcessingException {
        String indexName="magazine";
        String id="2";
        Map<String, Object> columnValues = new HashMap<>();
        columnValues.put("createDate", new Date().getTime() );
        columnValues.put("author", "劉淑琴/張佑之核稿" );
        System.out.println(service.updateDoc(indexName, id, columnValues));
    }

    @Test
    public void testGetDoc() throws JsonProcessingException {
        String indexName="magazine";
        String id="4";
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
        String keyword = "李白";
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

}
