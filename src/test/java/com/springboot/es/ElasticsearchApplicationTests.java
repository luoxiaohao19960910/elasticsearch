package com.springboot.es;

import com.alibaba.fastjson.JSON;
import com.springboot.es.pojo.User;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MatchPhraseQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 基于elasticsearch 7.6.1版本的测试
 */
@SpringBootTest
public class ElasticsearchApplicationTests {

    @Autowired
    private RestHighLevelClient client;
/**************************************************索引操作*************************************************************/
    //增加索引
    @Test
    public void insertIndex() throws IOException {
        //创建添加索引请求
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("luo_index");
        //客户端发出创建请求后获取到响应
        CreateIndexResponse createIndexResponse = client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        System.out.println(createIndexResponse);
    }

    //判断索引是否存在
    @Test
    public void isExistIndex() throws IOException {
        GetIndexRequest getIndexRequest = new GetIndexRequest("luo_index");
        boolean exists = client.indices().exists(getIndexRequest, RequestOptions.DEFAULT);
        System.out.println(exists);
    }

    //删除索引
    @Test
    public void deleteIndex() throws IOException {
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest("luo_index");
        //客户端发出删除请求后获取到响应
        AcknowledgedResponse delete = client.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
        System.out.println(delete.isAcknowledged());
    }
/**************************************************文档操作*************************************************************/
    //增加文档
    @Test
    public void insertDocument() throws IOException {
        //创建添加文档请求
        IndexRequest indexRequest = new IndexRequest("luo_index");
        User user = new User("张三", 3);
        indexRequest.id("1");
        //将添加的数据放入请求中
        indexRequest.source(JSON.toJSONString(user), XContentType.JSON);
        IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
        System.out.println(indexResponse.status());
        System.out.println(indexResponse.toString());
    }

    //判断文档是否存在
    @Test
    public void isExistDocument() throws IOException {
        GetRequest getRequest = new GetRequest("luo_index","1");
        boolean exists = client.exists(getRequest, RequestOptions.DEFAULT);
        System.out.println(exists);
    }

    //删除文档
    @Test
    public void deleteDocument() throws IOException {
        DeleteRequest deleteRequest = new DeleteRequest("luo_index", "1");
        DeleteResponse deleteResponse = client.delete(deleteRequest, RequestOptions.DEFAULT);
        System.out.println(deleteResponse.status());
        System.out.println(deleteResponse);
    }

    //更新文档
    @Test
    public void updateDocument() throws IOException {
        UpdateRequest updateRequest = new UpdateRequest("luo_index","1");
        User user = new User("lisi",4);
        //将更新的数据放入请求中
        updateRequest.doc(JSON.toJSONString(user),XContentType.JSON);
        UpdateResponse updateResponse = client.update(updateRequest, RequestOptions.DEFAULT);
        System.out.println(updateResponse.status());
        System.out.println(updateResponse);
    }

    //获取文档
    @Test
    public void getDocument() throws IOException {
        GetRequest getRequest = new GetRequest("luo_index", "1");
        GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);
        System.out.println(getResponse.getSourceAsString());
        System.out.println(getResponse);
    }
/*************************************************批量导入数据***********************************************************/
    //批量插入文档 (批量删除与更新都是类似的)
    @Test
    public void insertDocuments() throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        //设置超时时间，尽量设置大点，批处理会花费时间
        bulkRequest.timeout("10s");
        List<User> userList = new ArrayList<>();
        userList.add(new User("测试1",1));
        userList.add(new User("测试2",2));
        userList.add(new User("测试3",3));
        userList.add(new User("测试4",4));
        userList.add(new User("测试5",5));
        userList.add(new User("测试6",6));
        userList.add(new User("测试7",7));
        userList.add(new User("测试8",8));
        //构建批处理添加请求
        for (int i = 0; i < userList.size(); i++) {
            //批量删除与更新操作只需要修改请求类型即可
            bulkRequest.add(new IndexRequest("luo_index").id("" + i).source(JSON.toJSONString(userList.get(i)),XContentType.JSON));
        }
        BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        System.out.println(bulkResponse.hasFailures());
        System.out.println(bulkResponse);
    }
/*************************************************查询操作(重点)***********************************************************/
    //查询
    @Test
    public void search() throws IOException {
        SearchRequest searchRequest = new SearchRequest("luo_index");
        //创建搜索条件构建器，里面可以设置高亮、分页等功能
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //构建查询条件的查询构建器,不需要new QueryBuilder();可以使用QueryBuilders工具类快速实现
        //QueryBuilders.termQuery:精确查询(注：有大坑，不建议使用 https://blog.csdn.net/qq_43052725/article/details/106056649)
        //TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("name.keyword","测试1");
        // QueryBuilders.matchAllQuery():匹配所有,精确查询推荐使用QueryBuilders.matchPhraseQuer()
        MatchPhraseQueryBuilder matchPhraseQueryBuilder = QueryBuilders.matchPhraseQuery("name", "测试1");
        searchSourceBuilder.query(matchPhraseQueryBuilder);
        //将构建的查询条件放入请求中
        searchRequest.source(searchSourceBuilder);
        //发出请求获取到结果
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println(JSON.toJSONString(searchResponse.getHits()));

        for (SearchHit documentFields : searchResponse.getHits().getHits()) {
            System.out.println(documentFields.getSourceAsString());
        }
    }
/**********************************************************************************************************************/
    //查询（带分页和高亮）
    @Test
    public void highlight() throws IOException {
        SearchRequest searchRequest = new SearchRequest("luo_index");
        //创建搜索条件构建器，里面可以设置高亮、分页等功能
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //分页设置
        searchSourceBuilder.from(1);
        searchSourceBuilder.size(10);
        //高亮设置
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.field("title");
        //开启多个高亮功能(即：关键词重复时，高亮显示所有的关键词)
        highlightBuilder.requireFieldMatch(false);
        highlightBuilder.preTags("<span style='color:red'>");
        highlightBuilder.postTags("</span>");
        searchSourceBuilder.highlighter(highlightBuilder);
        //精确匹配设置
        MatchPhraseQueryBuilder matchPhraseQueryBuilder = QueryBuilders.matchPhraseQuery("title", "测试1");
        searchSourceBuilder.query(matchPhraseQueryBuilder);
        //将构建的查询条件放入请求中
        searchRequest.source(searchSourceBuilder);
        //发出请求获取到结果
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        //解析结果
        List<Map<String,Object>> list = new ArrayList<>();
        for (SearchHit documentFields : searchResponse.getHits().getHits()) {
            //获取高亮字段
            Map<String, HighlightField> highlightFields = documentFields.getHighlightFields();
            HighlightField title = highlightFields.get("title");
            //获取原来的结果(不含高亮字段)
            Map<String,Object> sourceAsMap = documentFields.getSourceAsMap();
            //将获取到的高亮字段替换掉原来的结果
            if(title != null){
                Text[] fragments = title.fragments();
                String new_title = "";
                for (Text fragment : fragments) {
                    //构建新的高亮字段
                    new_title += fragment;
                }
                //替换原来没有高亮的title
                sourceAsMap.put("title",new_title);
            }
            list.add(sourceAsMap);
        }
        //此时list中的数据就是带分页与高亮功能的
        //注：（前端页面要解析高亮的span标签,否则显示的就是字符串）
        System.out.println(list);
    }
}
