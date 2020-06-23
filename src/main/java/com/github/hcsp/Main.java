package com.github.hcsp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

public class Main {
  public static void main(String[] args) throws IOException {

    //1.建立一个待处理链接池
    List<String> linkPool = new ArrayList<>();
    //2.存放已经处理过的链接的链接池，判断一个东西是否在一个集合里，采用Set
    Set<String> processedLink =new HashSet<>();

    linkPool.add("http://sina.cn");

    while (true){
      if (linkPool.isEmpty()){
          //池子空就跳出
          break;
        }
      String link =  linkPool.remove(linkPool.size()-1);
      //每次从池中拿一个链接,从尾部拿并删除更有效率(先拿后删) arraylist d的remove方法返回删除的元素（先删后拿）
      if (processedLink.contains(link)){
        //判断这个链接是否处理过，如果已经处理继续下一步
          continue;
      }
      if (isInterestingLink(link)){
        Document doc = httpGetAndParseHtml(link);
        doc.select("a").stream().map(aTage -> aTage.attr("href")).forEach(linkPool::add);
        storeIntoDatabaseIfItIsNewsPage(doc);
        processedLink.add(link);
            //ArrayList<Element> links = doc.select("a");
            //获得所有a标签的集合
           // for (Element aTag :links){
          //获取a标签的href属性的值，这个值是一个链接
        //然后把这个结果放入待处理的连接接池
        //linkPool.add(aTag.attr("href"));
        // }
        }
    }
  }

  private static void storeIntoDatabaseIfItIsNewsPage(Document doc) {
    ArrayList<Element> articleTags = doc.select("article");
    //假如这是一个新闻的页面的详情页面，就存入数据库，否则什么都不做
    if (!articleTags.isEmpty()){
      for (Element articleTag : articleTags) {
        String title = articleTags.get(0).child(0).text();
        System.out.println(title);
      }
    }
  }

  private static Document httpGetAndParseHtml(String link) throws IOException {
    //这是我们感兴趣的，我们只处理新浪站内的链接
    CloseableHttpClient httpclient = HttpClients.createDefault();
    System.out.println(link);
    if (link.startsWith("//")){
      link="https:"+link;
      System.out.println(link);
    }
    HttpGet httpGet = new HttpGet(link);
    httpGet.addHeader("user-agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36");

    try (CloseableHttpResponse response1 = httpclient.execute(httpGet)) {
      System.out.println(response1.getStatusLine());
      HttpEntity entity1 = response1.getEntity();
      String html = EntityUtils.toString(entity1);
      return Jsoup.parse(html);
    }
  }

  private static boolean isInterestingLink(String link) {
    //只关心news.sina,并排除登录页面
    return (isNewPage(link)||isIndexPage(link)) && isNotLoginPage(link);
  }

  private static boolean isIndexPage(String link){
    return "http://sina.cn".equals(link);
  }

  private static boolean isNewPage(String link) {
    //是否是新闻页面
    return link.contains("new.sina.cn");
  }

  private static boolean isNotLoginPage(String link) {
    //并排除登录页面
    return (!link.contains("passport.sina.cn"));
  }
}



