package com.github.hcsp;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.stream.Collectors;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;


public class Crawler {

  private CrawlerDao dao = new MyBatisCrawlerDao();

  public void run() throws SQLException, IOException {

    String link;

    //从数据库中加载下一个连接，如果能加载到，则进行循环
    while ((link =dao.getNextLinkThenDelete()) != null) {
      if (dao.isLinkProcessed(link)) {
        // 判断这个链接是否处理过，如果已经处理继续下一步
        continue;
      }
      if (isInterestingLink(link)) {
        System.out.println(link);
        Document doc = httpGetAndParseHtml(link);
        parseUrlFromPageStoreIntoDatabase(doc);
        storeIntoDatabaseIfItIsNewsPage(doc, link);
        dao.insertProcessedLink(link);
      }
    }
  }


  public static void main(String[] args) throws IOException, SQLException {
    new Crawler().run();

  }

  private void parseUrlFromPageStoreIntoDatabase(Document doc)
      throws SQLException {
    for (Element aTage : doc.select("a")) {
      String href = aTage.attr("href");
      if (href.startsWith("//")) {
        href = "https:" + href;
      }
      if (!href.toLowerCase().startsWith("javascript")) {
        dao.insertLinkToBeProcesssed(href);
      }
    }
  }





  private void storeIntoDatabaseIfItIsNewsPage(Document doc, String link) throws SQLException {
    ArrayList<Element> articleTags = doc.select("article");
    if (!articleTags.isEmpty()) {
      for (Element articleTag : articleTags) {
        String title = articleTags.get(0).child(0).text();
        String content = articleTag.select("p").stream().map(Element::text)
            .collect(Collectors.joining("\n"));
        dao.insertNewsIntoDatabase(link, title, content);
      }
    }
  }

  private static Document httpGetAndParseHtml(String link) throws IOException {
    // 这是我们感兴趣的，我们只处理新浪站内的链接
    CloseableHttpClient httpclient = HttpClients.createDefault();
    System.out.println(link);

    HttpGet httpGet = new HttpGet(link);
    httpGet.addHeader(
        "user-agent",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36");
    try (CloseableHttpResponse response1 = httpclient.execute(httpGet)) {
      HttpEntity entity1 = response1.getEntity();
      String html = EntityUtils.toString(entity1);
      return Jsoup.parse(html);
    }
  }

  private static boolean isInterestingLink(String link) {
    // 只关心news.sina,并排除登录页面
    return (isNewPage(link) || isIndexPage(link)) && isNotLoginPage(link);
  }

  private static boolean isIndexPage(String link) {
    return "https://sina.cn".equals(link);
  }

  private static boolean isNewPage(String link) {
    // 是否是新闻页面
    return link.contains("news.sina.cn");
  }

  private static boolean isNotLoginPage(String link) {
    // 并排除登录页面
    return (!link.contains("passport.sina.cn"));
  }
}



