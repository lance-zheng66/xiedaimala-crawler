package com.github.hcsp;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
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


public class Main {

  private static String getNextLink(Connection connection, String sql) throws SQLException {

    ResultSet resultSet = null;

    try (PreparedStatement statement = connection.prepareStatement(sql)) {
      resultSet = statement.executeQuery();
      while (resultSet.next()) {
        return resultSet.getString(1);
      }
    } finally {
      if (resultSet != null) {
        resultSet.close();
      }
    }
    return null;
  }

  private static String getNextLinkThenDelete(Connection connection) throws SQLException {
    String link = getNextLink(connection, "select link from LINKS_TO_BE_PROCESSED LIMIT 1");
    if (link != null) {
      updateDataBase(connection, link, "DELETE FROM LINKS_TO_BE_PROCESSED where link = ?");
    }
    return link;
  }

  public static void main(String[] args) throws IOException, SQLException {

    Connection connection = DriverManager
        .getConnection("jdbc:h2:file:D:/maven-projects/xiedaimala-crawler/news");

    String link;

    //从数据库中加载下一个连接，如果能加载到，则进行循环
    while ((link = getNextLinkThenDelete(connection)) != null) {
      if (isLinkProcessed(connection, link)) {
        // 判断这个链接是否处理过，如果已经处理继续下一步
        continue;
      }
      if (isInterestingLink(link)) {
        System.out.println(link);
        Document doc = httpGetAndParseHtml(link);
        parseUrlFromPageStoreIntoDatabase(connection, doc);
        storeIntoDatabaseIfItIsNewsPage(connection, doc, link);
        updateDataBase(
            connection, link, "INSERT INTO LINKS_ALREADY_PROCESSED (link) values (?)");
      }
    }
  }

  private static void parseUrlFromPageStoreIntoDatabase(Connection connection, Document doc)
      throws SQLException {
    for (Element aTage : doc.select("a")) {
      String href = aTage.attr("href");
      if (href.startsWith("//")) {
        href = "https:" + href;
      }
      if (!href.toLowerCase().startsWith("javascript")) {
        updateDataBase(
            connection, href, "INSERT INTO LINKS_TO_BE_PROCESSED (link) values (?)");
      }
    }
  }

  private static boolean isLinkProcessed(Connection connection, String link) throws SQLException {
    ResultSet resultSet = null;
    try (PreparedStatement statement =
        connection.prepareStatement("SELECT LINK from LINKS_ALREADY_PROCESSED where link = ?")) {
      statement.setString(1, link);
      resultSet = statement.executeQuery();
      while (resultSet.next()) {
        return true;
      }
    } finally {
      if (resultSet != null) {
        resultSet.close();
      }
    }
    return false;
  }

  private static void updateDataBase(Connection connection, String link, String sql)
      throws SQLException {
    try (PreparedStatement statement = connection.prepareStatement(sql)) {
      statement.setString(1, link);
      statement.executeUpdate();
    }
  }

  private static void storeIntoDatabaseIfItIsNewsPage(Connection connection, Document doc,
      String link) throws SQLException {
    ArrayList<Element> articleTags = doc.select("article");
    if (!articleTags.isEmpty()) {
      for (Element articleTag : articleTags) {
        String title = articleTags.get(0).child(0).text();
        String content = articleTag.select("p").stream().map(Element::text)
            .collect(Collectors.joining("\n"));
        try (PreparedStatement statement = connection.prepareStatement(
            "insert into news(url,title,content,create_at, modified_at)values ( ?,?,?,now(),now() )")) {
          statement.setString(1, link);
          statement.setString(2, title);
          statement.setString(3, content);
          statement.executeUpdate();
        }
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


