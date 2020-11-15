package com.kyrie;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DistributeClient {


  private static String connectString="yarn1:2181,yarn2:2181,yarn3:2181";

  private static int sessionTimeout =100000;

  private ZooKeeper client = null;

  private String rootPath = "/servers";




  public static void main(String[] args) throws Exception {

    //1.获取连接
    DistributeClient client = new DistributeClient();
    client.getConnect();
    //2.获取服务列表
    client.getServerList();

    //3.做业务
    client.doBussiness();

  }


  /**
   * 获取连接
   */
  public void getConnect() throws IOException {

    client = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        try {
          getServerList();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    });

  }

  public void getServerList() throws KeeperException, InterruptedException {

    ArrayList<String> servers = new ArrayList<>();
    List<String> children = client.getChildren(rootPath, true);

    for (String child:children) {
      byte[] data = client.getData(rootPath + "/" + child, false, null);
      String hostname = new String(data);
      servers.add(hostname);
    }

    //打印服务列表
    System.out.println(servers);

  }

  /**
   * 业务
   */
  private void doBussiness() throws InterruptedException {
    System.out.println("client is working");
    Thread.sleep(Integer.MAX_VALUE);

  }



}
