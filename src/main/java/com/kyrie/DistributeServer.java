package com.kyrie;

import org.apache.zookeeper.*;

import java.io.IOException;

/**
 * 服务端
 */
public class DistributeServer {

  private static String connectString="yarn1:2181,yarn2:2181,yarn3:2181";

  private static int sessionTimeout =100000;

  private ZooKeeper client = null;

  private String rootPath = "/servers";


  public static void main(String[] args) throws Exception {
    //1.获取zk连接
    DistributeServer server = new DistributeServer();
    server.getConnect();

    //2.向zk注册服务器信息
    String hostname = args[0];
    server.registerServer(hostname);

    //3.启动业务功能

    server.doBussiness();


  }

  /**
   * 业务
   */
  private void doBussiness() throws InterruptedException {
    Thread.sleep(Integer.MAX_VALUE);

  }

  /**
   * 注册服务
   */
  private void registerServer(String hostname) throws KeeperException, InterruptedException {
    String servernode = client.create(rootPath + "/server",
      hostname.getBytes(),
      ZooDefs.Ids.OPEN_ACL_UNSAFE,
      CreateMode.EPHEMERAL_SEQUENTIAL); //短暂 序列

    System.out.println(hostname+ " is online" + servernode);
  }

  /**
   * 获取连接
   */
  public void getConnect() throws IOException {

    client = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
      @Override
      public void process(WatchedEvent event) {

      }
    });

  }


}
