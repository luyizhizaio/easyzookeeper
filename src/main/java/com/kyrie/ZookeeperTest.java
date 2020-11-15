package com.kyrie;

import org.apache.zookeeper.*;
import org.junit.Before;
import org.junit.Test;
import sun.jvm.hotspot.runtime.WatcherThread;

import java.io.IOException;
import java.util.List;

/**
 * Hello world!
 *
 */
public class ZookeeperTest {

  private static String connectString="yarn1:2181,yarn2:2181,yarn3:2181";

  private static int sessionTimeout =100000;

  private ZooKeeper client = null;

  @Before
  public void init () throws IOException {

    client = new ZooKeeper(connectString, sessionTimeout,
      new Watcher() {

        @Override
        public void process(WatchedEvent watchedEvent) {

          System.out.println("zk client："+watchedEvent.getPath() +" "+watchedEvent.getType());

          //添加子节点变更监听
          List<String> children = null;
          try {
            children = client.getChildren("/", true);
          } catch (KeeperException e) {
            e.printStackTrace();
          } catch (InterruptedException e) {
            e.printStackTrace();
          }

          for (String child:children) {
            System.out.println("child:"+ child);
          }


          if(watchedEvent.getPath() !=null && watchedEvent.getPath().equals("/kyrie1")){
            byte[] data = new byte[0];
            try {
              data = client.getData("/kyrie1", true, null);
            } catch (KeeperException e) {
              e.printStackTrace();
            } catch (InterruptedException e) {
              e.printStackTrace();
            }

            System.out.println(new String(data));
          }


        }
      });

  }

  @Test
  public void create() throws KeeperException, InterruptedException {

    String node = client.create("/kyrie1", "lcy".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

    System.out.println("node path:"+node);
  }


  @Test
  public void getchildern() throws KeeperException, InterruptedException {

    //添加子节点变更监听
    List<String> children = client.getChildren("/", true);

    for (String child:children) {
      System.out.println("child:"+ child);
    }


    Thread.sleep(Integer.MAX_VALUE);

  }


  /**
   * 获取节点的值,监控节点数据变化
   */
  @Test
  public void getValue() throws KeeperException, InterruptedException {

    //注册节点数据变更监听器
    byte[] data = client.getData("/kyrie1", true, null);

    System.out.println(new String(data));



    Thread.sleep(Integer.MAX_VALUE);

  }




}
