package com.yoyocheknow.hadoop;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;

/**
 * 类说明
 * hadoop hdfs java api 操作
 * @author yoyocheknow
 * @date 2018/7/27 10:44
 */
public class AppTest {

    FileSystem  fileSystem=null;
    Configuration configuration=null;
    public static final String HDFS_PATH="hdfs://hadoopMaster:8020";
    @Before
    public void setup() throws Exception{
        System.out.println("HDFS.setup");
        configuration=new Configuration();
        fileSystem=FileSystem.get(new URI(HDFS_PATH),configuration,"root");
    }

    @After
    public void tearDown()throws Exception{
        configuration=null;
        fileSystem=null;
        System.out.println("HDFS.tearDown");
    }
    /*
     *创建目录
     */
    @Test
    public void mkdir()throws Exception{
        fileSystem.mkdirs(new Path("/hdfsapi/test"));
    }
    /*
     *创建文件
     */
//    @Test
//    public void create()throws Exception{
//        FSDataOutputStream out=fileSystem.create(new Path("/hdfsapi/test/a.txt"));
//        out.write("hello Hadoop".getBytes());
//        out.flush();
//        out.close();
//    }
    /*
     *读取hdfs文件并打印在控制台
     */
//    @Test
//    public void cat() throws Exception{
//        FSDataInputStream in=fileSystem.open(new Path("/hdfsapi/test/a.txt"));
//        IOUtils.copyBytes(in, System.out,1024);
//        in.close();
//    }
    /*
     *重命名
     */
//    @Test
//    public void rename()throws Exception{
//        Path oldPath= new Path("/hdfsapi/test/a.txt");
//        Path newPath=new Path("/hdfsapi/test/b.txt");
//        fileSystem.rename(oldPath,newPath);
//    }
    /*
     *从本地 上传文件到hdfs
     */
//    @Test
//    public void copyFromLocalFiles()throws Exception{
//        Path localParh= new Path("D:\\知乎-编辑推荐.txt");
//        Path newPath=new Path("/hdfsapi/test");
//        fileSystem.copyFromLocalFile(localParh,newPath);
//    }
    /*
      *带进度条的上传
      */
//    @Test
//    public void copyFromLocalFileswithProgress()throws Exception{
//        InputStream in =new BufferedInputStream(new FileInputStream(new File("D:\\[洋葱] 课时07：netty服务器的快速实现.mp4")));
//       FSDataOutputStream outputStream=fileSystem.create(new Path("/hdfsapi/test/netty-vedio.mp4")
//               , new Progressable() {
//                   public void progress() {
//                       System.out.print("*");
//                   }
//               });
//      IOUtils.copyBytes(in,outputStream,4096);
//    }
}
