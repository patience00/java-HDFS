
import lombok.extern.log4j.Log4j2;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

/**
 * @author: 107
 * @date: 2019/1/28 14:02
 * @description:
 * @Review:
 */
public class HDFSUtil {

    /**
     * HDFS文件系统服务器的地址以及端口
     */
    public static final String HDFS_PATH = "hdfs://10.50.40.116:8020";
    /**
     * HDFS文件系统的操作对象
     */
    static FileSystem fileSystem = null;
    /**
     * 配置对象
     */
    static Configuration configuration = null;

    public static void main(String[] args) throws Exception {
        HDFSUtil.connection();
        HDFSUtil.fileList();
    }

    /**
     * 连接HDFS
     *
     * @throws Exception
     */
    public static void connection() throws Exception {
        configuration = new Configuration();
        // 第一参数是服务器的URI，第二个参数是配置对象，第三个参数是文件系统的用户名
        fileSystem = FileSystem.get(new URI(HDFS_PATH), configuration, "hadoop");
        System.out.println("hadoop已连接");
    }

    /**
     * 关闭连接
     *
     * @throws Exception
     */
    public static void shutDown() throws Exception {
        configuration = null;
        fileSystem = null;
    }

    /**
     * 创建文件
     *
     * @throws Exception
     */
    public static void createFile() throws Exception {
        // 创建文件
        FSDataOutputStream outputStream = fileSystem.create(new Path("/demo1/a.txt"));
        // 写入一些内容到文件中
        outputStream.write("hello hadoop".getBytes());
        outputStream.flush();
        outputStream.close();
    }

    /**
     * 查看文件内容
     *
     * @throws Exception
     */
    public static void readFile() throws Exception {
        // 读取文件
        FSDataInputStream in = fileSystem.open(new Path("/demo1/a.txt"));
        // 将文件内容输出到控制台上，第三个参数表示输出多少字节的内容
        IOUtils.copyBytes(in, System.out, 1024);
        in.close();
    }

    /**
     * 重命名
     *
     * @throws IOException
     */
    public static void reName() throws IOException {
        Path oldPath = new Path("/demo1/a.txt");
        Path newPath = new Path("/demo1/b.txt");
        // 第一个参数是原文件的名称，第二个则是新的名称
        fileSystem.rename(oldPath, newPath);
    }

    /**
     * 删除文件
     *
     * @throws Exception
     */
    public static void delete() throws Exception {
        // 删除文件，后面一个参数true代表如果这个目录是个文件夹，则删除文件夹和下面所有的文件，false代表只删除这个文件
        fileSystem.delete(new Path("/demo1/mysql-5.7.24-winx64.zip"), false);
    }

    /**
     * 上传一个本地文件到HDFS
     *
     * @throws IOException
     */
    public static void copyFileToHDFS() throws IOException {
        Path localPath = new Path("C:\\Users\\vcolco\\Desktop\\h.txt");
        Path hdfsPath = new Path("/demo1/");
        // 第一个参数是本地文件的路径，第二个则是HDFS的路径
        fileSystem.copyFromLocalFile(localPath, hdfsPath);
    }

    /**
     * 上传本地大文件，显示进度条
     *
     * @throws IOException
     */
    public static void copyFileWithProgressBar() throws IOException {
        InputStream in = new BufferedInputStream(new FileInputStream(new File("D:\\Setup\\mysql-5.7.24-winx64.zip")));
        FSDataOutputStream outputStream = fileSystem.create(new Path("/demo1/mysql-5.7.24-winx64.zip"), new Progressable() {
            public void progress() {
                // 进度条的输出
                System.out.print(".");
            }
        });
        IOUtils.copyBytes(in, outputStream, 4096);
        in.close();
        outputStream.close();
    }

    /**
     * 下载HDFS文件到本地，下载之后生成了一个crc的文件
     *
     * @throws IOException
     */
    public static void downloadFile() throws IOException {
        Path localPath = new Path("E:/b.txt");
        Path hdfsPath = new Path("/demo1/h.txt");
        fileSystem.copyToLocalFile(hdfsPath, localPath);
    }

    /**
     * windows环境推荐用这种方式下载
     *
     * @throws Exception
     */
    public static void downloadFile2() throws Exception {
        FSDataInputStream in = fileSystem.open(new Path("/demo1/h.txt"));
        OutputStream outputStream = new FileOutputStream(new File("E:/b.txt"));
        IOUtils.copyBytes(in, outputStream, 1024);
        in.close();
        outputStream.close();
    }

    /**
     * 获取文件列表
     *
     * @throws IOException
     */
    public static void fileList() throws IOException {
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/demo1/"));
        for (FileStatus fileStatus : fileStatuses) {
            System.out.println("这是一个：" + (fileStatus.isDirectory() ? "文件夹" : "文件"));
            System.out.println("副本系数：" + fileStatus.getReplication());
            System.out.println("大小：" + fileStatus.getLen() + "字节");
            System.out.println("路径：" + fileStatus.getPath());
        }
    }
}
