import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;

public class TestHDFS {
	public Configuration configuration= null;
	public FileSystem fileSystem = null;


	@Before
	public void before() throws Exception{
		configuration = new Configuration(true);//true
		// fs = FileSystem.get(conf);
		// <property>
		// 	<name>fs.defaultFS</name>
		// 	<value>hdfs://mycluster</value>
		// </property>
		//去环境变量 HADOOP_USER_NAME  root
		fileSystem = FileSystem.get(URI.create("hdfs://mycluster/"),configuration,"root");
	}
	@Test
	public void mkdir() throws Exception {

		Path dir = new Path("/gekun01");
		if(fileSystem.exists(dir)){
			fileSystem.delete(dir,true);
		}
		fileSystem.mkdirs(dir);
	}
	@Test
	public void upload() throws Exception {
		//创建输入流
		BufferedInputStream input = new BufferedInputStream(new FileInputStream(new File("./data/hello.txt")));
		//创建输出流
		Path outfile = new Path("/gekun01/out.txt");
		FSDataOutputStream output = fileSystem.create(outfile,true,1024, (short) 2,1048576L);
		//使用hdfs api通过输入流读入数据，写入到输出六中
		IOUtils.copyBytes(input,output,configuration,true);
	}

	@Test
	public void blocks() throws Exception {
		Path file = new Path("/gekun01/out.txt");
		FileStatus fss = fileSystem.getFileStatus(file);
		BlockLocation[] blks = fileSystem.getFileBlockLocations(fss, 0, fss.getLen());
		for (BlockLocation b : blks) {
			System.out.println(b);
		}
		//        0,        1048576,        node04,node02  A
		//        1048576,  540319,         node04,node03  B
		//通过以上输出可知，每个块记录此块信息相对于文件的偏移量和长度
		//其实用户和程序读取的是文件~！并不知道有块的概念~！
		//面向文件打开的输入流  无论怎么读都是从文件开始读起~！
		FSDataInputStream in = fileSystem.open(file);
		//        blk01: he
		//        blk02: llo msb 66231
		in.seek(1048576);
		//计算向数据移动后，期望的是分治，只读取自己关心（通过seek实现），同时，具备距离的概念（优先和本地的DN获取数据--框架的默认机制）
		System.out.print((char)in.readByte());
		System.out.print((char)in.readByte());
		System.out.print((char)in.readByte());
		System.out.print((char)in.readByte());
		System.out.print((char)in.readByte());
		System.out.print((char)in.readByte());
		System.out.print((char)in.readByte());
		System.out.print((char)in.readByte());
		System.out.print((char)in.readByte());
		System.out.print((char)in.readByte());
		System.out.print((char)in.readByte());
		System.out.print((char)in.readByte());
	}

	@After
	public void after(){
		try {
			fileSystem.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
