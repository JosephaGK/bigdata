package mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class MyMapper extends Mapper<Object, Text, Text, IntWritable> {
	//hadoop有自己一套数据类型可以序列化、反序列化
	//不使用hadoop的数据类型可以自己开发类型，必须：实现序列化，反序列化接口，实现比较器接口
	//排序有两种类型字典序、数值顺序

	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
	//需要计算的数据内容如下，统计每个单词的个数，执行map时，将每个单词map成key为单词，value为1,reduce时进行value统计即可
	//hello hadoop 1
	//hello hadoop 2
	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		//参数key  是每一行字符串自己第一个字节面向源文件的偏移量
		StringTokenizer itr = new StringTokenizer(value.toString());
		while (itr.hasMoreTokens()) {
			word.set(itr.nextToken());
			context.write(word, one);
		}
	}
}
