package cn.com.newbee.feng.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 利用MR程序简历HBase二级索引
 * @author newbeefeng
 *
 */
public class BatchCreateSecondIndex {

	/**
	 * 运行方法
	 * @param args
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// 创建配置的
		Configuration conf = HBaseConfiguration.create();
		
		// 创建job
		Job job = Job.getInstance(conf);
		
		// 设定job名称
		job.setJobName("mapreduce on HBase for create second index!");
		
		// 设定任务类（当前类）
		job.setJarByClass(BatchCreateSecondIndex.class);
		
		// 扫描
		Scan scan = new Scan();
		// 设定caching
		scan.setCaching(1000);
		// 对于mr程序来说必须设定为false
		scan.setCacheBlocks(false);
		
		// 初始化mapper
		TableMapReduceUtil.initTableMapperJob("mr_secondindex_resouce", scan, IndexMapper.class, Text.class, Text.class, job);
		   
		// 初始化reducer
		TableMapReduceUtil.initTableReducerJob("mr_secondindex_result", IndexReducer.class, job);
		
		// 提交并等待任务运行完毕
		boolean b = job.waitForCompletion(true);
		if (!b) {
		  throw new IOException("error with job!");
		}
		
	}
	
}

/**
 * 实现具体的mapper类，这个类定义是必须的，因为mr任务可以没有reducer但是一定要有mapper
 * 
 * 此类继承TableMapper
 * 
 * @author newbeefeng
 *
 */
class IndexMapper extends TableMapper<Text,Text>
{

	@Override
	protected void map(ImmutableBytesWritable key, Result value,
			Mapper<ImmutableBytesWritable, Result, Text, Text>.Context context)
			throws IOException, InterruptedException {
		Text k = new Text(Bytes.toString(key.get()));
		Text v = new Text(Bytes.toString(value.getValue(Bytes.toBytes("f"), Bytes.toBytes("age"))));
		
		System.out.println("k = " + k);
		System.out.println("v = " + v);
		context.write(k, v);
	}

}


class IndexReducer extends TableReducer<Text, Text, ImmutableBytesWritable>
{

	@Override
	protected void reduce(Text key, Iterable<Text> values,
			Reducer<Text, Text, ImmutableBytesWritable, Mutation>.Context context)
			throws IOException, InterruptedException {
		Text value = null;
		
		for(Text text : values)
		{
			value = text;
		}
		
		Put put = new Put(Bytes.toBytes(key.toString() + "|" + value.toString()));
		put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("age"), Bytes.toBytes(value.toString()));
		System.out.println(put);
		
		context.write(null, put);

			
	}
	
}


	
	






