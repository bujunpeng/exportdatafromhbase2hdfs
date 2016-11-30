package bjp.mr.second.exportdatafromhbase2hdfs.action;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by hadoop on 2016/11/18.
 */
public class HBaseReducer extends Reducer<ImmutableBytesWritable, Put, NullWritable,Text> {

    public void reduce(Text key, Iterable<Text> texts, Context context)
            throws IOException,InterruptedException {

        for(Text text : texts){
            context.write(NullWritable.get(), text);
        }

    }
}
