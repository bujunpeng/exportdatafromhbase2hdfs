package bjp.mr.second.exportdatafromhbase2hdfs.action;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * Created by hadoop on 2016/11/18.
 *
 *
 *
 *
 *
 */
public class Mapper extends TableMapper<NullWritable, Text> {

    Text outvalue = new Text();

    @Override
    protected void map(ImmutableBytesWritable key, Result value,
                       Context context) throws IOException,InterruptedException {
        outvalue.set(resultToText(value));
        context.write(NullWritable.get(), outvalue);
    }

    private static String resultToText(Result result) throws IOException {
        byte[] _phonenum = result.getRow();
        byte[] _name = result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("name"));

        String phonenum = Bytes.toString(_phonenum);
        String name = Bytes.toString(_name);

        return phonenum + "  " + name;
    }
}
