package bjp.mr.second.exportdatafromhbase2hdfs;

import bjp.mr.second.exportdatafromhbase2hdfs.action.Mapper;
import bjp.mr.second.exportdatafromhbase2hdfs.util.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by hadoop on 2016/11/18.
 *
 *
 * 命令行导出：
 * hbase org.apache.hadoop.hbase.mapreduce.Driver export base_user_info_mix /home/dianqu/data/
 * 数据格式有问题，编码有问题
 * 25分钟
 *
 *
 * 本程序执行不成功，权限问题
 *
 */
public class App extends Constants {

    private Configuration conf = null;
    public Configuration getConf(){return this.conf;}

    private void init(){

        // 这个配置文件主要是记录 kerberos的相关配置信息，例如KDC是哪个IP？默认的realm是哪个？
        // 如果没有这个配置文件这边认证的时候肯定不知道KDC的路径喽
        // 这个文件也是从远程服务器上copy下来的
        System.setProperty("java.security.krb5.conf", "/home/dianqu/krb5.conf");

        conf = HBaseConfiguration.create();
        conf.set("hbase.nameserver.address","10.0.180.2");
        conf.set("hadoop.security.authentication" , "Kerberos" );
        // 这个hbase.keytab也是从远程服务器上copy下来的, 里面存储的是密码相关信息
        // 这样我们就不需要交互式输入密码了
        conf.set("keytab.file" , "/etc/hbase/conf/hbase.keytab" );
        // 这个可以理解成用户名信息，也就是Principal
        conf.set("kerberos.principal" , "dianqu@EXAMPLE.COM" );
        conf.set("hbase.master.kerberos.principal","hbase/_HOST@EXAMPLE.COM");
        conf.set("hbase.regionserver.kerberos.principal","hbase/_HOST@EXAMPLE.COM");
        conf.set("hbase.zookeeper.quorum","nma04-305-bigdata-1802.ctc.local,nma04-305-bigdata-1803.ctc.local,nma04-305-bigdata-1804.ctc.local");
        conf.set("hbase.zookeeper.property.clientPort","2181");
        conf.set("hbase.security.authentication","kerberos");

        UserGroupInformation. setConfiguration(conf);
        try {
            UserGroupInformation. loginUserFromKeytab("dianqu@EXAMPLE.COM", "/home/dianqu/dianqu.keytab" );
        } catch (IOException e) {
//             TODO Auto-generated catch block
            e.printStackTrace();
        }
    }




    public Scan getDataScan(){
        Scan scan = new Scan();
        try {
            scan.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("name"));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return scan;
    }

    public static void main(String[] args) throws IOException {
        InputStream in = App.class.getResourceAsStream("/export.properties");
        Properties p = new Properties();
        p.load(in);
        try {
            App action = new App();
            action.init();
            Scan scan = action.getDataScan();
            Configuration conf = action.getConf();
            Job job = new Job(conf, p.get("jobname-app1").toString());

            job.setMapperClass(Mapper.class);
            job.setMapOutputKeyClass(NullWritable.class);
            job.setMapOutputValueClass(Text.class);
            job.setNumReduceTasks(0);
            job.setJarByClass(App.class);

            TableMapReduceUtil.initTableMapperJob("tcqtest", scan, Mapper.class, NullWritable.class, Text.class, job);
            job.setOutputFormatClass(TextOutputFormat.class);
            FileOutputFormat.setOutputPath(job, new Path(p.get("outputpath-app1").toString()));
            job.waitForCompletion(true);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
