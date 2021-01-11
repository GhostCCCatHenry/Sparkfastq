package DRCfastq.util;


import DRCfastq.entities.base_char.MatchEntry;
import DRCfastq.entities.base_char.ref_base;
import DRCfastq.util.base_func.comBase;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;
import scala.Tuple3;

import java.io.IOException;
import java.util.*;

import static org.apache.spark.api.java.StorageLevels.MEMORY_ONLY_SER;


//spark入口与参考序列建立
public class SparkApp {

    private int k = 28;//
    private final int VEC_SIZE = 1 << 20;
    //    private static final int PERCENT = 15; //the percentage of compressed sequence number uses as reference
    private final int kMerLen = 14; //the length of k-mer
    private final int kmer_bit_num = 2 * kMerLen; //bit numbers of k-mer
    private final int hashTableLen = 1 << kmer_bit_num; // length of hash table



    public Tuple2<List<String>,List<MatchEntry>> spark_reads(Iterator<String> s,dealReads dr){
        int i = 0;
        while (s.hasNext()) {
            String str = s.next();
            if(i==3) {
                i = 0;
                continue;
            }
            if(i==0){
                dr.firstLine(str);
            }
            if(i==1){
                dr.secondLine(str);
            }
//            if(i==2){
//                dr.thirdLine(str);
//            }
            i++;
        }

        dr.readFinal();
        return new Tuple2<>(dr.other,dr.me_t);
    }

    public void spark_qs(Iterator<String> s,dealReads dr){
        int i = 0;
        while (s.hasNext()) {
            String str =  s.next();
            if(i==3) {
                i=0;

                continue;
            }

            i++;
        }

    }

    public void s_Compress(ref_base ref,String tar_file,String out_path) throws IOException {
        SparkConf sparkConf = new SparkConf();
        //实现kryo序列化方式并注册需要使用序列化的类
        sparkConf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer");
        // 使用setMaster()可以设置Spark应用程序要连接的Spark集群的master节点的url
        // 但是如果设置为local[*]则代表，在本地使用最大线程运行
        sparkConf.set("spark.kryo.registrator", mykryo.class.getName()).setAppName("geneCompress").setMaster("local[*]");
        sparkConf.set("spark.kryoserializer.buffer.max","2000").set("spark.driver.maxResultSize", "6g").set("spark.shuffle.sort.bypassMergeThreshold","20");
        /*sparkConf.set("spark.default.parallelism", "20");*/
        sparkConf.set("spark.default.parallelism", "40").set("spark.shuffle.file.buffer","3000").set("spark.reducer.maxSizeInFlight", "1000")/*.set("spark.shuffle.io.maxRetries", "6")*/;
        sparkConf.set("spark.broadcast.blockSize", "256m");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        //hadoop的那一套参数在JavaSparkContext里也能设置！
        //最大分片数调整为256M，这样可以涵盖所有的基因文件，因为输入分片最大不会跨越文件。一个分片对应一个partition！
        jsc.hadoopConfiguration().set("mapreduce.input.fileinputformat.split.minsize","268435456");

        Path path2 = new Path(out_path);
        /*
            使用FileSystemAPI
            读取参考序列以及清理输出位置
        * */
        int per = 2;
        FileSystem fs =FileSystem.get(jsc.hadoopConfiguration());
        if(fs.exists(path2)){
            fs.delete(path2,true);
        }
        //对参考序列进行广播变量
        final Broadcast<ref_base> referenceTypeBroadcast = jsc.broadcast(ref);
        final Broadcast<Integer> percent = jsc.broadcast(per);

        JavaRDD<String> input = jsc.textFile(tar_file);

        JavaRDD<Tuple2<List<String>,List<MatchEntry>>> first_match = input.mapPartitions(s1->{
            dealReads dr = new dealReads(referenceTypeBroadcast.value());
            List<Tuple2<List<String>,List<MatchEntry>>> li = new ArrayList<>();
            li.add(spark_reads(s1,dr));
            return li.iterator();
        });

        JavaRDD<Tuple3<Integer,List<String>,List<MatchEntry>>> index = first_match.mapPartitionsWithIndex((v1,v2)->{
            List<Tuple3<Integer,List<String>,List<MatchEntry>>> li = new ArrayList<>();
            Tuple3<Integer,List<String>,List<MatchEntry>> t3 = null;
            while(v2.hasNext()){
                Tuple2<List<String>,List<MatchEntry>> t2 = v2.next();
                t3 = new Tuple3<>(v1,t2._1,t2._2);
            }
            li.add(t3);
            return li.iterator();
        },true).persist(MEMORY_ONLY_SER);


        JavaRDD<Tuple3<Map<Integer,List<MatchEntry>>,Map<Integer,int[]>,Map<Integer,List<Integer>>>> sec_ref =  index.filter(s->s._1()<=percent.getValue()).
                coalesce(1,true).mapPartitions(s->{
            int pr = percent.getValue();
            List<Tuple3<Map<Integer,List<MatchEntry>>,Map<Integer,int[]>,Map<Integer,List<Integer>>>> list = new ArrayList<>();
            Map<Integer,List<MatchEntry>> MatchList = new HashMap<>();
            Map<Integer,List<Integer>> seqLoc = new HashMap<>(pr+1);
            Map<Integer,int[]> seqBucket = new HashMap<>(pr+1);

            while (s.hasNext()){
                Tuple3<Integer,List<String>,List<MatchEntry>> l = s.next();

                comBase.matchResultHashConstruct(l._3(),seqBucket,seqLoc,l._1());
                MatchList.put(l._1(),l._3());
            }
//                    System.out.println(MatchList.size());
            list.add(new Tuple3<>(MatchList,seqBucket,seqLoc));
            return list.iterator();
        });

        Tuple3<Map<Integer,List<MatchEntry>>,Map<Integer,int[]>,Map<Integer,List<Integer>>> ref2 = sec_ref.first();
        ref.set_Ref_code(null);
        ref.setRefLoc(null);
        ref.setRefBucket(null);

        final Broadcast<Tuple3<Map<Integer,List<MatchEntry>>,Map<Integer,int[]>,Map<Integer,List<Integer>>>> sec_broad = jsc.broadcast(ref2);

        index.mapPartitions(s->{
            int pr = percent.getValue();
            List<String> list = new ArrayList<>();
//            Tuple3<Integer,List<MatchEntry>,List<String>> tu3;
            Tuple3<Integer,List<String>,List<MatchEntry>> tar;
            //根据分区号来进行筛选
            while (s.hasNext()&&sec_broad.value()!=null) {
                tar = s.next();
                list = tar._2();
//                Collections.copy(list,tu3._3());
                if(tar._1() == 0){
                    for (int i = 0; i < tar._2().size(); i++) {
                        comBase.saveMatchEntry(list, tar._3().get(i));
                    }
                }else
                    comBase.codeSecondMatch(tar._3(),tar._1()+1,
                            sec_broad.value()._2(),sec_broad.value()._3(),sec_broad.value()._1(), list, pr);//传递的是分区号
            }
            return list.iterator();
        }).saveAsTextFile(path2.toString());
        sec_ref.unpersist();
        first_match.unpersist();

        JavaRDD<Tuple2<List<MatchEntry>,List<String>>> quality_sc = input.mapPartitions(s2->{
            dealReads dr = new dealReads();
            spark_qs(s2,dr);
            return null;
        });

    }
}
