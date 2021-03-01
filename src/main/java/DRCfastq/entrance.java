package DRCfastq;


import DRCfastq.entities.base_char.bases_Seq;
import DRCfastq.entities.base_char.ref_base;
import DRCfastq.util.SparkApp;
import DRCfastq.util.base_func.comBase;
import DRCfastq.util.dealReads;

import java.io.File;


public class entrance {
    public static void main(String[] args) throws Exception{
//        SparkApp sa = new SparkApp();
//        dealReads ir = new dealReads();
//        ir.readingSeq("J:\\gene\\fastq\\DataSet\\SupplymentData\\ebi\\out.fastq",
//                28);
        long start = System.currentTimeMillis();
        ref_base rb = comBase.createRefBroadcast("C:\\Users\\86059\\Desktop\\FASTQ\\DataSet\\chr20.fa");
//        ref_base rb = new ref_base(1);//test qualityscore
        String input = "C:\\Users\\86059\\Desktop\\FASTQ\\DataSet\\chr20";

//        String input = "C:\\Users\\86059\\Desktop\\FASTQ\\DataSet\\chr11\\HG00119.chrom11.ILLUMINA.bwa.GBR.low_coverage.20120522.fastq";
        String output = "C:\\Users\\86059\\Desktop\\FASTQ\\DataSet\\output.fqdc";
        dealReads dealReads = new dealReads(rb,input,output); //HG00119.chrom11.ILLUMINA.bwa.GBR.low_coverage.20120522.fastq
        int i = 0;
        for (File fp : dealReads.input) {
            dealReads.setT_bs(new bases_Seq());
            dealReads.readingSeq_b(fp,i);
            i++;
        }
        System.gc();
        for (File fp : dealReads.input) {
            dealReads.readingSeq_q(fp);
        }
//        dealReads.setT_bs(new bases_Seq());
//        dealReads.readingSeq_b(new File(input),0);
//        dealReads.readingSeq_q(new File(input));//test qualityscore
        System.out.println("共耗费"+(System.currentTimeMillis()-start)/1000+"秒");
    }

}
