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
        ref_base rb = comBase.createRefBroadcast("J:\\gene\\1000\\chr20.fa");
        String input = "J:\\gene\\fastq\\DataSet\\chr20";
        String output = "J:\\gene\\outfastq\\output.fqdc";
        dealReads dealReads = new dealReads(rb,input,output); //HG00119.chrom11.ILLUMINA.bwa.GBR.low_coverage.20120522.fastq
        int i = 0;
        for (File fp : dealReads.input) {
//            dealReads.setOutput(output+fp.getName());
            dealReads.setT_bs(new bases_Seq());
            dealReads.readingSeq_b(fp,i);
            i++;
        }
        for (File fp : dealReads.input) {
            dealReads.readingSeq_q(fp);
        }
        System.out.println("共耗费"+(System.currentTimeMillis()-start)/1000+"秒");

    }

}
