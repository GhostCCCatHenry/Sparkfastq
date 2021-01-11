package DRCfastq.util;


import DRCfastq.entities.base_char.MatchEntry;
import DRCfastq.entities.base_char.bases_Seq;
import DRCfastq.entities.qualityS.byteBuffer;
import DRCfastq.entities.qualityS.qualityScores;
import DRCfastq.entities.base_char.ref_base;
import DRCfastq.util.base_func.comBase;
import DRCfastq.util.quality_func.q_Compressor;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import scala.Tuple2;
import scala.Tuple3;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class dealReads {

    private File output ;
    public File []input;
    private int lineWid = 0;
    private ref_base rb;
    private BufferedWriter bw;

    //第一行差量三元组
    private int second_size = 0;
    private List<String> name = new ArrayList<>();
    private int namenum1 = 0;
    private int namenum2 = 0;
    private int tempN1 = 0;
    private int tempN2 = 0;
    private List<String> sub1 = new ArrayList<>(10000000);
//    private List<Integer> sub2 = new ArrayList<>(10000000);
    private int linelen = 0;
//    private Tuple3<String,Integer,List<Integer>> tp;
//    private char[] second_ch = new char[100000];
//    private int[] second_len = new int[100000];

    //第二行
    private int tempLen;
    private bases_Seq t_bs;
    private List<bases_Seq> c ;
    public List<MatchEntry> me_t = new ArrayList<>();
    Map<Integer,int[]> seqBucketVec = new HashMap<>();
    Map<Integer,List<Integer>> seqLocVec = new HashMap<>();
    Map<Integer,List<MatchEntry>> matchList = new HashMap<>();
    private int n_letters_len = 0;
    private boolean flag = true;
    private boolean n_flag = false;
    public List<String> other;
    private List<String> out;


    //第四行
    private qualityScores qs;
    private q_Compressor qc;
    private int BUFFER_SIZE = 1 << 25;


    public void setT_bs(bases_Seq t_bs) {
        this.t_bs = t_bs;
        this.other = new ArrayList<>();
        this.out = new ArrayList<>();
    }

//    public void setOutput(String output) throws IOException{
//        bw = new BufferedWriter(new FileWriter(output));
//    }
//
    public dealReads(ref_base rb,String filename,String outname) throws IOException{
//        this.c = new ArrayList<>(100000);
        this.rb = rb;

        qs =  new qualityScores();
        output = new File(outname);
        File tmp = new File(filename);
        if(tmp.isDirectory()){
            input = tmp.listFiles();
        }
        bw = new BufferedWriter(new FileWriter(output));
        qc = new q_Compressor(bw);
    }

    public dealReads(ref_base rb){
//        this.c = new ArrayList<>(100000);
        this.rb = rb;
        t_bs = new bases_Seq();
    }

    public dealReads(){
//        this.rb = rb;
//        t_bs = new bases_Seq();
        qs = new qualityScores();
    }

    //分开读取！
    public void readingSeq_b(File fp,int ii){
//        File f = new File(filename);
        try {
            LineIterator iter = FileUtils.lineIterator(fp,"UTF-8");
            int i = 0;//用于四行一组记录read的行
            while (iter.hasNext()){
                linelen++;
                switch (i){
                    case 0:
                        firstLine(iter.nextLine());
                        ++i;
                        break;
                    case 1:
                        secondLine(iter.nextLine());
                        ++i;
                        break;
                    case 2:
                        iter.nextLine();
                        ++i;
                        break;
                    case 3:
                        iter.nextLine();
                        i=0;
                        break;
                }
            }
            if (n_flag)
                t_bs.addnCha_length(n_letters_len);

            for (int j = t_bs.getSpe_cha_len() - 1; j > 0; j--)
                t_bs.setSpe_cha_pos_Byturn(j,t_bs.getSpe_cha_pos_Byturn(j)-t_bs.getSpe_cha_pos_Byturn(j-1));
            List<MatchEntry> temp = comBase.codeFirstMatch(t_bs,rb);
            me_t.addAll(temp);
            System.out.println("一次匹配完毕");
            comBase.saveOtherData(t_bs,other);
            other.addAll(name);
            other.addAll(sub1);
//            sabe
            secondMatch(ii);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void readFinal(){
        if (n_flag)
            t_bs.addnCha_length(n_letters_len);

        for (int j = t_bs.getSpe_cha_len() - 1; j > 0; j--)
            t_bs.setSpe_cha_pos_Byturn(j,t_bs.getSpe_cha_pos_Byturn(j)-t_bs.getSpe_cha_pos_Byturn(j-1));
        List<MatchEntry> temp = comBase.codeFirstMatch(t_bs,rb);
        me_t.addAll(temp);
        System.out.println("一次匹配完毕");
        comBase.saveOtherData(t_bs,other);
        other.addAll(name);
        other.addAll(sub1);
//        secondMatch();
    }
    public void readingSeq_q(File fp){
//        File f = new File(filename);
        try {
            LineIterator iter = FileUtils.lineIterator(fp,"UTF-8");
            int i = 0;//用于四行一组记录read的行
            boolean call = false;
            String str;
            while (iter.hasNext()){
                if(i!=3){
                    iter.nextLine();
                    i++;
                    continue;
                }
                if(!call){
                    call = qc.qs_compress(iter.nextLine(),qs);
                }else if((str=iter.nextLine())!=null){
                    qc.dealing(str,qs);
                }

                i = 0;
            }
            for(int j = 0; j < 2; ++j) {
                if(qc.sb[j].size() > 0) {
                    qc.write(qc.sb[j], (byte) j, qc.pre[j], qc.cur[j], qc.eline[j]);
                }
            }
//            for(int j = 0; j < 2; ++j) qlen[j] = cur[j];

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void firstLine(String str){
        String[] tmp = str.split("\\.");
        String tname = tmp[0].substring(0,4);
        int num1 = Integer.parseInt(tmp[0].substring(4));
        int num2 = Integer.parseInt(tmp[1]);
        if (name.size()!=0) {
            if(tname.equals(name.get(name.size()-1))){
                sub1.add((tempN1-num1)+" "+(tempN2-num2));
//                sub2.add(tempN2-num2);
                tempN1 = num1;
                tempN2 = num2;
                return;
            }
            name.add(tname+" "+linelen);
//            name.add(tname);
//            name.add(String.valueOf(linelen));
            return;
        }
        tempN1=num1;
        tempN2=num2;
        name.add(tname+" "+linelen+" "+num1+" "+num2);

    }

    public void secondLine(String str){

        if(t_bs.getSeq_len()+str.length()<10100000){
            comBase.seqLines(str,t_bs,n_letters_len,n_flag);
            return;
        }
        long time1 = System.currentTimeMillis();
        List<MatchEntry> temp = comBase.codeFirstMatch(t_bs,rb);
        System.out.println("匹配后"+temp.size()+"个块 "+(System.currentTimeMillis()-time1)/1000);
        me_t.addAll(temp);
        comBase.saveOtherData(t_bs,other);
        t_bs = new bases_Seq();//重新声明一块空间
    }

    private void secondMatch(int ii){
        long time1 = System.currentTimeMillis();
        if(ii<=1)
            comBase.matchResultHashConstruct(me_t,seqBucketVec,seqLocVec,ii);
        matchList.put(ii,me_t);
        System.out.println((System.currentTimeMillis()-time1)/1000);

        for (Map.Entry<Integer, List<MatchEntry>> integerListEntry : matchList.entrySet()) {
            if(integerListEntry.getKey()==0) {
                for (int i = 0; i < integerListEntry.getValue().size(); i++) {
                    saveMatchEntry(integerListEntry.getValue().get(i));
                }
            }else
                comBase.codeSecondMatch(me_t,ii+1,seqBucketVec,seqLocVec,matchList,out,10);
        }
        try {
            for (String s : other) {
                bw.write(s);
                bw.newLine();
            }
            for (String s : out) {
                bw.write(s);
                bw.newLine();
            }
            bw.newLine();
            bw.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void saveFirstLine(List<String> other){

    }

    public void saveMatchEntry(MatchEntry matchEntry) {
        StringBuilder sbf = new StringBuilder();
        if(!matchEntry.getMisStr().isEmpty()){
            out.add(matchEntry.getMisStr());
        }
        sbf.append(matchEntry.getPos()).append(' ').append(matchEntry.getLength());
        out.add(sbf.toString());
    }

    //游程编码方式 将char转换成int
    private void runLengthCoding(char []vec , int length,File file) throws IOException{
        BufferedWriter bw = new BufferedWriter(new FileWriter(file));

        List<Integer> code=new ArrayList<>();
        if (length > 0) {
            code.add((int) vec[0]);
            int cnt = 1;
            for (int i = 1; i < length; i++) {
                if (vec[i] - vec[i-1] == 0)
                    cnt++;
                else {
                    code.add(cnt);
                    code.add((int) vec[i]);
                    cnt = 1;
                }
            }
            code.add(cnt);
//            int cnt = 1;
//            for (int i = 1; i < length-1; i++) {
//                if (vec[i]==vec[i+1])
//                    cnt++;
//                else if(cnt!=1){
//                    code.add(new Tuple2<>(vec[i],cnt));
//
//                    cnt = 1;
//                }else {
//                    code.add(new Tuple2<Character, Integer>(vec[i],null));
//                    if(i==length-2)
//                        code.add(new Tuple2<Character, Integer>(vec[i+1],null));
//                }
//            }
        }
        for (int c : code) {
            bw.write(c);
        }
        bw.newLine();
    }
}
