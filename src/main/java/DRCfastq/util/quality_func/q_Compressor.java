package DRCfastq.util.quality_func;

import DRCfastq.entities.qualityS.blockbuf;
import DRCfastq.entities.qualityS.byteBuffer;
import DRCfastq.entities.qualityS.qualityScores;
import scala.Tuple2;

import java.io.*;
import java.util.*;

import static java.lang.StrictMath.max;

public class q_Compressor {

    //    private Queue<blockbuf> qb = new LinkedList<>();
    private int []qlen = new int[2];
    private int BUFFER_SIZE = 1 << 25;
//    private String METHOD = "55,220,0";
    private blockbuf bf = new blockbuf();
    private List<String> qs_raw = new ArrayList<>();
    private char score;
    private List<String> tid = new ArrayList<>();
    private BufferedWriter bw ;
    private BufferedReader br;
    private int k = 2;
    private float α = (float) 0.1;
    private String s = "";
//    String str;
    public byteBuffer[]sb ;
    public int []cur = new int[3];
    public int []pre = new int[2];
    public int []eline = new int[2];
    private long base = 1;

    private int flag = 0;
    private int num = 0;

    public void get_score(qualityScores qs){
        int []score_cnt = new int[128];
        for(String s : qs.getSample()) {
            for(char c : s.toCharArray()) ++score_cnt[c];
        }
        int max = Arrays.stream(score_cnt).max().getAsInt();
        score = (char) Arrays.binarySearch(score_cnt,max);
    }

    public double get_table(qualityScores qs){
//        int k = 4;
        Map<Long, Integer> mp = new HashMap<>();
        int tot = 0;
        for(String s : qs.getSample()) {
            for(int i = k-1; i < s.length(); ++i) {
                long val = 0;
                for(int l = i+1-k; l <= i; ++l) val = val << 7 | s.charAt(l);
                if(mp.containsKey(val)){
                    mp.put(val,mp.get(val)+1);
                    continue;
                }
                mp.put(val,1);
            }
            tot += s.length()+1-k;
        }
        List<Tuple2<Integer, Long>> vec = new ArrayList<>();
        for(Map.Entry<Long,Integer> m : mp.entrySet()) {
            vec.add(new Tuple2<>(m.getValue(), m.getKey()));
        }

        //对vec排序
        vec.sort((o1,o2)-> {
            if(o1._1.equals(o2._1)) return o1._2<o2._2?-1:((o1._2.equals(o2._2))?0:1);
            return o1._1.compareTo(o2._1);
        });

        //sort(vec.begin(), vec.end(), greater<pair<int, long long>>());
        int cnt = (int)(tot * 0.7);

        for (int i = 0; cnt > 0; ++i) {
            Tuple2<Integer,Long> it = vec.get(i);
            qs.addTableByturn(it._2,it._1/(double)tot);
//            table.put(it._2,it._1/(double)tot) ;
            cnt -= it._1;
        }

        double mx = 0;
        for(String s : qs.getSample()) {
            double score = 0;
            for(int i = k-1; i < s.length(); ++i) {
                long val = 0;
                for(int l = i+1-k; l <= i; ++l) val = val << 7 | s.charAt(l);
                if(qs.getTable().containsKey(val)) score += qs.getTable().get(val);//此处java与unordered_map不同！java查找不同的会报错。
            }
            mx = max(mx, score/(s.length()+1-k));
        }
        return mx;
    }

    public q_Compressor(BufferedWriter output){
        bw = output;
        sb = new byteBuffer[2];//此处的StringBuffer不是字符串操作类，是自定义类！
        sb[0] = new byteBuffer();
        sb[1] = new byteBuffer();
        for(int j = 1; j < k; ++j) base = base << 7;
        base -= 1;
    }

    public q_Compressor() {
        sb = new byteBuffer[2];//此处的StringBuffer不是字符串操作类，是自定义类！
        sb[0] = new byteBuffer();
        sb[1] = new byteBuffer();
        for(int j = 1; j < k; ++j) base = base << 7;
        base -= 1;
    }

    private void pack(byteBuffer in) {
        if(in.size()==0) return;
        byteBuffer out = new byteBuffer(in.size()/2);
        int len = 0; // pending output bytes
        int j = 0, k = 0, l2 = max(33, score-7), l3 = l2 + 4, r = l2 + 7; // last 2 bytes
        for (int c = 0; (c = in.get()) != -1; k = j, j = c) {
            if (len == 0 && (c == score || c >= l2 && c <= r)) ++len;
            else if (len == 1 && (c == score && j == score || c >= l3 && c <= r && j >= l3 && j <= r)) ++len;
            else if (len >= 2 && len<55 && k == score && j == score && c == score) ++len;
            else {  // must write pending output
                ++len;  // c is pending
                if (len>2 && j == score && k == score || len==2 && j == score){
                    out.put(199 + len);len = 1;
                }// [201, 255]

                if (len == 3) {
                    if (c >= l3 && c <= r){
                        out.put(137 + (k - l3) + 4 * (j - l3) + 16 * (c - l3));
                        len = 0;
                    }// [137, 200]
                    else{
                        out.put(73 + (k - l2) + 8 * (j - l2));
                        len = 1;
                    } // [109, 112], [117, 120], [125, 128], [133, 136]
                }
                if (len == 2) {
                    if (c >= l2 && c <= r) {
                        out.put(73 + (j - l2) + 8 * (c - l2));
                        len = 0;
                    }// [73, 108], [113, 116], [121, 124], [129, 132]
                    else {
                        out.put(j - 32);
                        len = 1;
                    } // [32, 39]
                }
                if (len == 1) {
                    if (c == 10) {
                        len = j = 0;
//                        j = 0;
                        k = 0;
                        out.put(0);
                        continue;
                    }
                    if (c<l2 || c>r) {
                        out.put(c - 32);
                        len = 0;
                    }// [4, 31], [40, 72]
                }
            }
        }
        out.swap(in);
    }

    public boolean qs_compress(String str, qualityScores qs) {
//        double border;
        qs.addSampleByturn(str);
        if(qs.getSample().size() == 100000){
            get_score(qs);
            qs.setBoarder(get_table(qs) * α);
            compress_qs(qs);
        }
        return  (qs.getSample().size() == 100000);
    }

    private void compress_qs(qualityScores qs){

        while(num<qs.getSample().size()){
            //交换s与sample[num]的值
            StringBuilder s3 = new StringBuilder();
            String sr = qs.getSample().get(num);
            s3.append(s).append(sr);
            qs.setSampleByIndex(num++,s3.toString().substring(0,s.length()));
            s = s3.toString().substring(s.length());

            dealing(s,qs);
        }
    }

    public void dealing(String s,qualityScores qs){
        long val = 0;
        for(int j = 0; j < k-1; ++j) val = val << 7 | s.charAt(j);
        double score2 = 0;
        for(int j = k-1; j < s.length(); ++j) {
            val = (val & base) << 7 | s.charAt(j);
            if(qs.getTable().containsKey(val)){
                double it = qs.getTable().get(val);
                score2+=it;
            }
        }
        byte res = 0;
        if(score2 < qs.getBoarder()*(s.length()+1-k)) res = 1;
//            byteBuffer temp = sb[res];
        ++cur[res];
        sb[res].write(s.toCharArray(), s.length());
        sb[res].put('\n');
        if(res > 0) {
            sb[0].put('\n');
            ++cur[0];
        }
        if(sb[res].size() > BUFFER_SIZE) {
            write(sb[res],  res, pre[res], cur[res], eline[res]);
            pre[res] = cur[res];
            eline[res] = cur[res+1];
            if(res==1) ++flag;
            else flag = 0;
        }
        if(flag >= 3) {
            write(sb[0], (byte) 0, pre[0], cur[0], eline[0]);
            pre[0] = cur[0];
            eline[0] = cur[1];
            flag = 0;
        }
    }

    public void write(byteBuffer s, byte bucket, int start, int end, int eline){
        blockbuf bf = new blockbuf();
        bf.bucket=bucket;
        bf.start=start;
        bf.end=end;
        bf.eline=eline;
//        bf.method=method;
        bf.in = s;
        pack(bf.in);
        try {
            int i = 0;
            int out = 0;
            bw.write(bucket);
            bw.write(start);
            bw.write(end);
            bw.write(eline);
            while((out = bf.in.get())!=-1){
                i++;
                bw.write(out);
            }
            bw.newLine();
            bw.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
//        qb.add(bf);
    }

}
