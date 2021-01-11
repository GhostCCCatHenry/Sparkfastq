package DRCfastq.util.base_func;

import DRCfastq.entities.base_char.MatchEntry;
import DRCfastq.entities.base_char.bases_Seq;
import DRCfastq.entities.base_char.ref_base;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.lang.Math.abs;
import static java.lang.Math.min;

public class comBase {

    private static int k = 28;//
    private static final int VEC_SIZE = 1 << 20;
    //    private static final int PERCENT = 15; //the percentage of compressed sequence number uses as reference
    private static final int kMerLen = 14; //the length of k-mer
    private static final int kmer_bit_num = 2 * kMerLen; //bit numbers of k-mer
    private static final int hashTableLen = 1 << kmer_bit_num; // length of hash table
    //    private static int sec_seq_num = 120;
    private static int seqBucketLen = getNextPrime(1<<20);
    public static ref_base createRefBroadcast(String filename) throws IOException {
        String str;
        char []cha;
        int _seq_code_len = 0, _ref_low_len = 1, letters_len = 0;//record lowercase from 1, diff_lowercase_loc[i]=0 means mismatching
        boolean flag = true;
        //int []ref_low_begin = new int[500]; int []ref_low_length = new int[500]; char[] ref_seq = new char[1000];
        char temp_cha;
        File file = new File(filename);

        //最好把参考序列读取本地化，集群化可能会拖慢速度！Driver与Executor的关系！

        BufferedReader br = new BufferedReader(new FileReader(file));//使用Reader缓冲输入流的信息，被BufferedReader读取
        str = br.readLine();
        if(str.equals(">chr1")||str.equals(">chr2")||str.equals(">chr3")||str.equals(">chr4")
                ||str.equals(">chr5")||str.equals(">chr6")||str.equals(">chr7")||str.equals(">chr8")
                ||str.equals(">chr9")||str.equals(">chr10")||str.equals(">chr11")||str.equals(">chr12")
                ||str.equals(">chr13")||str.equals(">chrX")) k = 28;
        else k = 27;
        ref_base ref = new ref_base(k);
        while((str=br.readLine())!=null){
            cha = str.toCharArray();
            for (char a: cha) {
                temp_cha=a;
//                if(Character.isLowerCase(temp_cha)){
//                    if (flag) //previous is upper case
//                    {
//                        flag = false; //change status of flag
//                        ref.set_Ref_low_begin_byturn(letters_len,_ref_low_len);
//                        //ref_low_begin[_ref_low_len] = letters_len;
//                        letters_len = 0;
//                    }
//                    temp_cha = Character.toUpperCase(temp_cha);
//                }
//                else {
//                    if (!flag)  //previous is lower case
//                    {
//                        flag = true;
//                        ref.set_Ref_low_length_byturn(letters_len,_ref_low_len);
//                        _ref_low_len++;
//                        //ref_low_length[_ref_low_len++] = letters_len;
//                        letters_len = 0;
//                    }
//                }
                if (temp_cha == 'A' || temp_cha == 'C' || temp_cha == 'G' || temp_cha == 'T'){
                    ref.set_Ref_code_byturn(temp_cha,_seq_code_len);
                    _seq_code_len++;
                }
                letters_len++;
            }
        }
        br.close();

        ref.set_Ref_code_len(_seq_code_len);
        ref.set_Ref_low_len(_ref_low_len);

        kMerHashingConstruct(ref);
        System.out.println("参考序列创建完毕！");
        return ref;
    }

    private static int getNextPrime(int number) {
        int cur = number + 1;
        boolean prime = false;
        while (!prime)
        {
            prime = true;
            for (int i = 2; i < Math.sqrt(number) + 1; i++) {
                if (cur % i == 0) {
                    prime = false;
                    break;
                }
            }

            if (!prime) cur++;
        }
        return cur;
    }

    private static void kMerHashingConstruct(ref_base ref){
        //initialize the point array
        for (int i = 0; i < hashTableLen; i++)
            ref.setrefBucket_byturn(-1,i);
        int value = 0;
        int step_len = ref.getRef_code_len() - kMerLen + 1;

        //calculate the value of the first k-mer
        for (int k = kMerLen - 1; k >= 0; k--) {
            value <<= 2;
            value += integerCoding(ref.get_Ref_code_Byturn(k));
        }
        ref.setrefLoc_byturn(ref.getRefBucket_Byturn(value),0);
        ref.setrefBucket_byturn(0,value);

        int shift_bit_num = (kMerLen * 2 - 2);
        int one_sub_str = kMerLen - 1;

        //calculate the value of the following k-mer using the last k-merf
        for (int i = 1; i < step_len; i++) {
            value >>= 2;
            value += (integerCoding(ref.get_Ref_code_Byturn(i + one_sub_str))) << shift_bit_num;
            ref.setrefLoc_byturn(ref.getRefBucket_Byturn(value),i);    //refLoc[i] record the list of same values
            ref.setrefBucket_byturn(i,value);
        }
//        System.out.println("kMerHashingConstruct complete.");
    }


    private static int integerCoding(char ch) { //encoding ACGT
        switch (ch) {
            case 'A': return 0;
            case 'C': return 1;
            case 'G': return 2;
            case 'T': return 3;
            default : return -1;
        }
    }

    public static void seqLines(String str, bases_Seq c, int n_letters_len,
             boolean n_flag){
        char temp_cha;
        char[] cha;      //the content of one line

        cha=str.toCharArray();
        //有不满行！
        for (int i = 0; i < cha.length; i++){
            temp_cha = cha[i];

            //temp_cha is an upper letter
            if (temp_cha == 'A' || temp_cha == 'C' || temp_cha == 'G' || temp_cha == 'T')
                c.addSeq_code(temp_cha);
            else if (temp_cha != 'N') {
                c.addSpe_cha_pos(c.getSeq_len());
                c.addSpe_cha_ch(temp_cha - 'A');
            }
            if (!n_flag) {
                if (temp_cha == 'N') {
                    c.addnCha_begin(n_letters_len);
//                        nCha_begin[_nCha_len] = n_letters_len;
                    n_letters_len = 0;
                    n_flag = true;
                }
            }
            else {
                if (temp_cha != 'N') {
                    c.addnCha_length(n_letters_len);
                    n_letters_len = 0;
                    n_flag = false;
                }
            }
            n_letters_len++;
        }
    }


    private static List<Integer> runLengthCoding(int []vec , int length, int tolerance) {
        //File fp =new File(filename);
        List<Integer> code=new ArrayList<>();
        if (length > 0) {
            code.add(vec[0]);
            int cnt = 1;
            for (int i = 1; i < length; i++) {
                if (vec[i] - vec[i-1] == tolerance)
                    cnt++;
                else {
                    code.add(cnt);
                    code.add(vec[i]);
                    cnt = 1;
                }
            }
            code.add(cnt);
        }
        return code;
    }

    public static void saveOtherData(bases_Seq tar, List<String> other) {

        //N字符
        other.add(String.valueOf(tar.getnCha_len()));
        for(int i = 0;i<tar.getnCha_len();i++){
            if(tar.getnCha_length_Byturn(i)==1){
                other.add(String.valueOf(tar.getnCha_begin_Byturn(i)));
            }else
                other.add(String.valueOf(tar.getnCha_begin_Byturn(i))+" "+String.valueOf(tar.getnCha_length_Byturn(i)));
//            other.add(String.valueOf(tar.getnCha_length_Byturn(i)));
        }
        tar.setnCha_begin(null);
        tar.setnCha_length(null);

        //特殊字符
        other.add(String.valueOf(tar.getSpe_cha_len()));
        if(tar.getSpe_cha_len()>0){
//            saveSpeChaData();
            for(int i = 0;i<tar.getSpe_cha_len();i++){
                other.add(String.valueOf(tar.getSpe_cha_pos_Byturn(i))+" "+tar.getSpe_cha_ch_Byturn(i));
//                other.add(String.valueOf(tar.getSpe_cha_ch_Byturn(i)));
            }
        }
        tar.setSpe_cha_pos(null);
        tar.setSpe_cha_ch(null);
//        other.add(" ");
    }

    public static List<MatchEntry> codeFirstMatch(bases_Seq tar, ref_base ref) {
        int pre_pos = 0;
        int min_rep_len = 20;
        int step_len = tar.getSeq_len() - kMerLen + 1;
        int max_length, max_k;
        int i, id, k, ref_idx, tar_idx, length, cur_pos, tar_value;
        StringBuilder mismatched_str = new StringBuilder();
        List<MatchEntry> mr = new ArrayList<>();
        //mismatched_str.reserve(10240);
        MatchEntry me ;
        //matchResult.reserve(VEC_SIZE);
        for (i = 0; i < step_len; i++) {
            tar_value = 0;
            //calculate the hash value of the first k-mer
            for (k = kMerLen - 1; k >= 0; k--) {
                tar_value <<= 2;
                tar_value += integerCoding(tar.getSeq_code_byturn(i+k));
            }

            id = ref.getRefBucket_Byturn(tar_value);
            if (id > -1) {                      //there is a same k-mer in ref_seq_code
                max_length = -1;
                max_k = -1;
                //search the longest match in the linked list
                for (k = id; k != -1; k = ref.getrefLoc_byturn(k)) {
                    ref_idx = k + kMerLen;
                    tar_idx = i + kMerLen;
                    length = kMerLen;

                    while (ref_idx < ref.getRef_code_len() && tar_idx < tar.getSeq_len() &&
                            ref.get_Ref_code_Byturn(ref_idx++) == tar.getSeq_code_byturn(tar_idx++))
                        length++;

                    if (length >= min_rep_len && length > max_length) {
                        max_length = length;
                        max_k = k;
                    }
                }
                //exist a k-mer, its length is larger then min_rep_len
                if (max_length > -1) {
                    me= new MatchEntry();
                    //then save matched information
                    cur_pos = max_k - pre_pos;      //delta-coding for cur_pos
                    me.setPos(cur_pos);
                    me.setLength(max_length - min_rep_len);
                    me.setMisStr(mismatched_str.toString());
                    mr.add(me);
                    i += max_length;
                    pre_pos = max_k + max_length;
                    mismatched_str.delete(0,mismatched_str.length());
                    if (i < tar.getSeq_len())
                        mismatched_str.append(integerCoding(tar.getSeq_code_byturn(i))) ;//mismatched_str存储的是0123数字字符
                    continue;
                }
            }
            mismatched_str.append(integerCoding(tar.getSeq_code_byturn(i))) ;
        }
        if (i < tar.getSeq_len()) {
            for (; i < tar.getSeq_len(); i++)
                mismatched_str.append(integerCoding(tar.getSeq_code_byturn(i))) ;
            me= new MatchEntry();
            me.setPos(0);
            me.setLength(-min_rep_len);                 //no match information, not 0 ,is -min_rep_len;
            me.setMisStr(mismatched_str.toString());
            mr.add(me);
        }
//        System.out.println("seqCodeMatching complete. MatchResult size: "+mr.size());
        tar.setSeq_code(null);
        return mr;
    }

    //计算MatchEntry的hash值
    private static int getHashValue(MatchEntry me) {
        int result = 0;
        for (int i = 0; i < me.getMisStr().length(); i++) {
            result += me.getMisStr().charAt(i) * 92083;
        }
        result += me.getPos() * 69061 + me.getLength() * 51787;
        result %= getNextPrime(1<<20);
        return result;
    }

    //计算matchResult的hash值
    public static void matchResultHashConstruct(List<MatchEntry> matchResult, Map<Integer,int[]> seqBucketVec, Map<Integer,List<Integer>> seqLocVec, int Num) {
        int hashValue1, hashValue2, hashValue;
        List<Integer> seqLoc = new ArrayList<>(VEC_SIZE);
        int []seqBucket = new int[seqBucketLen];
        for (int i = 0; i < seqBucketLen; i++) {
            seqBucket[i] = -1;
        }

        hashValue1 = getHashValue(matchResult.get(0));  //比较getHashValue
        if (matchResult.size() < 2) {
            hashValue2 = 0;
        } else {
            hashValue2 = getHashValue(matchResult.get(1));
        }
        hashValue = Math.abs(hashValue1 + hashValue2) % seqBucketLen;
        seqLoc.add(seqBucket[hashValue]);
        seqBucket[hashValue] = 0;

        for (int i = 1; i < matchResult.size() - 1; i++) {
            hashValue1 = hashValue2;
            hashValue2 = getHashValue(matchResult.get(i + 1));
            hashValue = Math.abs(hashValue1 + hashValue2) % seqBucketLen;
            seqLoc.add(seqBucket[hashValue]);
            seqBucket[hashValue] = i;
        }
        seqLocVec.put(Num,seqLoc);
        seqBucketVec.put(Num,seqBucket);
    }

    private static int getMatchLength(List <MatchEntry> ref_me, int ref_idx, List <MatchEntry> tar_me, int tar_idx) {
        int length = 0;
        while (ref_idx < ref_me.size() && tar_idx < tar_me.size() && compareMatchEntry(ref_me.get(ref_idx++), tar_me.get(tar_idx++)))
            length++;
        return length;
    }

    private static Boolean compareMatchEntry(MatchEntry ref, MatchEntry tar) {
        return  ref.getPos() == tar.getPos() && ref.getLength() == tar.getLength() && ref.getMisStr().equals(tar.getMisStr());
    }

    public static void saveMatchEntry(List<String> list, MatchEntry matchEntry) {
        StringBuilder sbf = new StringBuilder();
        if(!matchEntry.getMisStr().isEmpty()){
            list.add(matchEntry.getMisStr());
        }
        sbf.append(matchEntry.getPos()).append(' ').append(matchEntry.getLength());
        list.add(sbf.toString());
    }

    public static void codeSecondMatch( List<MatchEntry> _mr, int seqNum, Map<Integer,int[]> seqBucket_vec,
                                         Map<Integer,List<Integer>> seqLoc_vec , Map<Integer,List<MatchEntry>> matchResult_vec,
                                         List<String> list,int percent) {
        int hashValue;
        int pre_seq_id=1;
        int max_pos=0, pre_pos=0, delta_pos, length, max_length, delta_length, seq_id=0, delta_seq_id;
        int id, pos, secondMatchTotalLength=0;
        int i;
        StringBuilder sbt = new StringBuilder();
        ArrayList<MatchEntry> misMatchEntry = new ArrayList<>();
        for (i = 0; i < _mr.size()-1; i++) {
            //构建这个matchentry的hash 表 ，每一个matchentry都要编码并且与
            if(_mr.size()<2) hashValue = abs(getHashValue(_mr.get(i))) % seqBucketLen;//一般这种情况不太会发生
            else hashValue = abs(getHashValue(_mr.get(i)) + getHashValue(_mr.get(i+1))) % seqBucketLen;
            max_length = 0;
            //寻找相同序列
            for (int m = 0; m < min( seqNum-1, percent); m++) {
                id = seqBucket_vec.get(m)[hashValue];//寻找出参考序列组的前m个编码序列依次匹配
                if (id!=-1) {
                    for (pos = id; pos!=-1; pos = seqLoc_vec.get(m).get(pos)) {
                        length = getMatchLength(matchResult_vec.get(m), pos, _mr, i);
                        if (length > 1 && length > max_length) {
                            seq_id = m + 1;  //在seqBucket是第m个，但是在seqName里面其实是第m+1个
                            max_pos = pos;
                            max_length = length;//赋值则说明拥有最大长度的序列
                        }
                    }
//                    System.out.println(max_length);
                }
            }
            //达到最长匹配长度后，保存
            if (max_length!=0) {
                delta_seq_id = seq_id - pre_seq_id;//delta encoding, 把seq_id变成0
                delta_length = max_length - 2;//delta encoding, 减小length的数值大小，因为最小替换长度是2
                delta_pos = max_pos - pre_pos;//delta encoding, 减小pos的数值大小
                pre_seq_id = seq_id;
                pre_pos = max_pos + max_length;
                secondMatchTotalLength += max_length;

                //firstly save mismatched matchentry！
                if (!misMatchEntry.isEmpty()) {
                    for (MatchEntry k:misMatchEntry) {
                        saveMatchEntry(list,k);
                    }
                    misMatchEntry.clear();  //只是清空vector中的元素，并不释放内存，如果要释放内存，需要用swap()函数
                }
                //secondly save matched matchentry！
                sbt.append(delta_seq_id).append(' ').append(delta_pos).append(' ').append(delta_length);
                list.add(sbt.toString());
                sbt.delete(0,sbt.length());
                i += max_length - 1;//移动i的位置
            }
            else {
                misMatchEntry.add(_mr.get(i));
            }
        }
        //剩下没存完的 存掉
        if (i == _mr.size()-1)  misMatchEntry.add(_mr.get(i));
        if (!misMatchEntry.isEmpty()) {
            for (MatchEntry matchEntry : misMatchEntry) saveMatchEntry(list, matchEntry);
            misMatchEntry.clear();
        }
        System.out.println(seqNum+" code second match complete. The second match length: "+secondMatchTotalLength);
    }


}
