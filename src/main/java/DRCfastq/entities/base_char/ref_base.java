package DRCfastq.entities.base_char;

//必须复写serializable才能用kryo序列化机制
public class ref_base implements java.io.Serializable{
    private  final int MAX_CHA_NUM ;//maximum length of a chromosome
//    private  final int VEC_SIZE = 1 << 20; //length for other character arrays
    private static int kMerLen = 14; //the length of k-mer
    private static int kmer_bit_num = 2 * kMerLen; //bit numbers of k-mer
    private static int hashTableLen = 1 << kmer_bit_num; // length of hash table
    private  int []refLoc; //reference hash location
    public ref_base(int k){
        MAX_CHA_NUM = 1 << k;
        initial();
    }

    private void initial(){
        refLoc = new int[MAX_CHA_NUM];
        refBucket = new int[hashTableLen];
        ref_code = new char[MAX_CHA_NUM];
    }

    public int[] getRefLoc() {
        return refLoc;
    }

    public int getRefLoc_len() {
        return refLoc_len;
    }
    public void add_refLoc(int loc){refLoc[refLoc_len++] = loc;}

    public void setrefLoc_byturn(int loc,int len){refLoc[len] = loc;}
    public int getrefLoc_byturn(int len){return refLoc[len];}


    private int refLoc_len ;

    public int getRefBucket_Byturn(int len) {
        return refBucket[len];
    }

    public void setrefBucket_byturn(int loc,int len){refBucket[len] = loc;}

    private  int []refBucket; //reference hash bucket



    public void set_Ref_code_len(int ref_code_len) {
        this.ref_code_len = ref_code_len;
    }

    public int getRef_code_len() {
        return ref_code_len;
    }

    public int getRef_low_len() {
        return ref_low_len;
    }

    //长度指示
    private int ref_code_len;

    public void set_Ref_low_len(int ref_low_len) {
        this.ref_low_len = ref_low_len;
    }

    //长度指示位
    private int ref_low_len;

    //参考序列的大写字符数组
    public char get_Ref_code_Byturn(int len) {
        return ref_code[len];
    }

    public void set_Ref_code(char[] ref_code) {
        this.ref_code = ref_code;
    }

    public void set_Ref_code_byturn(char code,int len){this.ref_code[len] = code;}

    private char []ref_code;

    public void setRefLoc(int[] refLoc) {
        this.refLoc = refLoc;
    }

    public void setRefBucket(int[] refBucket) {
        this.refBucket = refBucket;
    }


}
