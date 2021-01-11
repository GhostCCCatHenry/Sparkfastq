package DRCfastq.entities.base_char;


//碱基序列文件块
public class bases_Seq implements java.io.Serializable{

    public bases_Seq(){
        SeqInitial();
    }

    private  char []seq_code;//mismatched subsequence

    public int getSeq_len() {
        return seq_len;
    }

    private int seq_len;

    public char getSeq_code_byturn(int len) {
       return seq_code[len];
    }

    public void addSeq_code(char seq_code) {
        this.seq_code[seq_len++] = seq_code;
    }

    public int getnCha_begin_Byturn(int len) {
        return nCha_begin[len];
    }

    public void addnCha_begin(int begin) {
        this.nCha_begin[nCha_len] = begin;
    }

    public int getnCha_length_Byturn(int len) {
        return nCha_length[len];
    }

    public void addnCha_length(int length) {
        this.nCha_length[nCha_len++] = length;
    }

    public void addSpe_cha_pos(int spe_cha_pos) {
        this.spe_cha_pos[spe_cha_len] = spe_cha_pos;
    }

    public void setSpe_cha_pos(int[] spe_cha_pos) {
        this.spe_cha_pos = spe_cha_pos;
    }

    public int getSpe_cha_pos_Byturn(int len) {
        return  spe_cha_pos[len];
    }

    public void setSpe_cha_pos_Byturn(int len,int pos) {
        this.spe_cha_pos[len] = pos;
    }

    public int getSpe_cha_ch_Byturn(int len) {
        return spe_cha_ch[len];
    }


    public void addSpe_cha_ch(int spe_cha_ch) {
        this.spe_cha_ch[spe_cha_len++] = spe_cha_ch;
    }

    private void SeqInitial(){
        int VEC_SIZE = 1 <<17;//length for other character arrays
        nCha_begin= new int[VEC_SIZE/2];
        nCha_length= new int[VEC_SIZE/2];
        spe_cha_pos= new int[VEC_SIZE/4];
        seq_code= new char[10100000];
        spe_cha_ch= new int[VEC_SIZE/4];
    }

    public int getnCha_len() {
        return nCha_len;
    }

    private int nCha_len;

    public void setSeq_code(char[] seq_code) {
        this.seq_code = seq_code;
    }

    public void setnCha_begin(int[] nCha_begin) {
        this.nCha_begin = nCha_begin;
    }

    public void setnCha_length(int[] nCha_length) {
        this.nCha_length = nCha_length;
    }

    private int []nCha_begin;
    private int []nCha_length;
    private int []spe_cha_pos;

    public int getSpe_cha_len() {
        return spe_cha_len;
    }

    private int spe_cha_len;

    public void setSpe_cha_ch(int[] spe_cha_ch) {
        this.spe_cha_ch = spe_cha_ch;
    }

    private int []spe_cha_ch;
}
