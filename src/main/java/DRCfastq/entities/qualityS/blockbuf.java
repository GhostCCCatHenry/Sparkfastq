package DRCfastq.entities.qualityS;


public class blockbuf {
//    private BlockInfo info;
    public int start;
    public int end;
    public int eline;
    public byte bucket;
    public byteBuffer in;       // uncompressed input
    public byteBuffer out;      // compressed output
    public String method;         // compression level or "" to mark end of data
    public blockbuf() {}
}
