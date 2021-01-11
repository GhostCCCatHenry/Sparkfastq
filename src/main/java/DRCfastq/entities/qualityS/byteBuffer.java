package DRCfastq.entities.qualityS;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

public class byteBuffer {
    private ByteBuf bb;
//    private List<Byte> bp;  // allocated memory, not NUL terminated, may be NULL
//    private int wpos;       // index of next byte to write, wpos <= al
    private int rpos;       // index of next byte to read, rpos < wpos or return EOF.
//    private int limit;      // max size, default = -1

    public byteBuffer(){
        bb = Unpooled.buffer(128000);
//        bp=new ArrayList<>(128);
        rpos = 0;
    }

    public byteBuffer(int initial){
        bb = Unpooled.buffer(initial);
//        bp=new ArrayList<>(128);
        rpos = 0;
    }

    public void put(char c) {  // write 1 byte
//        assert(!bb..isEmpty());
        bb.writeByte(c);
    }

    public void put(int c) {  // write 1 byte
//        assert(!bb..isEmpty());
        bb.writeByte(c);
    }

    // Write buf[0..n-1]. If buf is NULL then advance write pointer only.
    public void write(char[] buf, int n) {
        if (n<1) return;
//        assert(!bp.isEmpty());
        Charset cs = Charset.forName("UTF-8");
        ByteBuffer b = cs.encode(String.valueOf(buf));

        bb.writeBytes(b);

    }

    // Read a single byte. Return EOF (-1) at end.
    public int get() {
//        assert(rpos<=bb.capacity());
        if(bb.readableBytes()>0)
            return  bb.readByte()&0xff ;
        else return -1;
    }

    public byte[] readAll() {
//        assert(rpos<=wpos);
//        assert(wpos<=al);
//        assert(!al==!p);
//        if (rpos+n>wpos) n=wpos-rpos;
//        if (n>0 && buf) memcpy(buf, p+rpos, n);
//        rpos+=n;
//        System.out.println(bb.nioBuffer());
        return bb.nioBuffer().array();
    }

    public int size() {return bb.readableBytes();}

    //深拷贝
    public void swap(byteBuffer b2){
        byteBuffer temp = new byteBuffer();
        temp.bb = this.bb;
        temp.rpos = this.rpos;
        this.bb = b2.bb;
        this.rpos = b2.rpos;
        b2.bb = temp.bb;
        b2.rpos = temp.rpos;
    }
}
