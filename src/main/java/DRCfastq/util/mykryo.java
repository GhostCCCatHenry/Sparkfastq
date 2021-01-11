package DRCfastq.util;

import DRCfastq.entities.base_char.MatchEntry;
import DRCfastq.entities.base_char.bases_Seq;
import DRCfastq.entities.base_char.ref_base;
import DRCfastq.entities.qualityS.qualityScores;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import org.apache.spark.serializer.KryoRegistrator;

public class mykryo implements KryoRegistrator {
    @Override
    public void registerClasses(Kryo kryo) {
        kryo.register(ref_base.class, new FieldSerializer(kryo, ref_base.class));  //在Kryo序列化库中注册ref类
        kryo.register(bases_Seq.class, new FieldSerializer(kryo, bases_Seq.class));//在Kryo序列化库中注册target的类
        kryo.register(MatchEntry.class, new FieldSerializer(kryo, MatchEntry.class));
        kryo.register(qualityScores.class, new FieldSerializer(kryo, qualityScores.class));
        //.register(tmp2.class, new FieldSerializer(kryo, tmp2.class));
    }
}
