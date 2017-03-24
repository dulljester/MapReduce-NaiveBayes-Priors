package ca.dal.csci6405.project.mrjob02;
import java.io.*;
import java.util.*;
import javax.json.*;
import javax.json.stream.JsonGenerator;
import org.apache.hadoop.io.WritableComparable;

public class StatMap implements WritableComparable<StatMap> {

    private Map<Integer,Map<Byte,Integer>> counts = new TreeMap<>();
    static Map<String,Object> config = new HashMap<>();
    static {
        config.put(JsonGenerator.PRETTY_PRINTING, true);
    }

    public StatMap( Long key, Integer frequency ) {
        int pos = MyUtils.getPos(key);
        counts.put(pos,new TreeMap<>());
        counts.get(pos).put( (byte)(((key>>(pos*MyUtils.WIDTH))&MyUtils.MASK(MyUtils.WIDTH))),frequency);
    }

    public StatMap() {}

    private String repr = null;

    @Override
    public String toString() {
        if ( repr != null ) return repr;
        JsonBuilderFactory factory = Json.createBuilderFactory(config);
        JsonArrayBuilder builder = factory.createArrayBuilder();
        for ( int i = 0; i < MyUtils.M; ++i )
            if ( counts.containsKey(i) ) {
                JsonObjectBuilder objectBuilder = factory.createObjectBuilder();
                for ( Map.Entry<Byte,Integer> entry: counts.get(i).entrySet() )
                    objectBuilder.add(Byte.toString(entry.getKey()),entry.getValue());
                builder.add(i,objectBuilder.build());
            }
            else
                builder.add(i,factory.createObjectBuilder().build());
        return repr = builder.build().toString();
    }

    void add( StatMap map ) {
        for ( Map.Entry<Integer,Map<Byte,Integer>> entry: map.counts.entrySet() )
            if ( counts.containsKey(entry.getKey()) ) {
                Map<Byte,Integer> m = counts.get(entry.getKey());
                for ( Map.Entry<Byte,Integer> e: entry.getValue().entrySet() )
                    if ( m.containsKey(e.getKey()) )
                        m.put(e.getKey(),m.get(e.getKey())+e.getValue());
                    else
                        m.put(e.getKey(),e.getValue());
            }
            else
                counts.put(entry.getKey(),entry.getValue());
        repr = null; // the map has been updated, need to recalculate the string representation
    }

    @Override
    public int compareTo(StatMap o) {
        return toString().compareTo(o.toString());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(toString());
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        String repr = dataInput.readUTF().toString();
        counts = null; //now it's available for Garbage collection
        counts = new TreeMap<>();

        JsonReaderFactory factory = Json.createReaderFactory(config);
        JsonReader reader = factory.createReader(new StringReader(repr));
        JsonArray array = reader.readArray();
        for ( int i = 0; i < MyUtils.M; ++i ) {
            JsonObject object = array.getJsonObject(i);
            if ( object.size() == 0 ) continue ;
            counts.put(i,new TreeMap<>());
            for ( Map.Entry<String,JsonValue> entry: object.entrySet() )
                counts.get(i).put(Byte.parseByte(entry.getKey()),Integer.parseInt(entry.getValue().toString()));
        }
    }
}

