package DRCfastq.entities.qualityS;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class qualityScores implements java.io.Serializable {
    private List<String> sample;
    private Map<Long, Double> table;

    private double boarder;
    private byteBuffer[] sb;

    public qualityScores() {
        sample = new ArrayList<>(100000);
        table = new HashMap<>();
    }

    public void setBoarder(double boarder) {
        this.boarder = boarder;
    }

    public double getBoarder() {
        return boarder;
    }

    public void getScore() {

    }

    public Map<Long, Double> getTable() {
        return table;
    }

    public void addTableByturn(long key, double value) {
        this.table.put(key, value);
    }

    public List<String> getSample() {
        return sample;
    }

    public void addSampleByturn(String str) {
        this.sample.add(str);
    }

    public void setSampleByIndex(int num, String str) {
        this.sample.set(num, str);
    }

}
