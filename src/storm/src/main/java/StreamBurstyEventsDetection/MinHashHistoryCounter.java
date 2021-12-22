package StreamBurstyEventsDetection;

public class MinHashHistoryCounter extends MinHashCounter{
    private final int[][] historyHashWindow;
    public int historyCount;

    public MinHashHistoryCounter() {
        super();
        historyHashWindow = new int[128][2];
        for (int i = 0; i < 128; i++) {
            historyHashWindow[i][0] = Integer.MAX_VALUE;
            historyHashWindow[i][1] = 0;
        }
        historyCount = 0;
    }

    @Override
    public void put(int id) {
        super.put(id);
        for (int i = 0; i < 128; i++) {
            if (hashWindow[i][0] < historyHashWindow[i][0]) {
                historyHashWindow[i][0] = hashWindow[i][0];
                historyHashWindow[i][1] = hashWindow[i][1];
            }
        }
        historyCount++;
    }

    public byte[] get_history() {
        byte[] ret = new byte[128];
        for (int i = 0; i < 128; i++)
            ret[i] = (byte)(historyHashWindow[i][1] & 0xff);
        return ret;
    }
}
