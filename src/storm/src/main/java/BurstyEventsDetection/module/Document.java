package BurstyEventsDetection.module;

import java.util.*;

public class Document {
    private ArrayList<Feature> _val = new ArrayList<Feature>();
    private int _len = 0;

    public ArrayList<Feature> get() {
        return _val;
    }
    public void add(Feature f) {
        _val.add(f);
        _len++;
    }
    void show() {
        String outstr = new String();
        for (int i = 0; i < _len; i++) {
            outstr += ", " + _val.get(i).get();
        }
        System.out.println(outstr);
    }
}
