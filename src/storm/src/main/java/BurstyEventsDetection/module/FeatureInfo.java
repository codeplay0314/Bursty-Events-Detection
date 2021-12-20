package BurstyEventsDetection.module;

import javafx.util.Pair;

import java.util.BitSet;
import java.util.concurrent.LinkedBlockingQueue;

public class FeatureInfo {

    public static class Info {
        private String _date;
        private BitSet _doc_set;
        private Pair<Integer, Integer> _doc_info;

        public Info(String date, BitSet doc_set, Pair<Integer, Integer> doc_info) {
            _date = date;
            _doc_set = doc_set;
            _doc_info = doc_info;
        }

        public String get_date() {
            return _date;
        }
        public BitSet get_doc_set() {
            return _doc_set;
        }
        public Pair<Integer, Integer> get_doc_info() {
            return _doc_info;
        }
    }

    private Feature _feature;
    private Info[] _infos;

    public FeatureInfo(Feature feature, LinkedBlockingQueue<Info> infos) {
        _feature = feature;
        _infos = infos.toArray(new Info[0]);
    }
    public Feature get_feature() {
        return _feature;
    }
    public Info[] get_infos() {
        return _infos;
    }
}