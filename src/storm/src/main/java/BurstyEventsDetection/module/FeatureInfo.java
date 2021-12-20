package BurstyEventsDetection.module;

import BurstyEventsDetection.lib.Binomial;
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
        public Double get_p() {
            return Double.valueOf(_doc_info.getKey()) / _doc_info.getValue();
        }
    }

    private Feature _feature;
    private Info[] _infos;
    private int avgN;
    private double avgp;

    public FeatureInfo(Feature feature, LinkedBlockingQueue<Info> infos) {
        _feature = feature;
        _infos = infos.toArray(new Info[0]);

        avgN = 0;
        avgp = 0;
        for (Info info : _infos) {
            avgN += info.get_doc_info().getValue();
            avgp += Double.valueOf(info.get_doc_info().getKey()) / info.get_doc_info().getValue();
        }
        avgN /= _infos.length;
        avgp /= _infos.length;
    }
    public Feature get_feature() {
        return _feature;
    }
    public Info[] get_infos() {
        return _infos;
    }
    public int get_N() {
        return avgN;
    }
    public double get_p() {
        return avgp;
    }

    public boolean isStopword() {
        if (_infos.length <= 5) return false;
        boolean res = Binomial.binomial(avgN, avgN, avgp) > 0;
        return res;
    }

}