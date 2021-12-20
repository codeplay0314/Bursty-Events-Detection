package BurstyEventsDetection.module;

import java.util.List;

public class Event {
    private List<Feature> _features;
    boolean _end_of_events;

    public Event(List<Feature> features) {
        _features = features;
        _end_of_events = false;
    }
    public Event(boolean end) {
        _end_of_events = end;
    }
    public List<Feature> get() {
        return _features;
    }
    public boolean ends() {
        return _end_of_events;
    }
    public String list() {
        String out = "{" + ((Feature) _features.get(0)).get();
        for (int i = 1; i < _features.size(); i++) {
            out += ", " + ((Feature) _features.get(i)).get();
        }
        out += "}";
        return out;
    }
}
