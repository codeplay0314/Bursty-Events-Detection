package BurstyEventsDetection.lib;

import BurstyEventsDetection.module.Event;
import BurstyEventsDetection.module.Feature;

import java.util.*;

public class UnionFind {
    private int[] fa;

    public UnionFind(int n) {
        fa = new int[n];
        for (int i = 0; i < n; i++) fa[i] = i;
    }
    public int find(int x) {
        return x == fa[x]? x: (fa[x] = find(fa[x]));
    }
    public void unite(int x, int y) {
        int u = find(x), v = find(y);
        fa[v] = u;
    }
    public List<List<Integer>> getSet() {
        HashMap<Integer, List<Integer>> map = new HashMap<Integer, List<Integer>>();
        for (int i = 0; i < fa.length; i++) {
            int fa = find(i);
            List<Integer> flist = map.containsKey(fa) ? map.get(fa) : new ArrayList<Integer>();
            flist.add(i);
            map.put(fa, flist);
        }
        List<List<Integer>> res = new ArrayList<>();
        for (int i : map.keySet()) {
            res.add(map.get(i));
        }
        return res;
    }
}