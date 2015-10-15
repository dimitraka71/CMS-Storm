package gr.tuc.dkap.cms.storm.utils;

import java.util.*;

/**
 * Helper class to hold only the top K values of the CountMinSketch algorithm
 */
public class TopKHolder {

    // (value -> count)
    protected Map<Object, Long> items = new HashMap<Object, Long>();

    /**
     * Add/Update a value's count
     *
     * @param value the value to add
     * @param count the count of the value
     */
    public void addToTopKList(Object value, Long count) {
        items.put(value, count);
    }


    /**
     * @return all the values with the count
     */
    public Map<Object, Long> getAll() {
        return items;
    }


    /**
     * Query for the topK values
     *
     * @param topK the k values to query for
     * @return top K values with count for each value
     */
    public Map<Object, Long> getTopK(int topK) {

        List list = new LinkedList(items.entrySet());
        Collections.sort(list, new Comparator() {
            @Override
            public int compare(Object o1, Object o2) {
                return ((Comparable) ((Map.Entry) (o2)).getValue()).compareTo(((Map.Entry) (o1)).getValue());
            }
        });

        Map<Object, Long> globalTopKResults = new LinkedHashMap<Object, Long>();
        for (Iterator it = list.iterator(); it.hasNext(); ) {
            Map.Entry entry = (Map.Entry) it.next();
            globalTopKResults.put(entry.getKey(), (Long) entry.getValue());
        }
        return globalTopKResults;
    }



}
