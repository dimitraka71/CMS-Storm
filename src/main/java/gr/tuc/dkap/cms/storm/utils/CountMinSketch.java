package gr.tuc.dkap.cms.storm.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Random;

/**
 * Count min sketch implementation for Long values only
 */
public class CountMinSketch {
    private static final Logger LOG = LoggerFactory.getLogger(CountMinSketch.class);
    private static final long PRIME_MODULUS = (1L << 31) - 1;
    private static final int SEED = 7364181;

    private TopKHolder topKHolder;
    private int depth;
    private int width;
    private long[][] table;
    private long[] hashA;

    /**
     * Count min sketch initializer. Array is defined as:
     * <ul>
     * <li>width=2/error</li>
     * <li>depth=log[(1-confidence),base2]</li>
     * </ul>
     *
     * @param error      expected error
     * @param confidence
     */
    public CountMinSketch(double error, double confidence) {
        this.width = (int) Math.ceil(2 / error);
        this.depth = (int) Math.ceil(-Math.log(1 - confidence) / Math.log(2));
        this.topKHolder = new TopKHolder();
        LOG.info("Creating count min sketch with width [" + width + "] and depth [" + depth + "]");
        setupHashTables(depth, width);
    }

    private void setupHashTables(int depth, int width) {
        this.table = new long[depth][width];
        this.hashA = new long[depth];
        Random r = new Random(SEED);
        for (int i = 0; i < depth; ++i) {
            hashA[i] = r.nextInt(Integer.MAX_VALUE);
        }
    }


    /**
     * Add item to cms. Holds user id in topK holder, if needed (if it's count is in the topK percentage to hold)
     *
     * @param item the item to add
     */
    public void add(Long item) {
        // Hash for each row and update count
        for (int i = 0; i < depth; ++i) {
            table[i][hash(item, i)]++;
        }

        // Check whether the value is in the topN percentage
        // We suppose we have a large domain space so we check only the topK values
        Long allSum = 0L;
        for (Map.Entry<Object, Long> entry : topKHolder.getAll().entrySet()) {
            Long counts = entry.getValue();
            allSum = allSum + counts;
        }

        if (estimateCount(item) >= (allSum * 0.01)) {
            topKHolder.addToTopKList(item, estimateCount(item));
        }
    }

    /**
     * Estimate the count of an item
     *
     * @param item the item to estimate value
     * @return the estimated value using cms
     */
    public long estimateCount(Long item) {
        long res = Long.MAX_VALUE;
        for (int i = 0; i < depth; ++i) {
            res = Math.min(res, table[i][hash(item, i)]);
        }
        return res;
    }

    /**
     * @return how many top k values the cms holds
     */
    public int getTopKValuesCount() {
        return topKHolder.getAll().size();
    }

    /**
     * Query for the Top K results the cms holds
     *
     * @param k the k results to return
     * @return the topL results
     */
    public Map<Object, Long> getTopK(int k) {
        return topKHolder.getTopK(k);
    }


    private int hash(long item, int i) {
        long hash = hashA[i] * item;
        hash += hash >> 32;
        hash &= PRIME_MODULUS;
        return ((int) hash) % width;
    }


}
