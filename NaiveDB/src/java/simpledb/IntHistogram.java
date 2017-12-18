package simpledb;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    private int minValue;
    private int maxValue;
    private int numBuckets;
    private int numTuples;

    // Use TreeMap to maintain order of keys. TreeMap also provides bounds operations(e.g. lower key, ceiling key, etc.)
    private TreeMap<Integer, Integer> histograms;
    private HashMap<Integer, Double> histogramsLen;

    public IntHistogram(int buckets, int min, int max) {
        minValue = min;
        maxValue = max;
        numBuckets = buckets;
        numTuples = 0;

        int intervalLen = (max - min + 1) / buckets;
        int extra = (max - min + 1) % buckets;
        histograms = new TreeMap<>();
        histogramsLen = new HashMap<>();

        int cur = min;
        for (int i = 0; i < buckets; i++) {
            int curLen = intervalLen;
            if (extra > 0) {
                curLen += 1;
                extra--;
            }
            histograms.putIfAbsent(cur + curLen - 1, 0);
            histogramsLen.putIfAbsent(cur + curLen - 1, curLen * 1.0);
            cur = cur + curLen;
        }
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        numTuples++;
        Integer k = histograms.ceilingKey(v);
        histograms.put(k, histograms.get(k) + 1);
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        if (op == op.EQUALS) {
            if (v < minValue || v > maxValue) {
                return 0;
            }
            Map.Entry<Integer, Integer> e = histograms.ceilingEntry(v);
            return (1 / histogramsLen.get(e.getKey())) * e.getValue() / numTuples;
        } else if (op == op.GREATER_THAN) {
            Map.Entry<Integer, Integer> e = histograms.ceilingEntry(v);
            Double[] ret = {0.0};
            histograms.forEach(
                    (k, value) -> {
                        if (e != null && k == e.getKey() && v != k) {
                            ret[0] += (k - v) / histogramsLen.get(k) * value;
                        } else if (e != null && k == e.getKey() && v == k) {
                            ret[0] += 0;
                        } else if (v < k) {
                            ret[0] += value;
                        }
            }
            );
            return ret[0] / numTuples;
        } else if (op == op.GREATER_THAN_OR_EQ) {
            Map.Entry<Integer, Integer> e = histograms.ceilingEntry(v);
            Double[] ret = {0.0};
            histograms.forEach(
                    (k, value) -> {
                        if (e != null && k == e.getKey()) {
                            ret[0] += (k - v + 1) / histogramsLen.get(k) * value;
                        } else if (v < k) {
                            ret[0] += value;
                        }
                    }
            );
            return ret[0] / numTuples;
        } else if (op == op.LESS_THAN) {
            // need to check if v is smaller than minValue
            // because we only save right boundary of our
            // histogram.
            if (v < minValue) {
                return 0;
            }

            Integer ceilingKey = histograms.ceilingKey(v);
            if (ceilingKey == null) {
                // no ceilingKey means that v is greater than
                // maxValue.
                return 1.0;
            }
            Double[] ret = {0.0};
            histograms.forEach (
                    (k, value) -> {
                        if (ceilingKey == k) {
                            ret[0] += (histogramsLen.get(k) - (k - v + 1)) / histogramsLen.get(k) * value;
                        } else if (k < v) {
                            ret[0] += value;
                        }
                    }
            );
            return ret[0] / numTuples;
        } else if (op == op.LESS_THAN_OR_EQ) {
            // need to check if v is smaller than minValue
            // because we only save right boundary of our
            // histogram.
            if (v < minValue) {
                return 0;
            }

            Integer ceilingKey = histograms.ceilingKey(v);
            if (ceilingKey == null) {
                // no ceilingKey means that v is greater than
                // maxValue.
                return 1.0;
            }
            Double[] ret = {0.0};
            histograms.forEach (
                    (k, value) -> {
                        if (ceilingKey == k) {
                            ret[0] += (histogramsLen.get(k) - (k - v)) / histogramsLen.get(k) * value;
                        } else if (k < v) {
                            ret[0] += value;
                        }
                    }
            );
            return ret[0] / numTuples;
        } else if (op == op.NOT_EQUALS) {
            if (v < minValue || v > maxValue) {
                return 1;
            }
            Map.Entry<Integer, Integer> e = histograms.ceilingEntry(v);
            return 1 - (1 / histogramsLen.get(e.getKey())) * e.getValue() / numTuples;
        } else {
            return -1;
        }
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        StringBuffer buffer = new StringBuffer();
        buffer.append("buckets: " + this.numBuckets + " # ");
        buffer.append("min: " + this.minValue + " # ");
        buffer.append("max: " + this.maxValue + " # ");
        int i = 0;
        for (Map.Entry<Integer, Integer> e : this.histograms.entrySet()) {
            buffer.append(i + "th key: " + e.getKey() + " value " + e.getValue());
        }
        return buffer.toString();
    }
}
