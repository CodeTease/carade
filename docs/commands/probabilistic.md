# Probabilistic Data Structures

Carade includes probabilistic data structures which are extremely memory efficient at the cost of a small, controlled error rate.

## HyperLogLog

HyperLogLog is used to estimate the cardinality of a set (number of unique elements).

*   **PFADD key element...**: Add elements to the HyperLogLog.
*   **PFCOUNT key...**: Return the approximated cardinality of the set(s) observed by the HyperLogLog at key(s).
*   **PFMERGE destkey sourcekey...**: Merge multiple HyperLogLog values into a single unique value.

## Bloom Filter

A Bloom filter is a probabilistic data structure that is used to test whether an element is a member of a set.

*   **BF.ADD key item**: Add an item to the Bloom Filter.
*   **BF.EXISTS key item**: Check if an item exists in the Bloom Filter.
*   **BF.MADD key item...**: Add multiple items.
*   **BF.MEXISTS key item...**: Check for multiple items.

## T-Digest

T-Digest is a data structure for accurate on-line accumulation of rank-based statistics such as quantiles and trimmed means.

*   **TD.ADD key value...**: Add one or more samples to a sketch.
*   **TD.QUANTILE key quantile...**: Get the value at a specific quantile (0.0 to 1.0).
*   **TD.CDF key value...**: Get the cumulative distribution function (CDF) for a given value.
*   **TD.INFO key**: Get information about the sketch.

See the full list in [Compatibility Matrix](compatibility.md).
