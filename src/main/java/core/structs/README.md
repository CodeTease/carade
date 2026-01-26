# Data Structures Module

This module implements the core probabilistic and deterministic data structures used by Carade to support advanced Redis-compatible data types.

## Core Mechanics

### 1. ZSet (Sorted Set)
*   **Implementation**: `CaradeZSet` utilizes a **SkipList** architecture.
*   **Underlying structures**:
    *   `ConcurrentSkipListSet<ZNode>`: Manages the ordering of elements by score (and member for tie-breaking), providing $O(\log N)$ time complexity for search, insertion, and deletion.
    *   `ConcurrentHashMap<String, Double>`: Provides $O(1)$ access to element scores for quick lookups and updates.
*   **Design Choice**: Java's standard `TreeMap` (Red-Black Tree) is not concurrent-friendly. `ConcurrentSkipListSet` offers a thread-safe, non-blocking alternative that performs well under the single-writer/multi-reader model.

### 2. HyperLogLog (HLL)
*   **Purpose**: Cardinality estimation with fixed memory usage.
*   **Algorithm**: Standard HyperLogLog.
*   **Hash Function**: `MurmurHash64A` (Internal implementation).
*   **Configuration**:
    *   **Registers ($M$)**: $2^{14} = 16384$.
    *   **Register Size**: 8 bits (byte) per register.
    *   **Bias Correction**: Includes standard bias correction for small ranges ($LinearCounting$) and large ranges ($2^{32}$).

### 3. Bloom Filter
*   **Purpose**: Probabilistic set membership testing (False Positives possible, False Negatives impossible).
*   **Algorithm**: Double Hashing strategy to simulate $k$ hash functions.
*   **Logic**: Uses two base hashes ($h1, h2$) derived from a simplified **Murmur3** adaptation. The $i$-th hash is calculated as:
    $$g_i(x) = (h1(x) + i \cdot h2(x)) \pmod m$$
*   **Configuration**:
    *   Default Capacity ($n$): 10,000 items.
    *   Default Error Rate ($p$): 0.01 (1%).
    *   Optimizes bit array size ($m$) and hash count ($k$) based on input parameters.

### 4. T-Digest
*   **Purpose**: Quantile estimation for latency tracking and distribution analysis.
*   **Implementation**: Located in `tdigest/`, uses Centroid-based compression to provide accurate percentiles (p50, p99, p99.9) with minimal memory.

## Technical Specifications

| Structure | Underlying Algo | Default Config | Space Complexity |
| :--- | :--- | :--- | :--- |
| **ZSet** | SkipList + HashMap | N/A | $O(N)$ |
| **HyperLogLog** | HLL (MurmurHash64A) | $M=16384$ ($2^{14}$) | Fixed (~12KB + overhead) |
| **BloomFilter** | Double Hashing | $n=10k, p=0.01$ | $O(n)$ (Configurable) |
| **TDigest** | Centroids | Compression=100 | Configurable |

## Key Components

| Class | Responsibility |
| :--- | :--- |
| `CaradeZSet` | Implements Sorted Set logic (Ranges, Ranks, Scoring). |
| `HyperLogLog` | Implements dense HLL registers and merging logic. |
| `BloomFilter` | Implements bit-array management and double hashing. |
| `TDigest` | Handles quantile estimation and centroid merging. |

## Extension & Usage

*   **Thread Safety**: While `CaradeZSet` uses concurrent collections, complex operations (like `ZINTERSTORE`) should still be coordinated via the `WriteSequencer` to ensure atomicity across multiple keys.
*   **Serialization**: All structures implement `Serializable` for RDB persistence compatibility.
*   **Adding New Types**:
    1.  Create a class implementing `Serializable`.
    2.  Register the type in `DataType` enum (if applicable).
    3.  Implement corresponding Commands in `core.commands`.
