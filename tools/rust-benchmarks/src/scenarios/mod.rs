use hdrhistogram::Histogram;

pub mod backpressure;
pub mod basic_stress;
pub mod complex_structures;
pub mod connection_churn;
pub mod feature_check;
pub mod large_payload;
pub mod lua_stress;
pub mod pipeline;
pub mod probabilistic;
pub mod pubsub;
pub mod workload_skew;

pub struct BenchStats {
    pub ops: usize,
    pub histogram: Histogram<u64>,
}

impl BenchStats {
    pub fn new() -> Self {
        Self {
            ops: 0,
            // 3 sig figs, dynamic range handled by library
            histogram: Histogram::<u64>::new(3).unwrap(),
        }
    }

    pub fn merge(&mut self, other: &BenchStats) {
        self.ops += other.ops;
        self.histogram.add(&other.histogram).unwrap();
    }
}
