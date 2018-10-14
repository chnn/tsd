use chrono::prelude::*;
use std::collections::HashMap;

use tags::{Identifiable, TagSet, TagSetID};

pub struct DB {
    config: Config,
    hot_slabs: HashMap<TagSetID, Vec<Slab>>,
}

pub struct Config {
    slab_duration: i64,
}

struct Slab {
    start_time: i64,
    duration: i64,
    times: Vec<i64>,
    values: Vec<f64>,
    last_modified_time: i64,
}

type Series = (Vec<i64>, Vec<f64>);

impl Slab {
    fn new(start_time: i64, duration: i64) -> Slab {
        let times: Vec<i64> = Vec::new();
        let values: Vec<f64> = Vec::new();
        let last_modified_time = Utc::now().timestamp_nanos();

        return Slab {
            last_modified_time,
            start_time,
            duration,
            times,
            values,
        };
    }

    fn write(&mut self, time: i64, value: f64) {
        self.times.push(time);
        self.values.push(value);
        self.last_modified_time = Utc::now().timestamp_nanos();
    }
}

impl DB {
    pub fn new(config: Config) -> DB {
        let hot_slabs: HashMap<TagSetID, Vec<Slab>> = HashMap::new();

        return DB { config, hot_slabs };
    }

    pub fn write(&mut self, tag_set: &TagSet, time: i64, value: f64) {
        let slabs = self
            .hot_slabs
            .entry(tag_set.id().to_owned())
            .or_insert_with(|| {
                let ss: Vec<Slab> = Vec::new();

                ss
            });

        let maybe_slab = slabs
            .iter_mut()
            .find(|x| x.start_time <= time && x.start_time + x.duration < time);

        let slab = match maybe_slab {
            Some(s) => s,
            None => {
                let s = Slab::new(time, self.config.slab_duration);

                slabs.push(s);
                slabs.last_mut().unwrap()
            }
        };

        slab.write(time, value);
    }

    pub fn read(&self, tag_set: &TagSet, start_time: i64, stop_time: i64) -> Series {
        let maybe_slabs = self.hot_slabs.get(&tag_set.id());

        if maybe_slabs.is_none() {
            let times: Vec<i64> = Vec::new();
            let values: Vec<f64> = Vec::new();

            return (times, values);
        }

        let mut points: Vec<(i64, f64)> = Vec::new();

        for slab in maybe_slabs.unwrap().iter() {
            if slab.start_time >= stop_time || slab.start_time + slab.duration <= start_time {
                continue;
            }

            for (i, time) in slab.times.iter().enumerate() {
                if *time >= start_time && *time < stop_time {
                    points.push((*time, slab.values[i]))
                }
            }
        }

        points.sort_by_key(|p| p.0);
        points.into_iter().unzip()
    }

    pub fn flush(&self) {}
}

#[cfg(test)]
mod test {
    use super::*;
    use tags::parse_tag_set;

    #[test]
    fn write_then_read_series() {
        let mut db = DB::new(Config { slab_duration: 10 });

        let tag_set_a = parse_tag_set(r#""a" = "A", "b" = "B""#);
        let tag_set_b = parse_tag_set(r#""b" = "B", "c" = "C""#);

        // for `tag_set_a`, write the series
        //
        //     [5,   7,   8,   20,  22]
        //     [1.0, 8.1, 2.4, 3.0, 120.6]
        //
        db.write(&tag_set_a, 5, 1.0);
        db.write(&tag_set_a, 7, 8.1);
        db.write(&tag_set_a, 8, 2.4);
        db.write(&tag_set_a, 20, 3.0);
        db.write(&tag_set_a, 22, 120.6);

        // for `tag_set_b`, write the series
        //
        //     [7,    20]
        //     [2.2, -1.1]
        //
        db.write(&tag_set_b, 7, 2.2);
        db.write(&tag_set_b, 20, -1.1);

        let (actual_times0, actual_values0) = db.read(&tag_set_a, 6, 22);

        assert_eq!(actual_times0, vec![7, 8, 20]);
        assert_eq!(actual_values0, vec![8.1, 2.4, 3.0]);

        let (actual_times1, actual_values1) = db.read(&tag_set_a, 0, 50);

        assert_eq!(actual_times1, vec![5, 7, 8, 20, 22]);
        assert_eq!(actual_values1, vec![1.0, 8.1, 2.4, 3.0, 120.6]);

        let (actual_times2, actual_values2) = db.read(&tag_set_b, 0, 50);

        assert_eq!(actual_times2, vec![7, 20]);
        assert_eq!(actual_values2, vec![2.2, -1.1]);

        let (actual_times3, actual_values3) = db.read(&tag_set_b, 50, 100);

        assert_eq!(actual_times3, vec![]);
        assert_eq!(actual_values3, vec![]);
    }

    #[ignore]
    #[test]
    fn write_then_read_series_partial_tag_set() {
        // write series "a=A,b=B" and "b=B,c=C"
        // read series b=B
        // should return all results from both series
    }
}
