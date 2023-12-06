use crate::{
    config::{Config, ESConfig},
    // parse_arg,
    sim_env::SimEnv,
};
use chrono;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::{BTreeMap, HashMap},
    fs::{self, File},
    io::{Read, Write},
};

// #[derive(Serialize, Deserialize)]
// pub struct ReqFrame {
//     // reqid
//     r: ReqId,
//     // dagid
//     d: DagId,
//     // new in
//     n: bool,
// }

// #[derive(Serialize, Deserialize)]
// pub struct NodeFrame {
//     // node id
//     n: NodeId,
//     // cpu
//     c: f32,
//     // mem
//     m: f32,
// }

// #[derive(Serialize, Deserialize)]
// pub struct RecordOneFrame {
//     frame: usize,
//     running_reqs: Vec<Value>,
//     nodes: Vec<Value>,
//     req_done_time_avg: f32,
//     req_done_time_std: f32,
//     req_done_time_avg_90p: f32,
//     cost: f32,
//     score:f32,
// }

pub struct OneFrameMetric {
    // pub frame: usize,
    done_request_count: usize,
}

impl OneFrameMetric {
    pub fn new() -> Self {
        Self {
            // frame: 0,
            done_request_count: 0,
        }
    }
    pub fn on_frame_begin(&mut self) {
        // self.frame += 1;
        self.done_request_count = 0;
    }
    pub fn add_done_request(&mut self) {
        self.done_request_count += 1;
    }
    // pub fn done_request_count(&self) -> usize {
    //     self.done_request_count
    // }
}

#[derive(Serialize, Deserialize)]
pub struct Records {
    record_name: String,
    // 0 frame,
    // 1 running_reqs,
    // 2 nodes,
    // 3 req_done_time_avg,
    // 4 req_done_time_std,
    // 5 req_done_time_avg_90p,
    // 6 cost
    pub frames: Vec<Vec<serde_json::Value>>,
}

const FRAME_IDX_FRAME: usize = 0;
const FRAME_IDX_RUNNING_REQS: usize = 1;
const FRAME_IDX_NODES: usize = 2;
const FRAME_IDX_REQ_DONE_TIME_AVG: usize = 3;
const FRAME_IDX_REQ_DONE_TIME_STD: usize = 4;
const FRAME_IDX_REQ_DONE_TIME_AVG_90P: usize = 5;
const FRAME_IDX_COST: usize = 6;
const FRAME_IDX_SCORE: usize = 7;
const FRAME_IDX_DONE_REQ_COUNT: usize = 8;

// the last + 1
const FRAME_LEN: usize = 9;

impl Records {
    pub fn new(mut key: String) -> Self {
        // let args = parse_arg::get_arg();
        key = key.replace(":", "_");
        key = key.replace(",", ".");
        key = key.replace("\"", "");
        let record_name = format!(
            "{}.{}",
            key,
            // match args.scale_from_zero {
            //     ScaleFromZeroType::LazyScaleFromZero => "lazy_scale_from_zero",
            //     ScaleFromZeroType::DirectlyScaleFromZero => "directly_scale_from_zero",
            // },
            chrono::offset::Utc::now().format("UTC_%Y_%m_%d_%H_%M_%S")
        );
        Self {
            record_name,
            frames: Vec::new(),
        }
    }
    pub fn add_frame(&mut self, sim_env: &SimEnv) {
        let mut frame = vec![Value::Null; FRAME_LEN];
        frame[FRAME_IDX_FRAME] = (*sim_env.current_frame.borrow()).into();
        frame[FRAME_IDX_RUNNING_REQS] = sim_env
            .requests
            .borrow()
            .iter()
            .map(|(reqid, req)| {
                serde_json::json!({
                    "r": *reqid,
                    "d": req.dag_i,
                    "n": (req.begin_frame == sim_env.current_frame()),
                })
            })
            .collect::<Vec<_>>()
            .into();
        frame[FRAME_IDX_NODES] = sim_env
            .nodes
            .borrow()
            .iter()
            .map(|node| {
                serde_json::json!( {
                    "n": node.node_id(),
                    "c": node.cpu,
                    "m": node.mem,
                })
            })
            .collect::<Vec<_>>()
            .into();
        frame[FRAME_IDX_REQ_DONE_TIME_AVG] = sim_env.req_done_time_avg().into();
        frame[FRAME_IDX_REQ_DONE_TIME_STD] = sim_env.req_done_time_std().into();
        frame[FRAME_IDX_REQ_DONE_TIME_AVG_90P] = sim_env.req_done_time_avg_90p().into();
        frame[FRAME_IDX_COST] = sim_env.cost_each_req().into();
        frame[FRAME_IDX_SCORE] = sim_env.score().into();
        frame[FRAME_IDX_DONE_REQ_COUNT] = sim_env.metric.borrow().done_request_count.into();

        self.frames.push(frame);
    }
    pub fn flush(&self, env: &SimEnv) {
        if env.config.no_log {
            return;
        }
        if self.frames.len() > 9 {
            fs::create_dir_all("records").unwrap();

            log::info!("flush to target key: {}", self.record_name);
            let mut file = File::create(format!("records/{}.json", self.record_name)).unwrap();
            file.write_all(serde_json::to_string(self).unwrap().as_bytes())
                .unwrap();

            // 计算几个关键指标，输出到对应seed的文件中
        }
    }
}

#[derive(Clone)]
struct RecordFile {
    file_name: String,
    config: Config,
    time_str: String,
}
impl RecordFile {
    fn new(file_name: String) -> Option<Self> {
        if let None = file_name.find("UTC") {
            return None;
        }
        let mut utciter = file_name.split("UTC");
        let front_utc = utciter.next().unwrap().to_owned();
        let back_utc = utciter.next().unwrap().to_owned();

        let mut idx = 0;
        let mut config = Config {
            rand_seed: "".to_owned(),
            request_freq: "".to_owned(),
            dag_type: "".to_owned(),
            cold_start: "".to_owned(),
            fn_type: "".to_owned(),
            // app_types: vec![],
            no_log: false,
            es: ESConfig {
                up: "".to_owned(),
                down: "".to_owned(),
                sche: "".to_owned(),
                ai_type: None,
                down_smooth: "".to_owned(),
                no_perform_cost_rate_score: "".to_owned().into(),
                fit_hpa: None,
            },
        };

        for config_part in front_utc.split(".") {
            match idx {
                0 => {
                    config.rand_seed = config_part.replacen("sd", "", 1);
                }
                1 => {
                    config.request_freq = config_part.replacen("rf", "", 1);
                }
                2 => {
                    config.dag_type = config_part.replacen("dt", "", 1);
                }
                3 => {
                    config.cold_start = config_part.replacen("cs", "", 1);
                }
                4 => {
                    config.fn_type = config_part.replacen("ft", "", 1);
                }
                5 => {
                    config.es.up = config_part.replacen("up", "", 1);
                }
                6 => {
                    config.es.down = config_part.replacen("dn", "", 1);
                }
                7 => {
                    config.es.sche = config_part.replacen("sc", "", 1);
                }
                8 => {
                    config.es.ai_type = if config_part.find("at").is_some() {
                        Some(config_part.replacen("at", "", 1))
                    } else {
                        None
                    };
                }
                9 => {
                    config.es.down_smooth = config_part.replacen("ds", "", 1);
                    break;
                }
                _ => {
                    unreachable!("impossible");
                }
            }
            idx += 1;
        }
        Some(Self {
            file_name,
            config,
            time_str: back_utc,
        })
    }
}

pub fn group_records_by_seed() {
    // 过滤掉已经有记录的seed文件，对剩下的文件进行扫描

    // 找出group文件，将所有已有的seed信息读取出来
    // seed->(config->record_file)
    let mut seed_files: HashMap<String, BTreeMap<String, RecordFile>> = HashMap::new();

    let direntries = fs::read_dir("./records").unwrap();
    for entry in direntries {
        let entry = entry.unwrap();
        let file_name = entry.file_name().into_string().unwrap();
        if file_name.ends_with(".json") {
            if let Some(record_file) = RecordFile::new(file_name) {
                log::info!("read one record file: {}", record_file.file_name);
                let different_confs = seed_files
                    .entry(record_file.config.rand_seed.clone())
                    .or_insert_with(|| {
                        let map = BTreeMap::new();
                        map
                    });
                different_confs
                    .entry(record_file.config.str())
                    .and_modify(|v| {
                        if record_file.time_str > v.time_str {
                            *v = record_file.clone();
                        }
                    })
                    .or_insert(record_file);
            }
            // .insert(record_file.config.str(), record_file);
        }
    }

    let mut seeds_metrics_cache: HashMap<String, Vec<Vec<Value>>> = HashMap::new();

    // flush seed infos
    for (seed, recordfiles) in seed_files {
        // let mut config_metrics = vec![];
        let mut count = recordfiles.len();
        if !seeds_metrics_cache.contains_key(&seed) {
            seeds_metrics_cache.insert(seed.clone(), get_seed_metrics(&seed).map_or(vec![], |v| v));
        }
        let mut config_metrics = seeds_metrics_cache.get_mut(&seed).unwrap();
        for (configstr, f) in recordfiles.iter().rev() {
            let mut read_data = || {
                let mut records: Records = serde_json::from_str(
                    &fs::read_to_string(format!("records/{}", f.file_name)).unwrap(),
                )
                .unwrap();
                count -= 1;
                log::info!(
                    "deserialed one record file: {}, left:{}",
                    f.file_name,
                    count
                );
                if records.frames.len() < 999 {
                    return None;
                }

                let last_frame = records.frames.iter_mut().rev().next().unwrap();
                let cost_per_req = last_frame[FRAME_IDX_COST].take();
                let time_per_req = last_frame[FRAME_IDX_REQ_DONE_TIME_AVG].take();
                let score = last_frame[FRAME_IDX_SCORE].clone();
                drop(last_frame);
                // let req_done = last_frame[FRAME_IDX_DONE_REQ_COUNT].take();
                let rps = records
                    .frames
                    .iter()
                    .map(|f| f[FRAME_IDX_DONE_REQ_COUNT].as_f64().unwrap())
                    .sum::<f64>()
                    / records.frames.len() as f64;

                let one_config_info: Vec<Value> = vec![
                    configstr.clone().into(),
                    cost_per_req,
                    time_per_req,
                    score,
                    rps.into(),
                    f.time_str.clone().into(),
                ];
                Some(one_config_info)
            };
            if let Some(update) = config_metrics
                .iter_mut()
                .filter(|config| config[0].as_str().unwrap() == &*configstr)
                .next()
            {
                if update.len() > 4 {
                    // check time is bigger
                    if update[5].as_str().unwrap() < f.time_str.as_str() {
                        if let Some(one_config_info) = read_data() {
                            *update = one_config_info;
                        }
                    }
                } else {
                    if let Some(one_config_info) = read_data() {
                        *update = one_config_info;
                    }
                }
            } else {
                if let Some(one_config_info) = read_data() {
                    config_metrics.push(one_config_info);
                }
            }
        }
        let mut file = File::options()
            .append(false)
            .write(true)
            .read(true)
            .create(true)
            .truncate(true)
            .open(format!("records/seed_{}.json", seed))
            .unwrap();
        file.write_all(serde_json::to_string(&config_metrics).unwrap().as_bytes())
            .unwrap();
    }
}

/// used sync io operation, use spawn_blocking
/// return: seed->[[configstr, cost, time, score]...]
pub fn get_seeds_metrics<'a>(
    seeds: impl Iterator<Item = &'a String>,
) -> HashMap<String, Vec<Vec<Value>>> {
    let mut seeds_metrics = HashMap::new();
    for seed in seeds {
        if let Some(metrics) = get_seed_metrics(seed) {
            seeds_metrics.insert(seed.clone(), metrics);
        }
    }
    seeds_metrics
}

/// return: [[configstr, cost, time, score]...]
pub fn get_seed_metrics(seed: &String) -> Option<Vec<Vec<Value>>> {
    let mut seed_metrics = vec![];
    if let Ok(mut file) = File::options()
        .read(true)
        .write(true)
        .append(false)
        .open(format!("records/seed_{}.json", seed))
    {
        let mut content = String::new();
        file.read_to_string(&mut content).unwrap();
        if content.len() == 0 {
            return None;
        }
        let records: Vec<Vec<Value>> = serde_json::from_str(&content).unwrap_or_else(|_| {
            panic!("deserial failed: {}", content);
        });
        for record in records {
            seed_metrics.push(record);
        }
        Some(seed_metrics)
    } else {
        None
    }
}
