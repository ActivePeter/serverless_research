// 关系图
//  - reqs:{
//      - req_id:
//      - dag_id:
// .    - dag_fns:{fn_id:,node:}  dag_fn 与 node 的映射关系
//      - done_fns:
//    }
//  - dags:{
//      - dag_id：
//      - dag图
// .  }
//  - node2node_speed: {node_a,node_b,speed}
//  - nodes:{
//        cpu:
// 。     mem：
// .  }

use std::collections::{BTreeMap, BTreeSet};

use crate::{
    fn_dag::{DagId, FnDagInner, FnId},
    request::ReqId,
    sim_env::SimEnv,
    // util::{ to_range, in_range },
};

use daggy::{petgraph::visit::Topo, Walker};
use serde::Serialize;

#[derive(Serialize)]
struct VecGraph {
    fn_id_2_index: BTreeMap<FnId, usize>,
    vec: Vec<Vec<f32>>,
}

impl VecGraph {
    fn new(graph: &FnDagInner) -> Self {
        let mut topo = Topo::new(graph);
        let mut fn_id_2_index = BTreeMap::new();
        let mut gvec = vec![];
        for _ in 0..graph.node_count() {
            gvec.push(vec![0f32; graph.node_count()]);
        }
        let mut cur_fn_index = 0;
        while let Some(node) = topo.next(graph) {
            let fn_id: FnId = *graph.node_weight(node).unwrap();
            fn_id_2_index.insert(fn_id, cur_fn_index);
            let parents = graph.parents(node);
            for (edge, parent) in parents.iter(graph) {
                let parent_fn_id = graph.node_weight(parent).unwrap();
                let parent_i = *fn_id_2_index.get(&parent_fn_id).unwrap();
                gvec[cur_fn_index][parent_i] = *graph.edge_weight(edge).unwrap();
            }

            cur_fn_index += 1;
        }
        Self {
            fn_id_2_index,
            vec: gvec,
        }
    }
}

#[derive(Serialize)]
struct SerialFunc {
    fn_id: FnId,
    cpu: f32,
    mem: f32,
}

#[derive(Serialize)]
struct SerialDag {
    dag_id: DagId,
    dag_graph: VecGraph,
}

#[derive(Serialize)]
struct SerialRequest {
    req_id: ReqId,
    dag_id: DagId,
    dag_fn_2_node: BTreeMap<FnId, usize>,
    done_fns: BTreeSet<FnId>,
    working_fns: BTreeSet<FnId>,
    total_fn_cnt: usize,
    // next_fn_to_schedule: FnId,
}

#[derive(Serialize)]
struct SerialDoneRequest {
    req_id: ReqId,
    dag_id: DagId,
    start_frame: usize,
    end_frame: usize,
}

#[derive(Serialize)]
struct RunningReqFn {
    req_id: ReqId,
    fn_id: FnId,
}
#[derive(Serialize)]
pub struct SerialNode {
    cpu: f32,
    mem: f32,
    used_cpu: f32,
    used_mem: f32,
    running_req_fns: Vec<RunningReqFn>,
}

#[derive(Serialize)]
pub struct State {
    // node2node_graph: Vec<Vec<f32>>,
    fns: Vec<SerialFunc>,
    dags: Vec<SerialDag>,
    requests: Vec<SerialRequest>,
    done_requests: Vec<SerialDoneRequest>,
    nodes: Vec<SerialNode>,
    cur_frame: usize,
    req_done_time_avg: f32,
    // req_done_time_std: f32,
    // req_done_time_avg_90p: f32,
    cost: f32,
}

impl SimEnv {
    // fn state_node2node_graph(&self) -> Vec<Vec<f32>> {
    //     self.node2node_graph.borrow().clone()
    // }
    fn state_fns(&self) -> Vec<SerialFunc> {
        let mut fns = vec![];
        for f in self.fns.borrow().iter() {
            fns.push(SerialFunc {
                fn_id: f.fn_id,
                cpu: f.cpu,
                mem: f.mem,
            });
        }
        fns
    }
    fn state_dags(&self) -> Vec<SerialDag> {
        let mut dags = vec![];
        for (i, d) in self.dags.borrow().iter().enumerate() {
            let mut dag_topo_walker = Topo::new(&d.dag_inner);
            let mut fn_id_2_index = BTreeMap::new();
            while let Some(gnode) = dag_topo_walker.next(&d.dag_inner) {
                let fn_id = *d.dag_inner.node_weight(gnode).unwrap();
                fn_id_2_index.insert(fn_id, fn_id_2_index.len());
            }
            dags.push(SerialDag {
                dag_id: i,
                dag_graph: VecGraph::new(&d.dag_inner),
            });
        }
        dags
    }
    fn state_requests(&self) -> Vec<SerialRequest> {
        let mut reqs = vec![];
        for (_req_id, req) in self.requests.borrow().iter() {
            let dag_fn_2_node = req.fn_node.iter().map(|(fid, nid)| (*fid, *nid)).collect();
            reqs.push(SerialRequest {
                req_id: req.req_id,
                dag_id: req.dag_i,
                dag_fn_2_node,
                done_fns: req.done_fns.iter().map(|(fnid, _)| *fnid).collect(),
                total_fn_cnt: req.fn_count(self),
                working_fns: req
                    .fn_node
                    .iter()
                    .filter(|(fnid, _)| !req.done_fns.contains_key(*fnid))
                    .map(|(fnid, _)| *fnid)
                    .collect(), // next_fn_to_schedule: req.fn_2_bind_node().unwrap_or_else(|| (0, 0.into())).0,
            });
        }
        reqs
    }

    fn state_done_requests(&self) -> Vec<SerialDoneRequest> {
        let mut done_reqs = vec![];
        for req in self.done_requests.borrow().iter() {
            done_reqs.push(SerialDoneRequest {
                req_id: req.req_id,
                dag_id: req.dag_i,
                start_frame: req.begin_frame,
                end_frame: req.end_frame,
            });
        }
        done_reqs
    }

    pub fn state_nodes(&self) -> Vec<SerialNode> {
        let nodes = self.nodes.borrow();
        let mut serial_nodes = vec![];
        for n in nodes.iter() {
            let mut running_req_fns = vec![];
            for (fnid, fn_cont) in n.fn_containers.borrow().iter() {
                fn_cont
                    .req_fn_state
                    .iter()
                    .for_each(|(req_id, _req_fn_state)| {
                        running_req_fns.push(RunningReqFn {
                            req_id: *req_id,
                            fn_id: *fnid,
                        });
                    });
            }
            serial_nodes.push(SerialNode {
                cpu: n.rsc_limit.cpu,
                mem: n.rsc_limit.mem,
                used_cpu: n.cpu,
                used_mem: n.mem(),
                running_req_fns,
            });
        }
        serial_nodes
    }

    // pub fn state_str(&self) -> String {
    //     let mut str: Vec<u8> = Vec::new();
    //     str.resize(400 * 400, 0);
    //     let fns = self.fns.borrow();
    //     let nodes = self.nodes.borrow();
    //     let fn_line_count = if fns.len() % 400 == 0 {
    //         fns.len() / 400
    //     } else {
    //         fns.len() / 400 + 1
    //     };
    //     let mut set_x_y = |x: usize, y: usize, v: u8| {
    //         str[x + y * 400] = v;
    //     };
    //     let collect_fn_container_schedule = self.algo_collect_ready_2_schedule_metric();
    //     for l in 0..fn_line_count {
    //         for fcol in 0..400 {
    //             if l * 400 + fcol >= fns.len() {
    //                 break;
    //             }
    //             let fnid = l * 400 + fcol;
    //             let _fn_ready_schedule_count = collect_fn_container_schedule.get(&fnid).map_or_else(
    //                 || 0,
    //                 |v| v.ready_2_schedule_fn_count
    //             );
    //             set_x_y(fcol, l * nodes.len(), self.func(fnid).nodes.len() as u8);
    //             set_x_y(fcol, l * nodes.len() + 1, self.func(fnid).cold_start_time as u8);
    //             set_x_y(fcol, l * nodes.len() + 2, self.func(fnid).dag_id as u8);
    //             // 第一行, 对应fn准备好被schedule的count
    //             // nodes行, 对应fn 在节点上运行情况
    //             for n in nodes.iter() {
    //                 if let Some(c) = n.fn_containers.get(&fnid) {
    //                     let mut speed = 1.0 + c.recent_handle_speed() * 10.0;
    //                     if speed > 255.0 {
    //                         speed = 255.0;
    //                     }
    //                     let speed: u8 = unsafe { speed.to_int_unchecked() };
    //                     set_x_y(fcol, l * nodes.len() + n.node_id() + 10, speed);
    //                 } else {
    //                     set_x_y(fcol, l * nodes.len() + n.node_id() + 10, 0);
    //                 }
    //             }
    //         }
    //     }

    //     let next_line = fn_line_count * (nodes.len() + 10);
    //     // 当前scaler标准
    //     {
    //         let scaler = self.scaler.borrow();
    //         let require = match &*scaler {
    //             ScalerImpl::AIScaler(inner) => inner.lass_scaler.latency_required,
    //             _ => 0.0,
    //         };
    //         set_x_y(0, next_line, to_range(require / 20.0, 0, 255) as u8);
    //     }
    //     // 当前请求延迟
    //     set_x_y(1, next_line, to_range(self.req_done_time_avg() / 20.0, 0, 255) as u8);
    //     // 当前平均成本
    //     set_x_y(2, next_line, to_range(self.cost_each_req() / 3.0, 0, 255) as u8);
    //     // 性价比
    //     set_x_y(3, next_line, to_range(self.cost_perform() / 3.0, 0, 255) as u8);
    //     // 观测窗口
    //     set_x_y(4, next_line, *self.each_fn_watch_window.borrow() as u8);

    //     let next_line = next_line + 1;
    //     //nodes 行表达nodes 状态
    //     for n in nodes.iter() {
    //         let l = next_line + n.node_id();
    //         // cpu
    //         set_x_y(0, l, to_range(n.cpu / n.rsc_limit.cpu, 0, 255) as u8);
    //         // mem
    //         set_x_y(1, l, to_range(n.mem / n.rsc_limit.mem, 0, 255) as u8);
    //         // 总任务量
    //         let node_handling_task = n.fn_containers
    //             .borrow()
    //             .iter()
    //             .map(|(_fi, c)| { c.req_fn_state.len() })
    //             .sum::<usize>();
    //         set_x_y(2, l, in_range(node_handling_task, 0, 255) as u8);
    //     }

    //     serde_json::to_string(&str).unwrap()
    // }
    pub fn state(&self) -> State {
        State {
            // node2node_graph: self.state_node2node_graph(),
            fns: self.state_fns(),
            dags: self.state_dags(),
            requests: self.state_requests(),
            done_requests: self.state_done_requests(),
            nodes: self.state_nodes(),
            cur_frame: self.current_frame(),
            req_done_time_avg: self.req_done_time_avg(),
            // req_done_time_std: self.req_done_time_std(),
            // req_done_time_avg_90p: self.req_done_time_avg_90p(),
            cost: *self.cost.borrow(),
        }
    }
}
