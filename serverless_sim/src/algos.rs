use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    hash::Hash,
};

use crate::{
    fn_dag::FnId,
    node::{Node, NodeId},
    request::{ReqId, Request},
    sim_env::SimEnv,
};

#[derive(Clone, Debug)]
pub struct ContainerMetric {
    pub container_count: usize,
    pub scheduled_fn_count: usize,
    pub ready_2_schedule_fn_reqs: Vec<ReqId>,
}

impl ContainerMetric {
    pub fn ready_2_schedule_fn_count(&self) -> usize {
        self.ready_2_schedule_fn_reqs.len()
    }
}

impl SimEnv {
    pub fn algo_get_fn_all_scheduled_metric(
        &self,
        fns_ready_2_schedule: &HashMap<FnId, ContainerMetric>,
    ) -> Vec<(FnId, ContainerMetric)> {
        self.fns
            .borrow()
            .iter()
            .filter(|f| !fns_ready_2_schedule.contains_key(&f.fn_id))
            .map(|f| {
                (
                    f.fn_id,
                    ContainerMetric {
                        container_count: self.fn_container_cnt(f.fn_id),
                        scheduled_fn_count: self.fn_2_nodes.borrow().get(&f.fn_id).map_or_else(
                            || 0,
                            |nodes| {
                                nodes
                                    .iter()
                                    .map(|n| {
                                        self.node(*n).container(f.fn_id).unwrap().req_fn_state.len()
                                    })
                                    .sum()
                            },
                        ),
                        ready_2_schedule_fn_reqs: vec![],
                    },
                )
            })
            .collect()
    }
    pub fn algo_collect_req_ready_2_schedule(&self) -> BTreeMap<ReqId, VecDeque<FnId>> {
        let env = self;
        let mut collect_map: BTreeMap<ReqId, VecDeque<FnId>> = BTreeMap::new();
        // 对于已经进来的请求，scale up 已经没有前驱的fns
        for (&reqid, req) in env.requests.borrow().iter() {
            let req_dag = env.dag(req.dag_i);
            let mut walker = req_dag.new_dag_walker();
            'outer: while let Some(f) = walker.next(&req_dag.dag_inner) {
                let fnid = req_dag.dag_inner[f];
                if req.done_fns.contains_key(&fnid) || req.fn_node.contains_key(&fnid) {
                    // log::info!("req {} fn {} done, no need to scale for", req.req_id, fnid,);
                    continue;
                }

                //确定前驱已完成
                let parent_fns = env.func(fnid).parent_fns(env);
                for p in &parent_fns {
                    if req.get_fn_node(*p).is_none() || !req.done_fns.contains_key(p) {
                        // exist a parent fn not done
                        // log::info!(
                        //     "req {} fn {} parent_fn {} on node {} not done",
                        //     req.req_id,
                        //     fnid,
                        //     p,
                        //     req.get_fn_node(*p).unwrap_or(1000000)
                        // );
                        continue 'outer;
                    }
                }

                collect_map
                    .entry(reqid)
                    .and_modify(|q| q.push_back(fnid))
                    .or_insert_with(|| {
                        let mut q = VecDeque::new();
                        q.push_back(fnid);
                        q
                    });
            }
        }
        collect_map
    }
    pub fn algo_collect_ready_2_schedule_metric(&self) -> HashMap<FnId, ContainerMetric> {
        let env = self;
        let mut collect_map: HashMap<FnId, ContainerMetric> = HashMap::new();
        // 对于已经进来的请求，scale up 已经没有前驱的fns
        for (_reqid, req) in env.requests.borrow().iter() {
            let req_dag = env.dag(req.dag_i);
            let mut walker = req_dag.new_dag_walker();
            'outer: while let Some(f) = walker.next(&req_dag.dag_inner) {
                let fnid = req_dag.dag_inner[f];
                if req.done_fns.contains_key(&fnid) {
                    // log::info!("req {} fn {} done, no need to scale for", req.req_id, fnid,);
                    continue;
                }

                //已经调度
                if req.fn_node.contains_key(&fnid) {
                    continue;
                }

                if !req.parents_all_done(env, fnid) {
                    continue;
                }

                let _metric = collect_map
                    .entry(fnid)
                    .and_modify(|metric| {
                        metric.ready_2_schedule_fn_reqs.push(req.req_id);
                    })
                    .or_insert(ContainerMetric {
                        container_count: env
                            .fn_2_nodes
                            .borrow()
                            .get(&fnid)
                            .map_or_else(|| 0, |nodes| nodes.len()),
                        scheduled_fn_count: env.fn_2_nodes.borrow().get(&fnid).map_or_else(
                            || 0,
                            |nodes| {
                                nodes
                                    .iter()
                                    .map(|n| {
                                        env.node(*n).container(fnid).unwrap().req_fn_state.len()
                                    })
                                    .sum()
                            },
                        ),
                        ready_2_schedule_fn_reqs: vec![req.req_id],
                    });
            }
        }
        collect_map
    }

    // //降序排序
    // pub fn algo_get_fn_containers_busy_rank_desc(&self) -> Vec<FnId> {
    //     let mut fnid_busy_s = vec![];
    //     let fn_2_nodes = self.fn_2_nodes.borrow();
    //     for (fnid, nodes) in fn_2_nodes.iter() {
    //         let mut busy = 0.0;
    //         for &n in nodes {
    //             busy += self.node(n).fn_containers.get(fnid).unwrap().recent_handle_speed();
    //         }
    //         fnid_busy_s.push((fnid, busy));
    //     }
    //     fnid_busy_s.sort_by(|a, b| { b.1.partial_cmp(&a.1).unwrap() });
    //     fnid_busy_s
    //         .iter()
    //         .map(|a| { *a.0 })
    //         .collect()
    // }

    pub fn algo_predict_fn_on_node_work_time(
        &self,
        req: &Request,
        fnid: FnId,
        nodeid: NodeId,
        n2n_connection_count: Option<&Vec<Vec<usize>>>,
    ) -> f32 {
        let get_connection_count = |n1: NodeId, n2: NodeId| -> usize {
            if let Some(n2n_connection_count) = n2n_connection_count.as_ref() {
                if n1 == n2 {
                    return 1;
                } else if n1 > n2 {
                    n2n_connection_count[n1][n2]
                } else {
                    n2n_connection_count[n2][n1]
                }
            } else {
                return 1;
            }
        };
        let parents = self.func(fnid).parent_fns(self);
        let mut transtime: f32 = 0.0;
        for &p in &parents {
            let pdata = self.func(p).out_put_size;
            let pnode = *req.fn_node.get(&p).unwrap();
            if pnode == nodeid {
            } else {
                transtime = transtime.max(
                    pdata
                        / (self.node_get_speed_btwn(pnode, nodeid)
                            / (get_connection_count(pnode, nodeid) as f32)),
                );
            }
        }
        let computetime = (self.func(fnid).cpu * (self.node(nodeid).frame_run_count as f32))
            / self.node(nodeid).rsc_limit.cpu;

        transtime + computetime
        // let run_speed=self.node(i).fn_containers.get(&fnid).unwrap().recent_handle_speed();
    }
    // /// return None if all nodes has the fn container
    // pub fn algo_find_the_most_fast_node_for_req_fn<F>(
    //     &self,
    //     parent_fns_nodes: &Vec<(FnId, NodeId)>,
    //     node_filter: F // child_fns: &Vec<FnId>,
    // ) -> Option<NodeId>
    //     where F: Fn(&&Node) -> bool
    // {
    //     let mut from_node_speeds = vec![];

    //     //计算节点到关联fn的传输时间，取最小的
    //     fn calc_node_2_rela_fn_transtime(
    //         env: &SimEnv,
    //         cur_node: NodeId,
    //         parent_fn_node: NodeId,
    //         parent_fn_data: f32
    //     ) -> f32 {
    //         parent_fn_data / env.node_get_speed_btwn(parent_fn_node, cur_node)
    //     }
    //     for node in self.nodes.borrow().iter().filter(node_filter) {
    //         let mut time_cost = 0.0;
    //         for &(p_fn, p_node) in parent_fns_nodes {
    //             let _parent_data = self.func(p_fn).out_put_size;
    //             time_cost += calc_node_2_rela_fn_transtime(
    //                 self,
    //                 node.node_id(),
    //                 p_node,
    //                 self.func(p_fn).out_put_size
    //             );
    //         }
    //         // for child_fn in child_fns {
    //         //     speed += calc_node_2_rela_fn_transtime(self, node, *child_fn, None);
    //         // }
    //         from_node_speeds.push((node.node_id(), time_cost));
    //     }
    //     from_node_speeds.sort_by(|a, b| {
    //         let a = a.1;
    //         let b = b.1;
    //         std::cmp::PartialOrd::partial_cmp(&a, &b).unwrap()
    //     });

    //     // 取出排序完后最开始的，即最快的
    //     from_node_speeds.first().map(|node_id_speed| {
    //         assert!(node_id_speed.0 < self.node_cnt());
    //         node_id_speed.0
    //     })
    // }
    // pub fn algo_find_the_most_idle_node_for_fn(&self, fnid: FnId) -> Option<NodeId> {
    //     let env_fn_2_nodes = self.fn_2_nodes.borrow();
    //     let fn_nodes = env_fn_2_nodes.get(&fnid).unwrap();
    //     // let mut node_id = *fn_nodes.iter().next().unwrap();

    //     // for fn_node in fn_nodes {
    //     //     // 选出资源占用最小的
    //     //     if self.nodes[*fn_node].cmp_rsc(&self.nodes[node_id]).is_lt() {
    //     //         node_id = *fn_node;
    //     //     }
    //     // }

    //     // node_id
    //     let res = fn_nodes
    //         .iter()
    //         .min_by(|a, b| self.nodes.borrow()[**a].cmp_rsc_used(&self.nodes.borrow()[**b]))
    //         .map(|v| *v);

    //     res
    // }
    // pub fn algo_find_the_most_idle_node<F: FnMut(&&Node) -> bool>(
    //     &self,
    //     filter: F
    // ) -> Option<NodeId> {
    //     let res = self.nodes
    //         .borrow()
    //         .iter()
    //         .filter(filter)
    //         .min_by(|a, b| a.cmp_rsc_used(b))
    //         .map(|n| n.node_id());
    //     res
    // }
}
