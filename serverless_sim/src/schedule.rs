use std::{
    collections::{BTreeSet, HashMap},
    vec,
};

use daggy::Walker;

use crate::{
    fn_dag::{FnContainer, FnContainerState, FnId},
    node::{Node, NodeId},
    request::{ReqId, Request},
    scale_executor::ScaleExecutor,
    sim_env::SimEnv,
};

pub trait Scheduler {
    /// For scaler to get some schdule info.
    /// Not needed for non-scaler scheduler.
    fn prepare_this_turn_will_schedule(&mut self, env: &SimEnv);
    /// For scaler to get some schdule info.
    /// Not needed for non-scaler scheduler.
    fn this_turn_will_schedule(&self, fnid: FnId) -> bool;
    fn schedule_some(&mut self, env: &SimEnv);
}

pub mod schedule_helper {
    use crate::{fn_dag::FnId, request::Request, sim_env::SimEnv};
    pub enum CollectTaskConfig {
        All,
        PreAllDone,
        PreAllSched,
    }

    pub fn collect_task_to_sche(
        req: &Request,
        env: &SimEnv,
        config: CollectTaskConfig,
    ) -> Vec<FnId> {
        let mut collect = vec![];
        let dag_i = req.dag_i;
        let mut dag_walker = env.dag(dag_i).new_dag_walker();
        // let mut schedule_able_fns = vec![];
        'next_fn: while let Some(fngi) = dag_walker.next(&*env.dag_inner(dag_i)) {
            let fnid = env.dag_inner(dag_i)[fngi];
            if req.fn_node.contains_key(&fnid) {
                //scheduled
                continue;
            }
            let parents = env.func(fnid).parent_fns(env);
            for p in &parents {
                match config {
                    CollectTaskConfig::PreAllDone => {
                        if !req.done_fns.contains_key(p) {
                            continue 'next_fn;
                        }
                    }
                    CollectTaskConfig::PreAllSched => {
                        if !req.fn_node.contains_key(p) {
                            continue 'next_fn;
                        }
                    }
                    CollectTaskConfig::All => {
                        // do nothing
                    }
                }
            }
            if req.fn_node.contains_key(&fnid) {
                continue;
            }
            // if
            //     env.fn_2_nodes.borrow().contains_key(&fnid) &&
            //     env.fn_running_containers_nodes(fnid).len() > 0
            {
                // parents all done schedule able
                // schedule_able_fns.push(fnid);
                collect.push(fnid);
            }
        }
        collect
    }
}

#[derive(Clone, Debug)]
struct TransPath {
    // from_node_id: NodeId,
    // to_node_id: NodeId,
    /// recv req
    req_id: ReqId,
    /// recv fn
    fn_id: FnId,
}

struct NodeTrans {
    send_paths: Vec<TransPath>,
    recv_paths: Vec<TransPath>,
}

impl NodeTrans {
    fn path_cnt(&self) -> usize {
        self.send_paths.len() + self.recv_paths.len()
    }
}

type NodeTransMap = HashMap<(NodeId, NodeId), NodeTrans>;

impl SimEnv {
    pub fn schedule_reqfn_on_node(&self, req: &mut Request, fnid: FnId, nodeid: NodeId) {
        // schedule on node
        // let new_fn_running = self.fn_new_fn_running_state(req, fnid);
        // if let Some(container) = self.nodes.borrow_mut()[nodeid].fn_containers.get_mut(&fnid) {
        //     container.req_fn_state.insert(req.req_id, new_fn_running);
        // }
        // .unwrap_or_else(|| {
        //     panic!("Node {} suppose to have fn {} container.", nodeid, fnid);
        // })
        self.node_mut(nodeid).add_task(req.req_id, fnid);
        req.fn_node.insert(fnid, nodeid);
    }

    fn sim_transfer_btwn_nodes(&self, node_a: NodeId, node_b: NodeId, transmap: &mut NodeTransMap) {
        assert_ne!(node_a, node_b);

        // 两个node之间的数据传输
        let a2b = transmap.remove(&mut (node_a, node_b)).unwrap();
        let _b2a = transmap.remove(&mut (node_b, node_a)).unwrap();
        let total_bandwith = self.node_get_speed_btwn(node_a, node_b);
        let each_path_bandwith = total_bandwith / (a2b.path_cnt() as f32);

        let updata_trans = |from: NodeId, to: NodeId, t: &TransPath| {
            let mut env_nodes = self.nodes.borrow_mut();
            let mut container = env_nodes[to]
                .container_mut(t.fn_id)
                .unwrap_or_else(|| panic!("node {} has no fn container for fn {}", to, t.fn_id));
            container.this_frame_used = true;

            let (all, recved) = container
                .req_fn_state
                .get_mut(&t.req_id)
                .unwrap()
                .data_recv
                .get_mut(&from)
                .unwrap();
            if *all < *recved {
                // 该数据已经传输完毕
                log::info!(
                    "data from {from} to {to} for req{} fn{} has been transfered",
                    t.req_id,
                    t.fn_id
                );
            } else {
                *recved += each_path_bandwith;
            }
        };

        // a，b之间单个任务的传输速度，取决于a，b间的路径数
        for t in a2b.send_paths {
            // a2b
            updata_trans(node_a, node_b, &t);
        }

        for t in a2b.recv_paths {
            updata_trans(node_b, node_a, &t);
        }
    }

    fn sim_transfers(&self) {
        // 收集所有node向其他函数发送和接收的路径数，这样每个接收函数可以知道从远程node收到多少数据，
        // 模拟传输时，一个一个路径遍历过来，
        //   两边一定有一个网速更快，那么选择慢的；然后快的那边可以把带宽分给其他的传输路径
        //
        let mut node2node_trans: NodeTransMap = HashMap::new();
        for x in 0..self.nodes.borrow().len() {
            for y in 0..self.nodes.borrow().len() {
                if x != y {
                    node2node_trans.insert(
                        (x, y),
                        NodeTrans {
                            send_paths: vec![],
                            recv_paths: vec![],
                        },
                    );
                }
            }
        }

        // go through all the fn task scheduled on node, and collect the transfer paths
        for node in self.nodes.borrow_mut().iter_mut() {
            let node_id = node.node_id();
            for (fnid, fn_container) in node.fn_containers.borrow_mut().iter_mut() {
                for (req_id, fnrun) in &mut fn_container.req_fn_state {
                    for (send_node, (all, recved)) in &mut fnrun.data_recv {
                        // 数据还没接受完才需要传输
                        if *recved < *all {
                            if *send_node == node_id {
                                // data transfer on same node can be done immediately
                                *recved = *all + 0.001;
                            } else {
                                let path = TransPath {
                                    req_id: *req_id,
                                    fn_id: *fnid,
                                };
                                // log::info!("new one path: {path:?} to node {node_id}");
                                let send_2_recv =
                                    node2node_trans.get_mut(&(*send_node, node_id)).unwrap();
                                send_2_recv.send_paths.push(path.clone());

                                let recv_2_send =
                                    node2node_trans.get_mut(&(node_id, *send_node)).unwrap();
                                recv_2_send.recv_paths.push(path.clone());
                            }
                        }
                    }
                }
            }
        }
        // go through all the transfer paths, and simulate the transfer
        let nodes_cnt = self.nodes.borrow().len();
        for x in 0..nodes_cnt {
            for y in 0..nodes_cnt {
                if x > y {
                    let connection_count = node2node_trans.len();
                    self.node_set_connection_count_between(x, y, connection_count);
                }
            }
        }
        for x in 0..nodes_cnt {
            for y in 0..nodes_cnt {
                if x > y {
                    // simu transfer between node x and y
                    self.sim_transfer_btwn_nodes(x, y, &mut node2node_trans);
                }
            }
        }
    }

    // return true means state move on
    fn sim_compute_container_starting(
        &self,
        fnid: FnId,
        fc: &mut FnContainer,
        cpu_for_one_task: f32,
    ) {
        let container_cpu_used = cpu_for_one_task.min(self.func(fnid).cold_start_container_cpu_use);
        fc.set_cpu_use_rate(cpu_for_one_task, container_cpu_used);

        fc.starting_left_frame_move_on();
    }

    fn sim_compute_container_running(
        &self,
        fnid: FnId,
        container_node_cpu: &mut f32,
        fc: &mut FnContainer,
        cpu_for_one_task: f32,
        req_fns_2_run: &BTreeSet<(ReqId, FnId)>,
    ) {
        let mut done_reqs = vec![];
        let mut calc_cnt = 0;

        // used to compute cpu use rate
        let mut container_alloced_cpu = 0.0;
        let mut container_used_cpu = 0.0;

        for (reqid, fn_running_state) in &mut fc.req_fn_state {
            if !req_fns_2_run.contains(&(fnid, *reqid)) {
                continue;
            }
            calc_cnt += 1;

            // calc process
            let used_cpu = cpu_for_one_task.min(fn_running_state.left_calc);
            fn_running_state.left_calc -= cpu_for_one_task;
            *container_node_cpu += used_cpu;

            // cpu suppose to use
            container_alloced_cpu += cpu_for_one_task;
            // cpu really used
            container_used_cpu += used_cpu;

            if fn_running_state.compute_done() {
                done_reqs.push(*reqid);
            }
        }

        //有计算，容器被使用
        if calc_cnt > 0 {
            fc.this_frame_used = true;
            // compute cpu use rate
            fc.set_cpu_use_rate(container_alloced_cpu, container_used_cpu);
        } else {
            fc.set_cpu_use_rate(1.0, 0.0);
        }

        fc.record_this_frame(self, done_reqs.len(), fc.req_fn_state.len());
        for reqid in done_reqs {
            fc.req_fn_state.remove(&reqid).unwrap();
            let mut req = self.request_mut(reqid);
            req.fn_done(self, fnid, self.current_frame());
            if req.is_done(self) {
                log::info!("req {} done", reqid);
                drop(req);
                self.on_request_done(reqid);
            }
        }
    }

    fn sim_compute_collect_compute_data(
        &self,
        n: &mut Node,
    ) -> Option<(BTreeSet<(ReqId, FnId)>, usize, f32)> {
        let mut req_fns_2_run = BTreeSet::new();

        // collect run fn count, alloc cpu resource equally
        let starting_container_cnt = n
            .fn_containers
            .borrow()
            .iter()
            .filter(|(_, fc)| match fc.state() {
                FnContainerState::Starting { .. } => true,
                _ => false,
            })
            .count();

        for (&fnid, fc) in n.fn_containers.borrow().iter() {
            if let FnContainerState::Running { .. } = fc.state() {
                for (&req_id, fn_running_state) in &fc.req_fn_state {
                    if fn_running_state.data_recv_done() && n.left_mem() > self.func(fnid).mem {
                        *n.mem.borrow_mut() += self.func(fnid).mem;
                        req_fns_2_run.insert((fnid, req_id));
                    }
                }
            }
        }

        // n.mem = used_mem;
        if req_fns_2_run.len() == 0 && starting_container_cnt == 0 {
            None
        } else {
            // 计算任务数，每个任务平分计算量
            let each_fn_cpu =
                n.rsc_limit.cpu / ((req_fns_2_run.len() + starting_container_cnt) as f32);
            n.frame_run_count = req_fns_2_run.len() + starting_container_cnt;
            Some((req_fns_2_run, starting_container_cnt, each_fn_cpu))
        }
    }

    fn sim_load_container(&self) {
        let mut nodes_mut = self.nodes_mut();
        for n in nodes_mut.iter_mut() {
            n.load_container(&self);
        }
    }

    fn sim_computes(&self) {
        for n in self.nodes.borrow_mut().iter_mut() {
            // collect the done receive data tasks
            if let Some((req_fns_2_run, _starting_container_cnt, cpu_for_one_task)) =
                self.sim_compute_collect_compute_data(n)
            {
                for (fnid, fc) in n.fn_containers.borrow_mut().iter_mut() {
                    match fc.state_mut() {
                        FnContainerState::Starting { .. } => {
                            self.sim_compute_container_starting(*fnid, fc, cpu_for_one_task);
                            if let FnContainerState::Running = fc.state() {
                                // starting -> running
                                *n.mem.borrow_mut() -=
                                    self.func(*fnid).cold_start_container_mem_use;
                                *n.mem.borrow_mut() += self.func(*fnid).container_mem();
                            }
                        }
                        _ => {}
                    }
                }
                for (fnid, fc) in n.fn_containers.borrow_mut().iter_mut() {
                    match fc.state_mut() {
                        FnContainerState::Running => self.sim_compute_container_running(
                            *fnid,
                            &mut n.cpu,
                            fc,
                            cpu_for_one_task,
                            &req_fns_2_run,
                        ),
                        _ => {}
                    }
                }
            } else {
                for (fnid, fc) in n.fn_containers.borrow_mut().iter_mut() {
                    match fc.state_mut() {
                        FnContainerState::Starting { .. } => {
                            panic!("should not be starting");
                        }
                        FnContainerState::Running => self.sim_compute_container_running(
                            *fnid,
                            &mut n.cpu,
                            fc,
                            0.0,
                            &BTreeSet::new(),
                        ),
                    }
                }
            }
        }
    }

    pub fn sim_run(&self) {
        log::info!("sim run");

        self.sim_load_container();
        self.sim_transfers();
        self.sim_computes();
    }
    // pub fn schedule_fn(&self) {
    //     self.try_put_fn();
    //     self.sim_run();
    // }
}
