use std::{collections::HashMap, u128::MAX};

use daggy::{
    petgraph::visit::{EdgeRef, IntoEdgeReferences},
    EdgeIndex, Walker,
};
use rand::{thread_rng, Rng};

use crate::{
    fn_dag::{DagId, FnId},
    node::NodeId,
    request::{ReqId, Request},
    scale_executor::{ScaleExecutor, ScaleOption},
    schedule::{
        schedule_helper::{collect_task_to_sche, CollectTaskConfig},
        Scheduler,
    },
    sim_env::SimEnv,
    util,
};

struct RequestSchedulePlan {
    fn_nodes: HashMap<FnId, NodeId>,
}

pub struct TimeScheduler {
    // dag_fn_prorities: HashMap<DagId, Vec<(FnId, f32)>>,
    // dag_fn_prorities_: HashMap<DagId, HashMap<FnId, f32>>,
    // 记录task的触发时间（入度为0的函数是请求到达的时间，否则是前驱函数完成的时间）
    fn_trigger_time: HashMap<(ReqId, FnId), usize>,
    fn_wait_time: HashMap<(ReqId, FnId), usize>,
}

// 基于时间感知的函数调度算法
impl TimeScheduler {
    pub fn new() -> Self {
        Self {
            fn_trigger_time: HashMap::new(),
            fn_wait_time: HashMap::new(),
        }
    }

    // fn prepare_priority_for_dag(&mut self, req: &mut Request, env: &SimEnv) {
    //     let dag = env.dag(req.dag_i);

    //     //计算函数的优先级P：
    //     if !self.dag_fn_prorities.contains_key(&dag.dag_i) {
    //         //map存储每个函数的优先级
    //         let mut map: HashMap<usize, f32> = HashMap::new();
    //         let mut walker = dag.new_dag_walker();
    //         // P = 函数的资源消耗量×(启动时间+函数执行时间)
    //         while let Some(func_g_i) = walker.next(&dag.dag_inner) {
    //             let fnid = dag.dag_inner[func_g_i];
    //             let t_exe = func.cpu / node.rsc_limit.cpu;

    //             let consume_mem = func.mem;
    //             let p = consume_mem * (t_exe + func.cold_start_time);

    //             map.insert(fnid, p);
    //         }
    //         let mut prio_order = map.into_iter().collect::<Vec<_>>();
    //         // Sort the vector by the value in the second element of the tuple.
    //         // 升序排序优先级
    //         prio_order.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
    //         self.dag_fn_prorities.insert(dag.dag_i, prio_order);
    //     }
    // }

    // fn select_node_for_fn(
    //     &self,
    //     // 为f分配的node
    //     schedule_to_map: &mut HashMap<FnId, NodeId>,
    //     schedule_to: &mut Vec<(FnId, NodeId)>,
    //     func_id: FnId,
    //     req: &mut Request,
    //     env: &SimEnv,
    // ) {
    //     let func = env.func(func_id);
    //     let nodes = env.nodes.borrow();

    //     for nodeid in 0..nodes.len() {
    //         let node = env.node(nodeid);
    //         let limit_cpu = node.rsc_limit.cpu;
    //         // 将满足资源需求的node分配给func
    //         if (func.cpu < limit_cpu) {
    //             schedule_to_map.insert(func_id, nodeid);
    //             schedule_to.push((func_id, nodeid));
    //             // 更新节点的资源
    //             nodes.rsc_limit.cpu = limit_cpu - func.cpu;
    //         }
    //     }
    // }
    // //实现Time算法
    // fn schedule_for_one_req(&mut self, req: &mut Request, env: &SimEnv) {
    //     self.prepare_priority_for_dag(req, env);

    //     let dag = env.dag(req.dag_i);
    //     let mut schedule_to = Vec::<(FnId, NodeId)>::new();
    //     let mut schedule_to_map = HashMap::<FnId, NodeId>::new();

    //     // 获取优先级
    //     let prio_order = self.dag_fn_prorities.get(&dag.dag_i).unwrap();

    //     log::info!("prio order: {:?}", prio_order);
    //     for (func_id, _fun_prio) in prio_order {
    //         self.select_node_for_fn(&mut schedule_to_map, &mut schedule_to, *func_id, req, env);
    //     }

    //     for (fnid, nodeid) in schedule_to {
    //         // if env.node(nodeid).fn_containers.get(&fnid).is_none() {
    //         //     if env
    //         //         .scale_executor
    //         //         .borrow_mut()
    //         //         .scale_up_fn_to_nodes(env, fnid, &vec![nodeid])
    //         //         == 0
    //         //     {
    //         //         panic!("can't fail");
    //         //     }
    //         // }
    //         // if env.node(fn_node).mem_enough_for_container(&env.func(fnid)) {
    //         env.schedule_reqfn_on_node(req, fnid, nodeid);
    //     }
    // }
}

impl Scheduler for TimeScheduler {
    fn schedule_some(&mut self, env: &SimEnv) {
        // 1、collect前驱已经执行完，当前函数未被调度的
        let mut tasks: Vec<(ReqId, FnId)> = vec![];
        for (reqid, r) in env.requests.borrow().iter() {
            let ts = collect_task_to_sche(&r, env, CollectTaskConfig::PreAllDone);
            tasks.append(
                &mut ts
                    .into_iter()
                    .map(|fnid| (*reqid, fnid))
                    .collect::<Vec<_>>(),
            )
        }

        // 2、处理超时的task，增加优先级
        // 当使用 FuncSched 调度时，如果优先级P[k]较高的函数请求不断到达，可能会导致有些函数的饥饿.这是因为整个服务器无感知计算平台的资源可能会
        // 一直分配给不断到达的函数高优先级请求，而其他函数请求一直处于待执行状态.
        // 对于这种状况，FuncSched 会维护一个更高的优先级队列Qstarve ，并设置一个可调节的阈值 StarveThreshold.
        // 当一 个 函 数 请 求 的 等 待 时 间F[k]s − F[k]a > StarveThreshold 时，该请求将会被调入 .
        // 在Qstarve中，所有函数请求按照等待时间从大到小排序，
        // 队头函数请求将被 FuncSched 调度器最先执行. 当Qstarve为空时 ，剩余函数请求再按照函数请求优先级P[k]执行. 依靠这一机制，
        // FuncSched 能够避免低优先级函数请求的饥饿情况

        for (reqid, fnid) in tasks {
            let req = env.request(reqid);
            let func = env.func(fnid);
            let func_pres_id = func.parent_fns(env);
            // 计算函数的触发时间
            if (!self.fn_trigger_time.contains_key(&(reqid, fnid))) {
                // 没有前驱函数
                if func_pres_id.len() == 0 {
                    self.fn_trigger_time.insert((reqid, fnid), req.begin_frame);
                } else {
                    // 最晚完成的前驱函数的结束时间
                    //   Request has pub done_fns: HashMap<FnId, usize>,
                    let mut fun_pres_time = HashMap::<FnId, usize>::new();
                    let mut pres_end_time;
                    for id in func_pres_id {
                        let Some(&func_pre_time) = req.done_fns.get(&id);
                        if func_pre_time > pres_end_time {
                            pres_end_time = func_pre_time;
                        }
                        fun_pres_time.insert(id, func_pre_time);
                    }
                    self.fn_trigger_time.insert((reqid, fnid), pres_end_time);
                }
            }
            // 计算函数的等待时间
            let mut wait_time =
                *env.current_frame.borrow() - self.fn_trigger_time.get(&(reqid, fnid)).unwrap();
            self.fn_wait_time.insert((reqid, fnid), wait_time);
        }

        // 3、优先级排序未被调度的
        tasks.sort_by(|(reqid1, fnid1), (reqid2, fnid2)| {});

        // 4、为每一个task选择node 简单策略：Least Connections
    }

    fn prepare_this_turn_will_schedule(&mut self, env: &SimEnv) {}
    fn this_turn_will_schedule(&self, fnid: FnId) -> bool {
        false
    }
}
