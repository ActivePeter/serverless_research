use std::{
    cell::{Ref, RefMut},
    collections::{HashMap, HashSet},
};

use daggy::petgraph::visit::Topo;

use crate::{
    fn_dag::{DagId, FnId},
    node::NodeId,
    sim_env::SimEnv,
    REQUEST_GEN_FRAME_INTERVAL,
};

pub type ReqId = usize;

// pub struct RequestPlan {
//     /// 请求id
//     pub req_id: ReqId,

//     /// 对应请求处理的应用
//     pub dag_i: usize,

//     /// 第一次walk是用来扩容，后面的walk是用来扩容
//     pub fn_dag_walker: Topo<NodeIndex, <Dag<Func, f32> as Visitable>::Map>,

//     /// 函数节点被调度到的机器节点
//     pub fn_node: HashMap<FnId, NodeId>,
// }

pub struct Request {
    /// 请求id
    pub req_id: ReqId,

    /// 对应请求处理的应用
    pub dag_i: DagId,

    /// 函数节点被调度到的机器节点
    pub fn_node: HashMap<FnId, NodeId>,

    ///完成执行的函数节点，时间
    pub done_fns: HashMap<FnId, usize>,

    pub cur_frame_done: HashSet<FnId>,
    //请求到达的时刻
    pub begin_frame: usize,

    pub end_frame: usize,

    // fn_dag_walker: Topo<NodeIndex, <FnDagInner as Visitable>::Map>,

    // current_fn: Option<(FnId, NodeIndex)>,
    pub walk_cnt: usize,

    // fnid-(predict_time, scheduled_prev_fns_cnt, prev_fns_cnt)
    pub fn_predict_prevs_done_time: HashMap<FnId, (f32, usize, usize)>,
}

impl Request {
    // pub fn new_from_plan(env: &SimEnv, plan: RequestPlan) -> Self {
    //     Self {
    //         /// 请求id
    //         req_id: plan.req_id,
    //         dag_i: plan.dag_i,
    //         fn_node: plan.fn_node,
    //         done_fns: HashSet::new(),
    //     }
    // }
    pub fn new(env: &SimEnv, dag_i: DagId, begin_frame: usize) -> Self {
        let new = Self {
            req_id: env.req_alloc_req_id(),
            dag_i,
            fn_node: HashMap::new(),
            done_fns: HashMap::new(),
            // fn_dag_walker: Topo::new(&env.dags.borrow()[dag_i].dag),
            // current_fn: None,
            begin_frame,
            end_frame: 0,
            cur_frame_done: HashSet::new(),
            walk_cnt: 0,
            fn_predict_prevs_done_time: HashMap::new(),
        };
        // new.prepare_next_fn_2_bind_node(&env.dags.borrow()[dag_i].dag);
        // {
        //     log::info!("req{} dag{dag_i} has following fns", new.req_id);
        //     print!("   ");
        //     new.print_fns(env);
        // }
        new
    }

    pub fn parents_all_done(&self, env: &SimEnv, fnid: FnId) -> bool {
        let ps = env.func(fnid).parent_fns(env);
        for p in &ps {
            if !self.done_fns.contains_key(p) {
                return false;
            }
        }
        true
    }

    pub fn print_fns(&self, env: &SimEnv) {
        let dag_i = self.dag_i;
        let mut iter = Topo::new(&env.dags.borrow()[dag_i].dag_inner);

        while let Some(next) = iter.next(&env.dags.borrow()[dag_i].dag_inner) {
            let fnid = &env.dags.borrow()[dag_i].dag_inner[next];
            print!("{} ", fnid);
        }
        println!();
    }

    pub fn get_fn_node(&self, fnid: FnId) -> Option<NodeId> {
        self.fn_node.get(&fnid).map(|v| *v)
    }

    // pub fn prepare_next_fn_2_bind_node(&mut self, g: &FnDagInner) -> Option<(FnId, NodeIndex)> {
    //     let res = self.fn_dag_walker.next(g);
    //     let res = res.map(|i| (g[i], i));
    //     self.current_fn = res;
    //     self.walk_cnt += 1;
    //     res
    // }

    // #[allow(dead_code)]
    // pub fn fn_2_bind_node(&self) -> Option<(FnId, NodeIndex)> {
    //     self.current_fn
    // }

    pub fn fn_done(&mut self, env: &SimEnv, fnid: FnId, current_frame: usize) {
        // log::info!("request {} fn {} done", self.req_id, fnid);
        self.done_fns.insert(fnid, current_frame);
        self.cur_frame_done.insert(fnid);
        if self.is_done(env) {
            self.end_frame = current_frame;
        }
    }
    pub fn fn_count(&self, env: &SimEnv) -> usize {
        env.dags.borrow()[self.dag_i].dag_inner.node_count()
    }
    pub fn is_done(&self, env: &SimEnv) -> bool {
        self.done_fns.len() == self.fn_count(env)
    }
}

impl SimEnv {
    pub fn req_alloc_req_id(&self) -> ReqId {
        let env = self;
        let ret = *env.req_next_id.borrow();
        *env.req_next_id.borrow_mut() += 1;
        ret
    }

    pub fn req_sim_gen_requests(&self) {
        let env = self;
        if *env.current_frame.borrow() % REQUEST_GEN_FRAME_INTERVAL == 0 {
            let scale = if env.config.dag_type_dag() {
                if env.config.request_freq_high() {
                    30
                } else if env.config.request_freq_middle() {
                    20
                } else {
                    10
                }
            } else {
                if env.config.request_freq_high() {
                    120
                } else if env.config.request_freq_middle() {
                    75
                } else {
                    30
                }
            };

            let req_cnt = env.env_rand_i(scale, scale * env.dags.borrow().len());

            for _ in 0..req_cnt {
                let dag_i = env.env_rand_i(0, env.dags.borrow().len() - 1);
                let request = Request::new(env, dag_i, *env.current_frame.borrow());
                let req_id = request.req_id;
                env.requests.borrow_mut().insert(req_id, request);
            }

            log::info!("Gen requests {req_cnt} at frame {}", env.current_frame());
        }
    }

    pub fn on_request_done(&self, req_id: ReqId) {
        let req = self.requests.borrow_mut().remove(&req_id).unwrap();
        self.metric.borrow_mut().add_done_request();
        self.done_requests.borrow_mut().push(req);
    }

    pub fn request<'a>(&'a self, i: ReqId) -> Ref<'a, Request> {
        let b = self.requests.borrow();

        Ref::map(b, |map| {
            map.get(&i)
                .unwrap_or_else(|| panic!("request {} not found", i))
        })
    }

    pub fn request_mut<'a>(&'a self, i: ReqId) -> RefMut<'a, Request> {
        let b = self.requests.borrow_mut();

        RefMut::map(b, |map| {
            map.get_mut(&i)
                .unwrap_or_else(|| panic!("request {} not found", i))
        })
    }
}
