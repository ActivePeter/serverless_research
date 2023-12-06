import requests
import numpy as np
import json
from pprint import pprint
from gym.spaces import Box
from numpy import uint8
import serverless_sim
import random

OBSERVATION_N = 80

SIM_URL = "http://localhost:3000/"

ACTION_SPACE_LOW = 0
ACTION_SPACE_HIGH = 1


class ProxyEnv2:

    url = SIM_URL

    multi_agent_cnt = 1
    multi_agent_obs = np.zeros((10,))

    action_space = type('', (object,), {
        "low": [ACTION_SPACE_LOW],
        "high": [ACTION_SPACE_HIGH],
        "shape": [1],
        "n": 12})()

    spec = type('', (object,), {"id": "proxy_env"})()

    observation_space = Box(-1, np.Inf,
                            (1, OBSERVATION_N, OBSERVATION_N), np.float32)

    obs = np.zeros((1, OBSERVATION_N, OBSERVATION_N))

    step_cnt = 0

    # according to network config
    # pub struct Config {
    #     /// for the different algos, should use the same seed
    #     pub rand_seed: String,
    #     /// low middle high
    #     pub request_freq: String,
    #     /// dag type: single, chain, dag, mix
    #     pub dag_type: String,
    #     /// cold start: high, low, mix
    #     pub cold_start: String,
    #     /// cpu, data, mix
    #     pub fn_type: String,
    #     /// each stage control algorithm settings
    #     pub es: ESConfig,
    # }
    config = {
        # /// "ai", "lass", "hpa", "es"
        "rand_seed": "",
        "request_freq": "low",
        "dag_type": "single",
        "cold_start": "high",
        "fn_type": "cpu",
        "no_log": False,
        # "app_types":[],
        # // optional
        "es": {
            # // ai, lass, hpa
            "up": "ai",
            # // no, ai, rule
            "down": "ai",
            # // rule,ai,faasflow
            "sche": "ai",
            # direct, smooth_30, smooth_100
            "down_smooth": "",
            # sac ppo mat
            "ai_type": "",
        },
    }

    reset_cnt = 0

    def typekey(self):
        if "es" in self.config:
            return self.config["plan"]+"_"+self.config["es"]["up"]+"_"+self.config["es"]["down"]+"_"+self.config["es"]["sche"]
        return self.config["plan"]

    def _rule_request_freq(self):
        # print("request_freq",self.config["request_freq"])
        assert self.config["request_freq"] in ["middle", "low", "high"]
        return self

    def _rule_dag_type(self):
        assert self.config["dag_type"] in ["single", "chain", "dag", "mix"]
        return self

    def _rule_cold_start(self):
        assert self.config["cold_start"] in ["high"]
        return self

    def _rule_fn_type(self):
        assert self.config["fn_type"] in ["cpu", "data"]
        return self

    def _rule_es(self):
        allowed_up = ["ai", "lass", "fnsche", "hpa", "faasflow", "no"]
        allowed_down = ["ai", "lass", "fnsche", "hpa", "faasflow", "no"]
        allowed_sche = ["rule", "fnsche", "faasflow", "rule_prewarm_succ",
                        "round_robin", "random", "load_least", "gofs", "pass"]
        up_down_must_same = ["ai", "lass", "hpa", "faasflow", "fnsche"]
        non_scaler_sche = ["fnsche", "faasflow"]

        config = self.config

        assert config["es"]["up"] in allowed_up
        assert config["es"]["down"] in allowed_down
        assert config["es"]["sche"] in allowed_sche
        assert config["es"]["down_smooth"] in [
            "direct", "smooth_30", "smooth_100"]
        if config["es"]["up"] in ["ai"]:
            assert config["es"]["ai_type"] in ["sac", "ppo", "mat", "ppo_hrf"]

        if config["es"]["up"] in up_down_must_same:
            assert config["es"]["up"] == config["es"]["down"]
        if config["es"]["sche"] in non_scaler_sche:
            assert "no" == config["es"]["up"]

    def __init__(self, do_change_seed, config, multi_agent=False):
        self.config = config
        self.begin_seed = config["rand_seed"]
        self.do_change_seed = do_change_seed
        self._rule_cold_start() \
            ._rule_dag_type() \
            ._rule_fn_type() \
            ._rule_request_freq() \
            ._rule_es()
        self.multi_agent = multi_agent

    def __request(self, api, data=None):
        # print("request: ",self.url+api,", data: ",data)
        if data is None:
            return requests.post(self.url+api)
        else:
            return requests.post(self.url+api, json=data)

    def reset(self):
        self.reset_cnt += 1

        def generate_random_str(randomlength=16):
            """
            生成一个指定长度的随机字符串
            """
            random_str = ''
            base_str = 'ABCDEFGHIGKLMNOPQRSTUVWXYZabcdefghigklmnopqrstuvwxyz0123456789'
            length = len(base_str) - 1
            for i in range(randomlength):
                random_str += base_str[random.randint(0, length)]

            return random_str
        if self.do_change_seed:
            if self.reset_cnt % 3 == 0:
                self.config["rand_seed"] = self.begin_seed
                self.config["no_log"] = False
            else:
                self.config["rand_seed"] = generate_random_str()
                self.config["no_log"] = True
        if self.reset_cnt > 20 and "no_perform_cost_rate_score" in self.config["es"]:
            self.config["es"].pop("no_perform_cost_rate_score")
        if self.reset_cnt > 7 and "fit_hpa" in self.config["es"]:
            self.config["es"].pop("fit_hpa")

        self.step_cnt = 0

        self.__request("reset", self.config)
        if self.multi_agent:
            return [self.multi_agent_obs for _ in range(self.n_agents)]
        else:
            return self.obs
        # serverless_sim.fn_reset(json.dumps(self.config))

    def step(self, action):
        if self.multi_agent:
            return self.multi_agent_step(action)
        else:
            return self.single_agent_step(action)

    def multi_agent_step(self, action):
        res = self.__request("step", {"action": action, "config": self.config})
        print("res", res)
        res = res.json()

    def single_agent_step(self, action):
        print("single_agent_step", action)
        # res=serverless_sim.fn_step(json.dumps({"action":action,"config":self.config}))
        res = self.__request("step", {"action": action, "config": self.config})
        print("res", res)
        res = res.json()

        # res=json.loads(res)

        # print("res: ",res)
        # print("res: ",res.status_code,res.text)
        # res=res.json()
        # return res["observation"],res["reward"],res["done"],res["info"]
        state_arr = json.loads(res["state"])
        print("state arr len", len(state_arr), "current step",
              self.step_cnt, "reset_cnt", self.reset_cnt)
        # state_arr
        # for c in state_str:
        #     state_arr.append(ord(c))
        if len(state_arr) < OBSERVATION_N*OBSERVATION_N:
            for i in range(OBSERVATION_N*OBSERVATION_N-len(state_arr)):
                state_arr.append(0)
        # elif len(state_arr) > OBSERVATION_N*OBSERVATION_N:
        #     print("Warning: state length is greater than OBSERVATION_N, truncating, info may be lost",len(state_arr))
        #     state_arr=state_arr[:OBSERVATION_N*OBSERVATION_N]
        state_mat = np.reshape(state_arr, self.obs.shape)
        self.step_cnt += 1
        # if self.step_cnt==10000:
        #     res["stop"]=True
        return state_mat, res["score"], res["stop"], res["info"]

# class EnvForAI:
#     env=ProxyEnv2({
#         "rand_seed":"hello",
#         "request_freq":"middle",
#         "dag_type":"single",
#         "cold_start":"high",
#         "fn_type":"cpu",
#         "es": {
#             "up":"fnsche",
#             "down":"fnsche",
#             "sche":"fnsche",
#         },
#     })
#     def __init__(self):
#         self.observation_space=self.env.observation_space
#         self.action_space=self.env.action_space
#         self.spec=self.env.spec

#     def step(self,action):
#         return self.env.step(action)
#     def reset(self):
#         self.env.config["rand_seed"]=generate_random_str()
#         return self.env.reset()
