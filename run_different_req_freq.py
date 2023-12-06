req_freqs=["low","middle","high"]

import threading
from proxy_env2 import ProxyEnv2

class Task: 
    def algo(self,up:str,down:str,sche:str):
        self.env=ProxyEnv2(False,{
            "rand_seed":"hello",
            "request_freq":"middle",
            "dag_type":"single",
            "cold_start":"high",
            "no_log":False,
            "fn_type":"cpu",
            "es": {
                "up":up,
                "down":down,
                "sche":sche,
                "down_smooth":"direct",
            },    
        })
        return self
        
    def config(self,config_cb):
        config_cb(self.env.config)
        return self
        
    def run(self):
        self.env.reset()
        
        state,score,stop,info=self.env.step(1)
        print(state,score,stop,info)
        self.env.reset()
        return self

algos=[
    ["hpa","hpa","rule"],
    ["no","no","pass"],
    # ["lass","lass","rule"],
    # ["fnsche","fnsche","fnsche"],
    ["no","no","faasflow"],
]

ts=[]

for req_freq in req_freqs:    
    for algo in algos:
        def cb(config):
            config["request_freq"]=req_freq
        def task():
            Task() \
                .algo(algo[0],algo[1],algo[2]) \
                .config(cb) \
                .run()
        t = threading.Thread(target=task, args=())
        t.start()
        ts.append(t)

for t in ts:
    t.join()

    