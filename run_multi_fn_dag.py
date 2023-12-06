from proxy_env2 import ProxyEnv2
import threading
dag_types = ["dag"]


class Task:
    def algo(self, up: str, down: str, sche: str):
        self.env = ProxyEnv2(False, {
            "rand_seed": "hello",
            "request_freq": "high",
            "dag_type": "dag",
            "cold_start": "high",
            "fn_type": "cpu",
            "no_log": False,
            "app_types": [],
            "es": {
                "up": up,
                "down": down,
                "sche": sche,
                "down_smooth": "direct",
            },
        })
        return self

    def config(self, config_cb):
        config_cb(self.env.config)
        return self

    def run(self):
        self.env.reset()

        state, score, stop, info = self.env.step(1)
        print(state, score, stop, info)
        self.env.reset()
        return self


algos = [
    ["hpa", "hpa", "rule"],
    ["no", "no", "pass"],
    ["lass", "lass", "rule"],
    ["no", "no", "faasflow"],
    ["no", "no", "fnsche"],
]

ts = []

for dag_type in dag_types:
    for algo in algos:
        def cb(config):
            config["dag_type"] = dag_type

        def task():
            Task() \
                .algo(algo[0], algo[1], algo[2]) \
                .config(cb) \
                .run()
        t = threading.Thread(target=task, args=())
        t.start()
        ts.append(t)

for t in ts:
    t.join()
