import yaml


def load_conf():
    with open("../config.yaml") as f:
        return yaml.safe_load(f)


class TestTask:
    conf = load_conf()

    def test_flink(self):
        print(self.conf["flink"])
