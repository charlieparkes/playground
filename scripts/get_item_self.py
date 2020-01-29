class Test:
    def __init__(self):
        self.x = {"a": "b"}

    def __getitem__(self, key):
        return self.x[key]

    def test_self_get(self, key):
        return self[key]


foo = Test()
assert foo.x["a"] == "b"
assert foo["a"] == "b"
assert foo.test_self_get("a") == "b"
