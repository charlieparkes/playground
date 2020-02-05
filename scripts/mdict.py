def get_nested(d, keys):
    """Given a dictionary, fetch a key nested several levels deep."""
    head = d
    for k in keys:
        head = head.get(k, {})
        if not head:
            return head
    return head


def set_nested(d, keys, value):
    for key in keys[:-1]:
        d = d.setdefault(key, {})
    d[keys[-1]] = value


my_dict = {}
keys = ["a", "b", "c", "wiz", "bang"]

set_nested(my_dict, keys, "test1")
print(my_dict)

val = get_nested(my_dict, keys)
print(val)
assert val == "test1"


set_nested(my_dict, keys, "test2")
print(my_dict)

val = get_nested(my_dict, keys)
print(val)
assert val == "test2"
