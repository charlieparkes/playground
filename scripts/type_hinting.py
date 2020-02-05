import inspect
from typing import cast, get_type_hints, Optional


def _merge_similar(functions, iter):
    output = {}
    for f in functions:
        i = iter(f)
        for k, v in i.items():
            if k in output:
                try:
                    assert v == output[k]
                except AssertionError as e:
                    raise TypeError(
                        f"Incompatible functions {functions}: {k} represented as both {v} and {output[k]}"
                    ) from e
            else:
                output[k] = v
    return output


def merge_signatures(functions):
    return _merge_similar(functions, lambda f: inspect.signature(f).parameters)


def merge_type_hints(functions):
    return _merge_similar(functions, lambda f: get_type_hints(f))


def get_defaults(signature):
    return {
        k: v.default
        for k, v in signature.items()
        if v.default is not inspect.Parameter.empty
    }


def validate(functions, params):
    signature = merge_signatures(functions)
    defaults = get_defaults(signature)
    hints = merge_type_hints(functions)

    validated = {}
    for k, v in signature.items():
        if k in params:
            p = params[k]
            if k in hints:
                t = hints[k]
                try:
                    assert isinstance(p, t)
                except AssertionError as e:
                    raise TypeError(f"Type of {k} should be {t} not {type(p)}.") from e
                validated[k] = p
            else:
                validated[k] = p
        elif not k in params and k not in ("kwargs", "args"):
            try:
                assert k in defaults
            except AssertionError as e:
                raise TypeError(f"{functions} missing required argument: '{k}'") from e

    return validated


def test(wiz, foo: str, bar: str = "asdf", **kwargs):
    print(f"test(wiz={wiz}, foo={foo}, bar={bar})")


def test2(a: int, foo: str, c, **kwargs):
    print(f"test2(a={a}, foo={foo}, c={c})")


print("--- Test 1 ---")
params = {
    "wiz": "a",
    "foo": "b",
}
params = validate([test], params)
test(**params)


print("\n--- Test 2 ---")
params = merge_signatures([test, test2])
print(params)
test(**params)
test2(**params)


print("\n--- Test 3 ---")
hints = merge_type_hints([test, test2])
print(hints)


print("\n--- Test 4 ---")
params = {
    "wiz": "a",
    "foo": "b",
    "a": 1,
    "c": True,
}
functions = [test, test2]
params = validate(functions, params)
print(f"params: {params}")
test(**params)
test2(**params)
