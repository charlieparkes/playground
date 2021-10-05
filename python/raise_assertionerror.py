try:
    assert 1 == 2
except AssertionError as e:
    raise AssertionError(f"we did it") from e
