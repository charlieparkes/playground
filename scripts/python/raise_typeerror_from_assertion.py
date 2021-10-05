try:
    assert True == False
except AssertionError as e:
    raise TypeError("asdf") from e
