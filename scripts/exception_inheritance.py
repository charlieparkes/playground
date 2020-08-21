class MyBaseException(Exception):
    pass

class MyChildException(MyBaseException):
    pass


def my_func():
    try:
        raise MyChildException("child")
    except MyBaseException:
        print("caught")
        raise


def main():
    try:
        my_func()
    except MyChildException as e:
        print(f"{e.__class__} {e}")

__main__ = main()
