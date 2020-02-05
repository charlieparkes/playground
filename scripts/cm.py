from functools import wraps


def push_context(context={}):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # https://docs.sentry.io/enriching-error-data/scopes/?platform=python#local-scopes
            print(args)
            print(kwargs)
            return func(*args, **kwargs)

        return wrapper

    return decorator


@test_decorator(foo="bar")
def test(a=1):
    print(a)


test(a=3)
