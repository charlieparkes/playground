def digit_adder(x):
    negative = (x < 0)
    if negative:
        x = str(x)[1:]
    converted = [str(int(i)+1) for i in str(x)]
    result = int(''.join(converted))
    return result if not negative else int(f"-{result}")

def digit_adder_no_strings(x):
    exp = 0
    reduc = x
    while reduc > 0:
        x += 1
        reduc = reduc/10

tests = (
    (0, 1),
    (99, 1010),
    (1000, 2111),
    (998, 10109),
    (-1, -2),
    (-9, -10),
)

for t in tests:
    r = digit_adder(t[0])
    print(f"{t} -> {r}")
    assert r == t[1]

# for t in tests:
#     r = digit_adder_no_strings(t[0])
#     print(f"{t} -> {r}")
#     assert r == t[1]