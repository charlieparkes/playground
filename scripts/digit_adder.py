def digit_adder(x):
    negative = (x < 0)
    if negative:
        x = str(x)[1:]
    converted = [str(int(i)+1) for i in str(x)]
    result = int(''.join(converted))
    return result if not negative else int(f"-{result}")

def digit_adder_no_str(x):
    nums = []
    reduc = abs(x)
    while reduc >= 0:
        nums.append((reduc%10) + 1)
        reduc = reduc // 10
        if reduc == 0:
            break
    new_x = 0
    for n in nums[::-1]:
        new_x *= (10 if n < 10 else 100)
        new_x += n
    return new_x if not (x < 0) else -1 * new_x

tests = (
    (0, 1),
    (99, 1010),
    (1000, 2111),
    (998, 10109),
    (-1, -2),
    (-9, -10),
)

print("Testing digit_adder")
print("-------------------")
for t in tests:
    r = digit_adder(t[0])
    print(f"{t} -> {r}")
    assert r == t[1]

print("\n\nTesting digit_adder_no_str")
print("--------------------------")
for t in tests:
    r = digit_adder_no_str(t[0])
    print(f"{t} -> {r}")
    assert r == t[1]
