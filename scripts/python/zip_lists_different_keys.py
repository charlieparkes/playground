x = [1, 2, 3, 4]
y = ["a", "b", "c", "d"]

"""
result = [
    {"x": 1, "y": 2},
    ...
]
"""

z1 = dict(zip(x, y))
z2 = [{"x": x, "y": y} for x, y in z1.items()]

print(x)
print(y)
print(z1)
print(z2)
