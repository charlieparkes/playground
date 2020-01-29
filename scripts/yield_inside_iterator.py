from lpipe.utils import batch

x = [1, 2, 3, 4, 5]

def generator(l):
    for b in batch(l, 2):
        yield b

for b in generator(x):
    print(b)
