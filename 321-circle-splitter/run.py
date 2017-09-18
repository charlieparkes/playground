#! /usr/bin/env python3

"""
Challenge #321 [Hard] Circle Splitter

You've got a square in 2D space with axis values between 0-1 which is filled
with points. Can you place a circle within the square such that exactly half
of the points in the square lie within the circle?

    * Circle may touch the edge of the square but may not pass outside.
    * Points on the edge of the circle count as inside.
    * There will always be an even number of points.
    * You must indicate if it is unsolvable.
"""

import os
import time
from decimal import *
from itertools import combinations

from circle_splitter import CircleSplitter, Point


test_input = os.listdir('test_input')

for filename in test_input:
    points = []
    with open('test_input/'+filename, 'r') as f:
        print('Test {} '.format(filename), end='')
        for line in f:
            l = line.split(' ')
            if len(l) is 2:
                x = Decimal(l[0])
                y = Decimal(l[1])

                p = Point(x, y)

                if x < 0 or x > 1 or y < 0 or y > 1:
                    print('{} is out of bounds'.format(p))
                    continue

                points.append(p)

    start = time.time()
    cs = CircleSplitter(points)
    solutions = cs.calculate()
    total = time.time() - start

    if solutions:
        print('solution(s):')
        for circle in solutions:
            print('\t({}, {}) r{}'.format(
                round(circle.x, 8),
                round(circle.y, 8),
                round(circle.radius, 8))
            )
    else:
        print('No solutions found.')

    print('\tRan in {}'.format(round(total, 4)), end='\n\n')

