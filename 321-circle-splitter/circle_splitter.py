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

from decimal import *
from itertools import combinations
from math import sqrt, pow


class LinesDoNotIntersect(Exception):
    pass


class Point(object):
    def __init__(self, x, y):
        self.x = Decimal(x)
        self.y = Decimal(y)

    def __repr__(self):
        return u'({},{})'.format(
            round(self.x, 8),
            round(self.y, 8)
        )

    def distance_from(self, p):
        return Decimal(sqrt(pow(self.x - p.x, 2) + pow(self.y - p.y, 2)))


class Line(object):
    def __init__(self, m, b, inf=False):
        self.m = Decimal(m)
        self.b = Decimal(b)
        self.inf = inf

    def intersects_at(self, l):
        if self.m == l.m:
            raise LinesDoNotIntersect()
        if self.b == l.b:
            return Point(self.b, 0)

        if self.inf:
            return Point(self.b, (l.m * self.b) + l.b)
        elif l.inf:
            return Point(l.b, (self.m * l.b) + self.b)

        x = (l.b - self.b) / (self.m - l.m)
        y = (self.m * x) + self.b
        return Point(x, y)

    @staticmethod
    def bisect(p1, p2):
        num = (p2.y - p1.y)
        denom = (p2.x - p1.x)
        midpoint = Point((p1.x + p2.x) / 2, (p1.y + p2.y) / 2)
        inf = False

        slope = 0
        if num == 0:  # We have a horizontal line becoming vertical.
            inf = True
        elif denom == 0:  # We have a vertical line becoming horizontal.
            pass
        else:
            slope = num / denom
            slope = -(1 / slope)

        b = -((slope * midpoint.x) - midpoint.y)
        l = Line(slope, b, inf)
        return l

    def __repr__(self):
        if self.inf:
            return u'x = {}'.format(self.b)
        elif self.m == 0:
            return u'y = {}'.format(self.b)

        return u'y = {}x + {}'.format(self.m, self.b)


class Circle(object):
    def __init__(self, center, radius):
        self.center = center
        self.radius = Decimal(radius)

    @property
    def x(self):
        return self.center.x

    @property
    def y(self):
        return self.center.y

    def __repr__(self):
        return u'center {}, radius {}'.format(
            self.center,
            round(self.radius, 8)
        )

    @staticmethod
    def make_from_three_points(p1, p2, p3):
        l1 = Line.bisect(p1, p2)
        l2 = Line.bisect(p2, p3)
        center = l1.intersects_at(l2)
        radius = p1.distance_from(center)
        return Circle(center, radius)

    @staticmethod
    def make_from_two_points(p1, p2):
        center = Point((p1.x + p2.x) / 2, (p1.y + p2.y) / 2)
        radius = p1.distance_from(center)
        return Circle(center, radius)

    def contains_point(self, p):
        if p.distance_from(self.center) <= self.radius:
            return True
        return False


class CircleSplitter(object):
    solution_set = []

    def __init__(self, points):
        self.points = points
        if len(points) == 0:
            raise Exception('No points provided to the CircleSplitter')
        self.target_number = len(points)/2

    def within_bounds(self, circle):
        if circle.x + circle.radius > 1 or circle.x - circle.radius < 0:
            return False
        if circle.y + circle.radius > 1 or circle.y - circle.radius < 0:
            return False
        return True

    @property
    def solution(self):
        return next(iter(self.solution_set or []), None)

    def invalid_solution(self, c):
        # SKIP: Circle falls outside of the square
        # SKIP: Circle is larger than the best solution found so far
        return not self.within_bounds(c) or (self.solution and c.radius > self.solution.radius)

    def valid_solution(self, c):
        encircled_points = 0
        for p in self.points:
            if c.contains_point(p):
                encircled_points += 1
        return encircled_points == self.target_number

    def store_solution(self, c):
        if self.solution and c.radius == self.solution.radius:
            self.solution_set.append(c)
        else:
            self.solution_set = [c]

    def calculate(self):
        unique_trios = list(combinations(self.points, 3))
        for (t1, t2, t3) in unique_trios:
            try:
                c = Circle.make_from_three_points(t1, t2, t3)
            except LinesDoNotIntersect as e:
                continue
            if self.invalid_solution(c):
                continue
            if self.valid_solution(c):
                self.store_solution(c)

        unique_duos = list(combinations(self.points, 2))
        for (t1, t2) in unique_duos:
            c = Circle.make_from_two_points(t1, t2)
            if self.invalid_solution(c):
                continue
            if self.valid_solution(c):
                self.store_solution(c)

        return self.solution_set

