class Solution:
    def insert(self, intervals: List[List[int]], newInterval: List[int]) -> List[List[int]]:
        if not intervals:
            return [newInterval]

        start = bisect.bisect([i[0] for i in intervals], newInterval[0])
        end = bisect.bisect([i[1] for i in intervals], newInterval[1])

        _new_left = newInterval[0]
        _new_right = newInterval[1]

        if intervals[:start] and _new_left <= intervals[:start][-1][1]:
            _new_left = intervals[:start][-1][0]
            start -= 1

        if intervals[end:] and _new_right >= intervals[end:][0][0]:
            _new_right = intervals[end:][0][1]
            end += 1

        return [*intervals[:start], [_new_left, _new_right], *intervals[end:]]
