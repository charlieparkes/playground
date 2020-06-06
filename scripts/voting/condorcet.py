import hashlib
import json
import random
from itertools import combinations
from statistics import mean

candidates = {k: v for k, v in enumerate([
    "Neuromancer by William Gibson",                # 0
    "Guards! Guards! by Terry Pratchett",           # 1
    "The Library at Mount Char by Scott Hawkins",   # 2
    "The Raven Tower by Ann Leckie",                # 3
    "A Fire Upon the Deep by Vernor Vinge"          # 4
])}

# ([0-9])\s
# '$1, '
_ballots = {                   # 0  1  2  3  4
    "jdoepke@mintel.com":       [2, 5, 4, 1, 3],
    "cmathews@mintel.com":      [3, 5, 4, 2, 1],
    "ekishchukova@mintel.com":  [2, 3, 5, 4, 1],
    "layers@mintel.com":        [4, 2, 1, 3, 5],
    "epingolt@mintel.com":      [4, 1, 5, 3, 2],
    "agura@mintel.com":         [3, 1, 5, 4, 2],
}

active_voters = [
    "jdoepke@mintel.com",
    "cmathews@mintel.com",
    "ekishchukova@mintel.com",
    "agura@mintel.com",
]
active_voter_weight = 1.5


def hash(data):
    return hashlib.sha1(json.dumps(data).encode("utf-8")).hexdigest()


class Ballot:
    def __init__(self, name, ranking, **kwargs):
        assert sorted(list(set(ranking))) == sorted(ranking)
        self.name = name
        self.ranking = ranking
        self.cache = {}
        self._candidates = None

    def __repr__(self):
        return f"Ballot(name={self.name}, ranking={self.ranking}, candidates={self.candidates})"

    @property
    def candidates(self):
        if self._candidates:
            return self._candidates
        self._candidates = [self.ranking.index(i+1) for i in range(len(candidates))]
        return self._candidates

    def position(self, c):
        return self.candidates.index(c)


ballots = [Ballot(k, v) for k, v in _ballots.items()]


def condorcet(candidates: list, ballots: list):
    comparisons = list(combinations(candidates, 2))
    score = {}

    for c in comparisons:
        key = str(sorted(list(c)))
        score[key] = {c[0]: 0, c[1]: 0}
        for b in ballots:
            if b.position(c[0]) > b.position(c[1]):
                score[key][c[0]] += 1
            else:
                score[key][c[1]] += 1

    totals = {c: 0 for c in candidates}

    for key, s in score.items():
        pair = list(s.keys())
        if s[pair[0]] > s[pair[1]]:
            totals[pair[0]] += 1
        elif s[pair[0]] < s[pair[1]]:
            totals[pair[1]] += 1
        # print(f"{key}: {s}")

    for candidate, wins in totals.items():
        if wins == len(candidates)-1:
            return totals, candidate
    return totals, None


# IRV elimination
# _min = min(totals.values())
# lowest = [c for c in candidates if totals[c] == _min]
# lowest_of_the_low = irv(lowest, ballots)
#
# def irv(_candidates: list, ballots: list):
#     distribution = {c: 0 for c in _candidates}
#     for b in ballots:
#         _ranking = [j if i in _candidates else len(candidates)+1 for i, j in enumerate(b.ranking)]
#         favorite = b.ranking.index(min(_ranking))
#         distribution[favorite] += 1
#     print(distribution)


# Average rank elimination
# print(_candidates)
# lowest_of_the_low = average_rank_elimination(_candidates, totals, ballots)
# print(f"Trailing candidate with lowest average placement was {lowest_of_the_low}")
# _candidates.remove(lowest_of_the_low)
# print(_candidates)
#
# def avg_rank(candidate: int, ballots: list):
#     return mean([b.position(candidate) for b in ballots])
#
# def average_rank_elimination(candidates: list, totals: dict, ballots: list):
#     print("Seeking lowest...")
#     _min = min(totals.values())
#     lowest = [c for c in candidates if totals[c] == _min]
#     print(f"Lowest condorcet value was {_min} and was discovered on candidates {lowest}")
#     avg_ranks = {c: avg_rank(c, ballots) for c in lowest}
#     return max(avg_ranks.keys(), key=(lambda k: avg_ranks[k]))


def main():
    for b in ballots:
        print(f"{b}")

    winner = False
    _candidates = list(candidates.keys())
    while not winner:
        totals, winner = condorcet(_candidates, ballots)

        # for candidate, wins in totals.items():
        #     print(f"{candidate}: {wins}")

        if winner:
            print(f"Winner: [{winner}] {candidates[winner]}")
            return winner
        else:
            # All lowest or random elimination
            # https://www.daneckam.com/?p=374
            _min = min(totals.values())
            lowest = [c for c in candidates if totals[c] == _min]
            if len(lowest) == len(_candidates):
                _candidates.remove(random.choice(_candidates))
            else:
                for l in lowest:
                    _candidates.remove(l)


__main__ = main()
