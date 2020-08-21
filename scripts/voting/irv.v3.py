import json
import random
from itertools import combinations
from operator import itemgetter
from statistics import mean

candidates = dict(enumerate([
    "A Fire Upon the Deep by Vernor Vinge",
    "A Deepness in the Sky by Vernor Vinge",
    "The Long Earth by Terry Pratchett & Stephen Baxter",
    "Blindsight by Peter Watts",
    "American Gods by Neil Gaiman",
    "A Game of Thrones by GRRM",
    "Gideon the Ninth by Tamsyn Muir",
    "The Final Empire by Brandon Sanderson",
    "Pathfinder by Orson Scott Card",
]))

# ([0-9])\s
# '$1, '
_ballots = {
    "cmathews@mintel.com": [1, 2, 5, 7, 4, 3, 6, 8, 9],
    "jdoepke@mintel.com": [2, 1, 5, 3, 4, 9, 8, 6, 7],
    "epingolt@mintel.com": [2, 6, 7, 5, 8, 9, 1, 3, 4],
    "jkalmer@mintel.com": [5, 6, 4, 7, 2, 8, 9, 1, 3],
    "chavlin@mintel.com": [1, 6, 5, 3, 8, 9, 2, 4, 7],
    "layers@mintel.com": [1, 6, 4, 3, 7, 9, 5, 2, 8],
    "bcouche@mintel.com": [8, 7, 6, 3, 2, 9, 4, 1, 5],
}

active_voters = [
    "cmathews@mintel.com",
    "jdoepke@mintel.com",
    "epingolt@mintel.com",
    "jkalmer@mintel.com",
    "chavlin@mintel.com",
    "layers@mintel.com",
    "bcouche@mintel.com",
]
active_voter_weight = 1


class Ballot:
    def __init__(self, name, ranking, **kwargs):
        assert sorted(list(set(ranking))) == sorted(ranking)
        self.name = name
        self.ranking = ranking
        self._candidates = None

    def __repr__(self):
        return f"Ballot(name={self.name}, ranking={self.ranking})"

    @property
    def candidates(self):
        if self._candidates:
            return self._candidates
        self._candidates = [self.ranking.index(i + 1) for i in range(len(candidates))]
        return self._candidates

    def position(self, c: int):
        return self.candidates.index(c)

    def favorite(self, candidates):
        _ranking = {c: self.position(c) for c in candidates}
        return min(_ranking.keys(), key=(lambda k: _ranking[k]))


def _format_ballot(b):
    msg = f"\n{b}"
    _fav = b.favorite(candidates.keys())
    msg = f"{msg}\n- Top Choice: [#{_fav}] {candidates[_fav]}"
    _cand = "\n\t".join([f"{b.ranking[c]}: {candidates[c]}" for c in b.candidates])
    msg = f"{msg}\n- Ranking:\n\t{_cand}"
    return msg


def output_round(totals, eliminate):
    print("Candidates")
    for candidate, count in totals.items():
        print(f"\t[#{candidate}] {candidates[candidate]} earned {count} votes")
    if eliminate is not None:
        print(f"\nWill eliminate [#{eliminate}] {candidates[eliminate]}")


def condorcet(candidates: list, ballots: list):
    comparisons = list(combinations(candidates, 2))
    # print(comparisons)
    score = {}

    for c in comparisons:
        key = str(sorted(list(c)))
        score[key] = {c[0]: 0, c[1]: 0}
        for b in ballots:
            if b.position(c[0]) > b.position(c[1]):
                score[key][c[0]] += 1
            else:
                score[key][c[1]] += 1

    # print(score)
    totals = {c: 0 for c in candidates}

    for key, s in score.items():
        pair = list(s.keys())
        if s[pair[0]] > s[pair[1]]:
            totals[pair[0]] += 1
        elif s[pair[0]] < s[pair[1]]:
            totals[pair[1]] += 1
        # print(f"{key}: {s}")

    for candidate, wins in totals.items():
        if wins == len(candidates) - 1:
            return totals, candidate
    return totals, None


def irv(candidates: list, ballots: list):
    if len(candidates) == 1:
        raise Exception("d i c t a t o r s h i p")

    totals = {c: 0 for c in candidates}
    for b in ballots:
        totals[b.favorite(candidates)] += 1

    eliminate = None
    elim_threshold = min(totals.values())
    elim_candidates = [c for c in candidates if totals[c] == elim_threshold]

    if len(elim_candidates) > 1:
        print("Tie-breaker...")
        elim_ballots = [b for b in ballots if b.favorite(candidates) in elim_candidates]

        # Attempt elimination with condorcet
        condorcet_totals, condorcet_winner = condorcet(elim_candidates, elim_ballots)
        elim_candidates = sorted(condorcet_totals, key=condorcet_totals.get)
        if condorcet_totals[elim_candidates[0]] != condorcet_totals[elim_candidates[1]]:
            eliminate = elim_candidates[0]
            print(f"\tcondorcet {condorcet_totals} -> {eliminate}")
        else:
            print(f"\tcondorcet failed {condorcet_totals}")

        # If one elimination candidate has more first-choice votes, prefer them
        if eliminate is None:
            elim_totals = {c: 0 for c in elim_candidates}
            for b in ballots:
                elim_candidate = b.candidates[0]
                if elim_candidate in elim_candidates:
                    elim_totals[elim_candidate] += 1
            elim_candidates = sorted(elim_totals, key=elim_totals.get)
            if elim_totals[elim_candidates[0]] != elim_totals[elim_candidates[1]]:
                eliminate = elim_candidates[0]
                print(f"\tfirst-choice {elim_totals} -> {eliminate}")
            else:
                print(f"\tfirst-choice failed {elim_totals}")

        # If there wasn't a first-choice winner, pick at random
        if eliminate is None:
            eliminate = random.choice(elim_candidates)
            print(f"\trandom {elim_candidates} -> {eliminate}")

    # Easy, no-tie elimination
    elif len(elim_candidates) == 1:
        eliminate = elim_candidates[0]

    return totals, eliminate


def main():
    ballots = [Ballot(k, v) for k, v in _ballots.items()]

    print("Candidates")
    for id, name in candidates.items():
        print(f"[#{id}] {name}")

    for b in ballots:
        print(f"{_format_ballot(b)}")

    round = 1
    _candidates = list(candidates.keys())

    while len(_candidates) > 1:
        print(f"\n\nRound #{round}")
        totals, eliminate = irv(_candidates, ballots)
        output_round(totals, eliminate)
        if eliminate is not None:
            _candidates.remove(eliminate)
            if len(_candidates) == 1:
                winner = _candidates[0]
                print(f"Winner: [#{winner}] {candidates[winner]}")
                return
        else:
            print("No elimination")
            return
        round += 1


__main__ = main()
