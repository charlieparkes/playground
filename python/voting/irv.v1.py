import hashlib
import json
from statistics import mean

candidates = {
    k: v
    for k, v in enumerate(
        [
            "Neuromancer by William Gibson",
            "Guards! Guards! by Terry Pratchett",
            "The Library at Mount Char by Scott Hawkins",
            "The Raven Tower by Ann Leckie",
            "A Fire Upon the Deep by Vernor Vinge",
        ]
    )
}

# ([0-9])\s
# '$1, '
_ballots = {
    "jdoepke@mintel.com": [2, 5, 4, 1, 3],
    "cmathews@mintel.com": [3, 5, 2, 4, 1],
    "ekishchukova@mintel.com": [2, 3, 5, 4, 1],
    "layers@mintel.com": [4, 2, 1, 3, 5],
    "epingolt@mintel.com": [4, 1, 5, 3, 2],
    "agura@mintel.com": [3, 1, 5, 4, 2],
}

active_voters = [
    "jdoepke@mintel.com",
    "cmathews@mintel.com",
    "ekishchukova@mintel.com",
    "agura@mintel.com",
]
active_voter_weight = 2


def hash(data):
    return hashlib.sha1(json.dumps(data).encode("utf-8")).hexdigest()


class Ballot:
    def __init__(self, name, ranking):
        assert sorted(list(set(ranking))) == sorted(ranking)
        self.name = name
        self.ranking = ranking
        self.cache = {}

    def __repr__(self):
        return f"Ballot(name={self.name}, ranking={self.ranking})"

    def favorite(self, active_candidates):
        """
        Given a list of candidate IDs [0, 2, 3]

        """
        if not active_candidates:
            return None
        key = hash(list(active_candidates))
        if key in self.cache:
            return self.cache[key]
        try:
            _ranking = [
                j if i in active_candidates else len(candidates) + 1
                for i, j in enumerate(self.ranking)
            ]
            self.cache[key] = self.ranking.index(min(_ranking))
            print(f"{self.name}\t\t{self.ranking} {_ranking} {self.cache[key]}")
            return self.cache[key]
        except Exception as e:
            print(
                f"{self} failed to find favorite candidate among {active_candidates} - {e}"
            )


ballots = [Ballot(k, v) for k, v in _ballots.items()]


def unpopular_popularity(unpopular, popular=None):
    print(f"unpopular({unpopular}, {popular})")
    popular = set(popular if popular else [])
    [popular.discard(c) for c in unpopular]
    scale = len(unpopular)
    popularity = {}
    for c in unpopular:
        try:
            _uncounted_ballots = [b for b in ballots if b.favorite(popular) is not None]
            _unpopular_ballots = [b.ranking[c] for b in _uncounted_ballots]
            popularity[c] = mean(_unpopular_ballots) / scale
            print(f"uncounted: {_uncounted_ballots}")
            print(f"unpopular {_unpopular_ballots}")
            print(f"popularity[{c}]: {popularity[c]}")
        except:
            popularity[c] = 0
    return popularity


def count_votes(active_candidates, weigh=False):
    distribution = {ac: 0 for ac in active_candidates}
    for b in ballots:
        distribution[b.favorite(active_candidates)] += (
            active_voter_weight if weigh and b.name in active_voters else 1
        )
    return distribution


def rank():
    n_voters = len(ballots)
    majority = int(n_voters / 2) + 1
    print(f"Voters: {n_voters}, Majority Required: {majority}")

    active_candidates = candidates.copy().keys()
    pop = unpopular_popularity(active_candidates)
    for cid, c in candidates.items():
        print(f"[{cid}] {c}:")
        print(f"\tplurality votes: {count_votes(active_candidates)[cid]}")
        print(f"\tunpopular popularity: {pop[cid]}")

    while len(active_candidates) > 1:
        print(f"\nCandidates: {active_candidates}")
        # distribution = {ac: 0 for ac in active_candidates}
        # for b in ballots:
        #     distribution[b.favorite(active_candidates)] += 1
        distribution = count_votes(active_candidates)
        for id, count in distribution.items():
            print(f"[{id}] {candidates[id]} -> {count} votes")

        # Remove least-favorited candidates
        dist_values = distribution.values()
        lowest_count = min(dist_values)
        if list(dist_values).count(lowest_count) > 1:
            lowest_candidates = [
                ac for ac, count in distribution.items() if count == lowest_count
            ]
            print(f"Tie breaker between {lowest_candidates}...")
            pop = unpopular_popularity(lowest_candidates, active_candidates)
            lc_scores = {lc: pop[lc] for lc in lowest_candidates}
            print(f"{lc_scores}")
            lowest_scorer = None
            for lc, score in lc_scores.items():
                if not lowest_scorer or score < lc_scores[lowest_scorer]:
                    lowest_scorer = lc
                print(f"{lc} averaged {score}")
            # If these are the same, draw lots because ffs this is for a book club.
            active_candidates = [
                k for k, v in distribution.items() if k != lowest_scorer
            ]
        else:
            active_candidates = [
                k for k, v in distribution.items() if v != lowest_count
            ]

    if len(active_candidates) > 0:
        print(f"\nWinner: {candidates[active_candidates[0]]}")
    else:
        print("\nAll candidates eliminated.")


__main__ = rank()
