"""Method implementations under a common interface (see base.py).

Each method takes (news, target, lag, optional regime) and returns a
MethodVerdict. The harness iterates all (dataset, seed, hypothesis) tuples
and aggregates FPR/TPR per dataset.
"""
