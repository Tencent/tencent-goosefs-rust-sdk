#!/usr/bin/env python3
# Copyright (C) 2026 Tencent. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Validate the title of a pull request.

This mirrors the convention used by projects such as Apache Fluss: every
merged commit is a *squash* of the PR, and GitHub automatically appends the
PR number to the squashed commit title, e.g.::

    [sdk] Reduce log verbosity in SplitGenerator (#3700)
                                                       ^^^^^^^^
                                            GitHub adds this on squash-merge

So the only thing we need to enforce is the PR *title* format. When the PR is
squash-merged, GitHub turns the title into a commit that links back to the PR
via ``(#NNNN)``. A title of the form ``[area] Short summary`` therefore yields
a commit that links to its PR, exactly like Fluss.

Allowed formats
---------------
* ``[area] Short summary``            (one area tag, e.g. ``[sdk]``)
* ``[area][area2] Short summary``     (multiple area tags, e.g. ``[sdk][py]``)
  The area tag is a lowercase identifier: ``[a-z0-9]`` plus ``/`` ``-`` ``_``
  and single spaces between words.
* Exceptions that need no area tag:
  ``Revert ...``, ``Release ...``, ``Bump ...``, ``Merge ...``,
  ``Initial ...``, ``chore(release): ...``.

Usage
-----
    python3 scripts/ci/pr_title_check.py "PR title here"
    # or read from $PR_TITLE (used by GitHub Actions)
"""

from __future__ import annotations

import os
import re
import sys

# One or more "[area]" tags followed by a non-empty summary.
# Area tag: starts with lowercase letter/digit, may contain lowercase
# letters, digits and the separators "/", "-", "_", and single spaces -- but
# each separator must occur *between* non-empty alphanumeric segments
# (e.g. "sdk", "sdk/py", "sdk-py"), so malformed tags like "[sdk ]",
# "[sdk py ]" or "[sdk//py]" are rejected.
_AREA_TITLE_RE = re.compile(
    r"^(?:\[[a-z0-9][a-z0-9]+(?:[ /_-][a-z0-9]+)*\])+\s+\S.*$"
)

# Titles that are allowed without an [area] tag.
_EXEMPT_RE = re.compile(
    r"^(?:"
    r"revert\b"  # git revert
    r"|release\b"
    r"|bump\b"
    r"|merge\b"
    r"|initial\b"
    r"|chore\(release\):"
    r")",
    re.IGNORECASE,
)


def is_valid(title: str) -> bool:
    title = (title or "").strip()
    if not title:
        return False
    if _EXEMPT_RE.match(title):
        return True
    return bool(_AREA_TITLE_RE.match(title))


def main() -> int:
    title = sys.argv[1] if len(sys.argv) > 1 else os.environ.get("PR_TITLE", "")
    title = (title or "").strip()
    if is_valid(title):
        print(f"OK: PR title follows the '[area] Summary' convention: {title!r}")
        return 0

    print(
        "ERROR: PR title does not follow the required convention.\n"
        "\n"
        "Expected one of:\n"
        '  * "[area] Short summary"            e.g. "[sdk] Reduce log verbosity"\n'
        '  * "[area][area2] Short summary"     e.g. "[sdk][py] Add retry to upload"\n'
        "\n"
        "On squash-merge, GitHub appends the PR number to the title, producing a\n"
        "commit like:\n"
        '    [sdk] Reduce log verbosity (#3700)\n'
        "which links back to this PR (the same behavior as Apache Fluss).\n"
        "\n"
        "Allowed without an [area] tag: titles starting with\n"
        "  Revert / Release / Bump / Merge / Initial / chore(release):\n"
        "\n"
        f"Actual title: {title!r}\n",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())