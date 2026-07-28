<!-- template: independent-review v1 -->
<!-- Use this template verbatim. Substituting the declared <placeholders> is
     required and is not a modification; adding or editing any other wording is
     an anchoring leak (contract §4). -->

Independently review the following target(s):

- Files: <target files, each pinned to repository commit `<sha>`>
- design.md baseline (when applicable): commit `<sha or N/A>`
- Input packet: <path or N/A> (SHA-256 `<hash or N/A>`)
- Scope: <REQ IDs under review and affected dependent areas, or "full document">

You have received no prior analysis for this review. Judge for yourself.

Examine from these angles and report numbered findings:
1. Ambiguity — clauses interpretable in more than one way
2. Contradictions — within a document or between documents
3. Omissions — unstated exceptions, failure behaviors, boundary conditions
4. Unverifiable requirements — clauses whose completion cannot be judged
5. Risk — concerns involving money, data, security, or irreversibility

For each finding: location (section), severity (Blocker/Major/Minor), reasoning,
and a fix suggestion where possible.
If you find nothing, report "no findings" together with the basis for that
judgment. Do not modify the reviewed files. Report only.
