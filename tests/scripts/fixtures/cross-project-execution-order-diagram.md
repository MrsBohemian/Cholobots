<!--
ABOUTME: Point-in-time snapshot of the ## Dependency Diagram section proposed
ABOUTME: for homelab wiki page 1 (cross-project-execution-order). Test fixture
ABOUTME: only — the live wiki is the source of truth; this file MUST NOT be
ABOUTME: kept in sync with post-write homelab drift. Its sole purpose is to
ABOUTME: prove that parse_dependency_diagram(content, default_namespace="homelab")
ABOUTME: returns the documented graph.
-->

## Dependency Diagram

_Machine-parseable mirror of the hard prerequisite edges above. Parsed by `scripts/lib/dependency_parser.py` (`parse_dependency_diagram(content, default_namespace="homelab")`) in the Alterna internal-infra repo. **The prose sections remain authoritative** — this block encodes hard prereqs only; soft deps and rationale edges are deliberately omitted. Refs: bare `#N` = homelab (project 1); `alterna#N` = Alterna Dev Studio (4); `gg#N` = Growth Garage (5). Append new stanzas; never rewrite existing lines._

```
alterna#329
  ├─► alterna#330, alterna#347, #369, #380, alterna#335

#380
  └─► alterna#330

alterna#330
  └─► #368
alterna#347
  └─► #368
#369
  └─► #368
#380
  └─► #368
alterna#335
  └─► #368

#368
  └─► alterna#331
        └─► alterna#332
              ├─► alterna#333
              └─► alterna#334

alterna#412
  ├─► alterna#413
  └─► alterna#414

alterna#413
  └─► alterna#183
alterna#414
  └─► alterna#183
#178
  └─► alterna#183

alterna#183
  └─► alterna#406

#173
  └─► #174
        └─► #175
              └─► #177
                    └─► #178

gg#3
  ├─► alterna#17
  └─► #20
gg#4
  ├─► alterna#17
  └─► #20

#28
  └─► #20

gg#1
  └─► alterna#1

alterna#190
  └─► #164
alterna#191
  └─► #164

alterna#328
  ├─► #367
  ├─► #106
  ├─► #258
  ├─► #20
  └─► #27
```
