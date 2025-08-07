
# Benchmarks for a complete Babel ingest using `ingest_babel.ply`

Trial Name  | Instance type               | Instance name    | Commit                                                                                                   | Start Time                  | End Time | Run Time (`hh:mm:ss`) | Peak Memory Usage | Size (kb, `du -k`) | Notes
--|--|--|--|--|--|--|--|--|--
Last Build  | `c7g.4xlarge` / `gp3`       | `stitch.rtx.ai`  | [`1bbbc50`](https://github.com/Translator-CATRAX/stitch/commit/1bbbc5056aafdaef8a159bee6e11810ffeea7c45) | `2025-07-27T18:29:52+00:00` | ?        | `64:50:31` | ? | 180723968 | Had to restart partial build, due to bug
Base Trial  | `c7g.4xlarge` / `gp3`       | `stitch.rtx.ai`  | [`9a22eed`](https://github.com/Translator-CATRAX/stitch/commit/9a22eedb3ca9952a9e4a4051609b6ea3dab2e93d) | `2025-08-07T01:49:38+00:00` | ?        | ?        | ? | ? |
SSD storage | `i4i.2xlarge` / `Nitro SSD` | `stitch2.rtx.ai` | [`9a22eed`](https://github.com/Translator-CATRAX/stitch/commit/9a22eedb3ca9952a9e4a4051609b6ea3dab2e93d) | `2025-08-07T01:51:20+00:00` | ?        | ?        | ? | ? |

