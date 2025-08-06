
# Benchmarks for a complete Babel ingest using `ingest_babel.ply`

Trial Name  | Instance type               | Instance name    | Commit                                                                                                   | Start Time                  | End Time | Run Time (`hh:mm:ss`) | Peak Memory Usage | Size (kb, `du -k`) | Notes
--|--|--|--|--|--|--|--|--|--
Last Build  | `c7g.4xlarge` / `gp3`       | `stitch.rtx.ai`  | [`1bbbc50`](https://github.com/Translator-CATRAX/stitch/commit/1bbbc5056aafdaef8a159bee6e11810ffeea7c45) | `2025-07-27T18:29:52+00:00` | ?        | `64:50:31` | ? | 180723968 | Had to restart partial build, due to bug
Base Trial  | `c7g.4xlarge` / `gp3`       | `stitch.rtx.ai`  | [`ca59970`](https://github.com/Translator-CATRAX/stitch/commit/ca59970c860ef8e82c9cf1563ac71ed491b76660) | `2025-08-05T21:52:42+00:00` | ?        | ?        | ? | ? |
SSD storage | `i4i.2xlarge` / `Nitro SSD` | `stitch2.rtx.ai` | [`62af084`](https://github.com/Translator-CATRAX/stitch/commit/62af084666c93f01e1d7fa655d1252a0f17b9b64) | `2025-08-06T21:25:43+00:00` | ?        | ?        | ? | ? |

