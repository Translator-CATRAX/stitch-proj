#!/usr/bin/env python3

import sqlite3


def map_curie_to_preferred_curies(conn: sqlite3.Connection,
                                  curie: str) -> dict[str, str]:
    s = """SELECT prim_identif.curie, types.curie
FROM (((identifiers as prim_identif 
INNER JOIN cliques ON prim_identif.id = cliques.primary_identifier_id) 
INNER JOIN identifiers_cliques AS idcl ON cliques.id = idcl.clique_id) 
INNER JOIN identifiers on idcl.identifier_id = identifiers.id)
INNER JOIN types on cliques.type_id = types.id
WHERE identifiers.curie = ?;"""
    rows = conn.execute(s, (curie,)).fetchall()
    return {row[0]: row[1] for row in rows}


def map_pref_curie_to_synonyms(conn: sqlite3.Connection,
                               pref_curie: str) -> set[str]:
    s = """SELECT identifiers.curie
FROM ((identifiers as prim_identif 
INNER JOIN cliques ON prim_identif.id = cliques.primary_identifier_id) 
INNER JOIN identifiers_cliques AS idcl ON cliques.id = idcl.clique_id) 
INNER JOIN identifiers on idcl.identifier_id = identifiers.id
WHERE prim_identif.curie = ?;"""
    rows = conn.execute(s, (pref_curie,)).fetchall()
    return set(c[0] for c in rows)


with sqlite3.connect("db/babel-20250123.sqlite") as conn:
    print(map_curie_to_preferred_curies(conn, "UNII:059QF0KO0R"))
    print(map_pref_curie_to_synonyms(conn, "CHEBI:15377"))

