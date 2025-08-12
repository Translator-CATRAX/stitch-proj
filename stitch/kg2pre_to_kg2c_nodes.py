#!/usr/bin/env python3

import argparse
import sqlite3
from typing import Any, Optional
import local_babel as lb
import stitchutils as su

# Fill in with kg2_util.py when merged
CURIE_ID_KEY = 'id' # Fill in with kg2_util.py when merged

def _get_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description='kg2pre_to_kg2c_nodes.py: '
                                 'from a JSON-lines format of KG2pre nodes '
                                 'as input, produce a JSON-lines KG2c nodes file')
    ap.add_argument('nodes_file',
                    type=str,
                    help=('the nodes JSON-lines file, like kg2-10-3-nodes.jsonl'))
    ap.add_argument('babel_db',
                    type=str,
                    help='the sqlite database file for the local Babel database')
    ap.add_argument('nodes_output_file',
                    type=str,
                    help=('the nodes JSON lines file to which the output should be '
                          'saved'))
    return ap.parse_args()

def process_nodes(conn, nodes_file):
    cursor = conn.cursor()

    nodes_read_jsonlines_info = su.start_read_jsonlines(nodes_file)
    nodes = nodes_read_jsonlines_info[0]

    kg2pre_nodes_set = set()

    for node in nodes:
        node_curie = node[CURIE_ID_KEY]

        node_cliques = lb.map_any_curie_to_cliques(conn, node_curie)

        for node_clique in node_cliques:
            preferred_node_curie = node_clique['id']['identifier']
            preferred_node_description = node_clique['id']['description']
            preferred_node_name = node_clique['id']['label']
            preferred_node_category = node_clique['type']

            if preferred_node_category in {"biolink:Protein", "biolink:Gene"}:
                preferred_node_organism_taxon = lb.get_taxon_for_gene_or_protein(conn, preferred_node_curie)

            preferred_node_synonyms = lb.map_pref_curie_to_synonyms(cursor, preferred_node_curie)

        kg2pre_nodes_set.add(node_curie)

    su.end_read_jsonlines(nodes_read_jsonlines_info)


def main(nodes_file: str,
         babel_db: str,
         nodes_output_file: str):
    print(f"nodes file is: {nodes_file}")
    print(f"babel-db file is: {babel_db}")

    with lb.connect_to_db_read_only(babel_db) as conn:
        process_nodes(conn, nodes_file)


if __name__ == "__main__":
    main(**su.namespace_to_dict(_get_args()))

