#!/usr/bin/env python3.12

import argparse
import bmt
import itertools as it
import json
import pprint
import pandas as pd
import sqlite3
import local_babel as lb
import stitchutils as su
from typing import Any, Callable, Iterable, Optional


def _predicate_curie_to_space_case(curie: str) -> str: # noqa
    return curie[len('biolink:'):].replace('_', ' ')

def _get_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description='normalize_kg2pre.py: '
                                 'from a JSON-lines format of KG2pre '
                                 'as input, produce a JSON-lines KG2c')
    ap.add_argument('nodes_file',
                    type=str,
                    help=('the nodes JSON-lines file, like kg2-10-3-nodes.jsonl'
                          'or kg2-10-3-nodes.jsonl.gz (i.e., compression is OK)'))
    ap.add_argument('edges_file',
                    type=str,
                    help=('the edges JSON lines file, like kg2-10-3-edges.jsonl'
                          'or kg2-10-3-edges.jsonl.gz (i.e., compression is OK)'))
    ap.add_argument('babel_db',
                    type=str,
                    help='the sqlite database file for the local Babel database')
    ap.add_argument('edges_output_file',
                    type=str,
                    help=('the edges JSON lines file to which the output should be '
                          'saved'))
    return ap.parse_args()

def _read_jsonl_chunks(filename: str,
                       lines_per_chunk: int) -> Iterable[pd.DataFrame]:
    return pd.read_json(filename,
                        lines=True,
                        chunksize=lines_per_chunk)

EDGE_PROPERTIES_COPY_FROM_KG2PRE = \
    ('agent_type',
     'knowledge_level',
     'predicate',
     'primary_knowledge_source',
     'domain_range_exclusion')

EDGE_PROPERTIES_COPY_FROM_KG2PRE_IF_EXIST = \
    ('qualified_predicate',
     'qualified_object_direction',
     'qualified_object_aspect')

PREDICATE_CURIES_SKIP = tuple()
#    ('biolink:same_as',
#     'biolink:related_to',
#     'biolink:close_match',
#     'biolink:subclass_of',
#     'biolink:has_subclass',
#     'biolink:exact_match')

def _pick_category(categories: set[str],
                       sub_obj: str,
                       predicate: str) -> set[str]:
    return set()

def _make_pick_category():
    tk = bmt.Toolkit()
    def pick_category(categories: set[str],
                      sub_obj: str,
                      predicate: str) -> set[str]:
        if sub_obj == "subject":
            pred_finder = tk.get_all_predicates_with_class_domain
        elif sub_obj == "object":
            pred_finder = tk.get_all_predicates_with_class_range
        else:
            raise ValueError(f"invalid value for sub_obj: {sub_obj}; "
                             "must be \"subject\" or \"object\"")
        for category in categories:
            allowed_preds = pred_finder(category, check_ancestors=True, formatted=True)
            if predicate in allowed_preds:
                return {category}
        if categories == {'biolink:Protein', 'biolink:SmallMolecule'}:
            return {'biolink:SmallMolecule'}
        if categories == {'biolink:Protein', 'biolink:ChemicalEntity'}:
            if predicate == 'biolink:coexists_with':
                return {'biolink:Protein'}
            if (predicate == 'biolink:may_be_treated_by' \
                and sub_obj == 'object') \
                or \
                (predicate == 'biolink:may_treat' \
                 and sub_obj == 'subject'):
                return {'biolink:ChemicalEntity'}
            if (predicate == 'biolink:affects' and sub_obj == 'object') or \
               (predicate in {'biolink:causes', 'biolink:has_input'} and \
                sub_obj == 'subject'):
                return {'biolink:Protein'}
        return categories
    return pick_category

def _filter_pref_curies(pref_curie_tuple: tuple[tuple[str, str, str], ...],
                        sub_obj: str,
                        predicate: str) -> set[str]:
    if len(pref_curie_tuple) == 0:
        return set()
    if len(pref_curie_tuple) == 1:
        return {pref_curie_tuple[0][0]}
    categories_to_pref_curies = {st[1]: st[0] for st in pref_curie_tuple}
    categories = set(categories_to_pref_curies.keys())
    picked_categories = _pick_category(categories, sub_obj, predicate)
    return {categories_to_pref_curies[c] for c in picked_categories}

def _fix_curie_if_broken(curie: str) -> str:
    if curie.startswith('OBO:NCIT_'):
        curie = 'NCIT:' + curie[len('OBO:NCIT_'):]
    return curie

def _make_process_edges_row(conn: sqlite3.Connection) -> Callable:
    def process_edges_row(edge_series: pd.Series) -> \
            tuple[tuple[Optional[dict[str, Any]], str, str], ...]:
        edge = edge_series.to_dict()
        kg2pre_edge_id = edge['id']
        res_edge = {k: edge[k] for k in EDGE_PROPERTIES_COPY_FROM_KG2PRE}
        res_edge['id'] = None  # this will eventually be a global integer index
                               # of the edge in a list of all edges (can't compute
                               # that information here since we are processing one
                               # edge only, within this function
        res_edge['kg2_ids'] = [kg2pre_edge_id]
        predicate = res_edge['predicate']
        if predicate in PREDICATE_CURIES_SKIP:
            return ((None,
                     kg2pre_edge_id,
                     f"predicate is on the skip list: {predicate}"),)
        res_edge.update({k: edge[k] for k in EDGE_PROPERTIES_COPY_FROM_KG2PRE_IF_EXIST
                         if su.nan_to_none(edge[k])})
        kg2pre_subject_curie = _fix_curie_if_broken(edge['subject'])
        pref_curie_tuple = lb.map_curie_to_preferred_curies(conn,
                                                            kg2pre_subject_curie)
        picked_pref_curies_subject = _filter_pref_curies(pref_curie_tuple,
                                                         "subject",
                                                         predicate)
        if len(picked_pref_curies_subject)==0:
            return ((None, kg2pre_edge_id,
                     "unable to find preferred CURIE for subject: "
                     f"{kg2pre_subject_curie}"),)
        kg2pre_object_curie = _fix_curie_if_broken(edge['object'])
        pref_curie_tuple = lb.map_curie_to_preferred_curies(conn,
                                                            kg2pre_object_curie)
        picked_pref_curies_object = _filter_pref_curies(pref_curie_tuple,
                                                        "object",
                                                        predicate)
        if len(picked_pref_curies_object)==0:
            return ((None, kg2pre_edge_id,
                     "unable to find preferred CURIE for object: "
                     f"{kg2pre_object_curie}"),)
        if len(picked_pref_curies_subject) > 2 or len(picked_pref_curies_object) > 2:
            print(edge_series)
            assert False
        res: list[tuple[Optional[dict[str, Any]], str, str]] = []
        for subject_curie, object_curie in it.product(picked_pref_curies_subject,
                                                      picked_pref_curies_object):
            new_res_edge = res_edge
            new_res_edge['subject'] = subject_curie
            new_res_edge['object'] = object_curie
            res.append((new_res_edge, kg2pre_edge_id, 'OK'))
        if len(res) == 0:
            res.append((None,
                        kg2pre_edge_id,
                        "no preferred curies available"))
        return tuple(res)
    return process_edges_row

def _process_edges_row(row: pd.Series) -> \
        tuple[tuple[Optional[dict[str, Any]], str, str], ...]:
    return tuple()

def _process_chunk_of_edges(edge_chunk: pd.DataFrame) -> \
        Iterable[tuple[Optional[dict[str, Any]], str, str]]:
    t = it.chain.from_iterable(tuple(_process_edges_row(row)) \
                               for _, row in edge_chunk.iterrows())
    return filter(lambda st: st[0] is not None, t)

def main(nodes_file: str,
         edges_file: str,
         babel_db: str,
         edges_output_file: str):
    print(f"nodes file is: {nodes_file}")
    print(f"edges file is: {edges_file}")
    global _pick_category
    _pick_category = _make_pick_category()
    with lb.connect_to_db_read_only(babel_db) as conn:
        global _process_edges_row
        _process_edges_row = _make_process_edges_row(conn)
        chunks_iter = _read_jsonl_chunks(edges_file, 100_000)
        edge_tuples = tuple(t[0] for t in
                            it.chain.from_iterable(map(_process_chunk_of_edges,
                                                       chunks_iter))
                            if t[0] is not None)
        with open(edges_output_file, 'w') as fo:
            for record in edge_tuples:
                json.dump(record, fo)
                fo.write("\n")

if __name__ == "__main__":
    main(**su.namespace_to_dict(_get_args()))

