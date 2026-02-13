"""Org hierarchy resolution — build reporting trees and span-of-control metrics."""

import logging
from collections import defaultdict

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

type OrgNode = dict[str, str | int | list["OrgNode"]]
type ManagerChain = list[str]
type SpanOfControl = dict[str, int]


def _build_adjacency(employees: pd.DataFrame) -> dict[str, list[str]]:
    """Build a manager_id -> list[employee_id] adjacency map."""
    tree: dict[str, list[str]] = defaultdict(list)
    for _, row in employees.iterrows():
        mgr = row.get("manager_id")
        if pd.notna(mgr):
            tree[str(mgr)].append(str(row["employee_id"]))
    return dict(tree)


def _classify_org_level(depth: int) -> str:
    """Classify the organizational level based on depth from CEO."""
    match depth:
        case 0:
            return "CEO"
        case 1:
            return "C-Suite"
        case 2:
            return "VP"
        case 3:
            return "Director"
        case 4:
            return "Manager"
        case 5:
            return "Lead"
        case d if d <= 8:
            return "IC"
        case _:
            return "Deep IC"


def _walk_tree(
    node_id: str,
    adjacency: dict[str, list[str]],
    depth: int = 0,
) -> OrgNode:
    """Recursively walk the org tree to build a nested structure."""
    children = adjacency.get(node_id, [])
    return {
        "employee_id": node_id,
        "depth": depth,
        "org_level": _classify_org_level(depth),
        "direct_reports": len(children),
        "children": [_walk_tree(c, adjacency, depth + 1) for c in children],
    }


def resolve_org_hierarchy(employees: pd.DataFrame) -> pd.DataFrame:
    """Flatten the org tree into a DataFrame with depth and span-of-control metrics."""
    active = employees[employees["is_active"]].copy()
    adjacency = _build_adjacency(active)

    # Find root nodes (employees who are not subordinates of anyone,
    # or whose manager_id is not in the employee list)
    all_employees = set(active["employee_id"].astype(str))
    all_subordinates = {eid for subs in adjacency.values() for eid in subs}
    roots = all_employees - all_subordinates

    flat_rows = []

    def _flatten(node: OrgNode) -> None:
        flat_rows.append({
            "employee_id": node["employee_id"],
            "depth": node["depth"],
            "org_level": node["org_level"],
            "direct_reports": node["direct_reports"],
            "total_reports": _count_total(node) - 1,
        })
        for child in node["children"]:
            _flatten(child)

    def _count_total(node: OrgNode) -> int:
        return 1 + sum(_count_total(c) for c in node["children"])

    for root in roots:
        tree = _walk_tree(root, adjacency)
        _flatten(tree)

    result = pd.DataFrame(flat_rows)
    if not result.empty:
        result = result.merge(
            active[["employee_id", "department", "job_title"]].astype({"employee_id": str}),
            on="employee_id",
            how="left",
        )

    logger.info("Resolved org hierarchy: %d nodes, %d root(s)", len(result), len(roots))
    return result
