#!/usr/bin/env python3
"""Find @validate_call functions whose type annotations don't match runtime data.

Scans all Python files under deid/ for functions decorated with @validate_call
that use TypedDict annotations (from table_schemas) on parameters receiving
plain dicts or lists at runtime. These will fail with Pydantic ValidationError.

Usage:
    python scripts/find_validate_call_mismatches.py
"""

import ast
import sys
from pathlib import Path

# TypedDict names from deid/config/table_schemas.py that are known to cause
# runtime mismatches when used with @validate_call.
TYPEDDICT_NAMES = {
    "TableDetailsForUI",
    "ColumnDetailsForUI",
    "IgnoreRowsConfig",
    "IgnoreRowsColumn",
    "JoinCondition",
    "Condition",
}


def _get_annotation_name(node: ast.expr) -> str | None:
    """Extract the top-level name from a type annotation AST node."""
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    # Handle Union / X | Y
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.BitOr):
        left = _get_annotation_name(node.left)
        right = _get_annotation_name(node.right)
        # Return the TypedDict side if one exists
        if left in TYPEDDICT_NAMES:
            return left
        if right in TYPEDDICT_NAMES:
            return right
    # Handle Optional[X] / Union[X, Y]
    if isinstance(node, ast.Subscript):
        if isinstance(node.slice, ast.Tuple):
            for elt in node.slice.elts:
                name = _get_annotation_name(elt)
                if name in TYPEDDICT_NAMES:
                    return name
        else:
            return _get_annotation_name(node.slice)
    return None


def _has_validate_call_decorator(decorators: list[ast.expr]) -> bool:
    for dec in decorators:
        if isinstance(dec, ast.Name) and dec.id == "validate_call":
            return True
        if isinstance(dec, ast.Call):
            func = dec.func
            if isinstance(func, ast.Name) and func.id == "validate_call":
                return True
            if isinstance(func, ast.Attribute) and func.attr == "validate_call":
                return True
    return False


def scan_file(filepath: Path) -> list[dict]:
    issues = []
    try:
        source = filepath.read_text()
        tree = ast.parse(source, filename=str(filepath))
    except (SyntaxError, UnicodeDecodeError):
        return issues

    for node in ast.walk(tree):
        # Match both top-level functions and methods inside classes
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue

        if not _has_validate_call_decorator(node.decorator_list):
            continue

        for arg in node.args.args + node.args.posonlyargs + node.args.kwonlyargs:
            if arg.annotation is None:
                continue
            ann_name = _get_annotation_name(arg.annotation)
            if ann_name in TYPEDDICT_NAMES:
                issues.append({
                    "file": str(filepath),
                    "line": node.lineno,
                    "function": node.name,
                    "param": arg.arg,
                    "annotation": ann_name,
                })

    return issues


def main():
    root = Path(__file__).resolve().parent.parent / "deid"
    all_issues = []

    for pyfile in sorted(root.rglob("*.py")):
        all_issues.extend(scan_file(pyfile))

    if not all_issues:
        print("No mismatches found.")
        return 0

    print(f"Found {len(all_issues)} @validate_call parameter(s) using TypedDict annotations:\n")
    for issue in all_issues:
        print(f"  {issue['file']}:{issue['line']}")
        print(f"    {issue['function']}({issue['param']}: {issue['annotation']})")
        print(f"    -> Widen to `dict` or `list` to match runtime data\n")

    return 1


if __name__ == "__main__":
    sys.exit(main())
