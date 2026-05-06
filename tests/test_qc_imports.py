"""Verify QC package has no Django/nd_api/old-path imports after refactoring."""
import ast
import pathlib


def _collect_imports(filepath: pathlib.Path) -> list[str]:
    """Parse a Python file and return all imported module names."""
    source = filepath.read_text()
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return []
    imports = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                imports.append(alias.name)
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                imports.append(node.module)
    return imports


def test_no_django_imports_in_qc():
    qc_dir = pathlib.Path("deid/qc")
    bad_imports = []
    for pyfile in qc_dir.rglob("*.py"):
        for imp in _collect_imports(pyfile):
            if any(keyword in imp for keyword in ("django", "nd_api", "qc_package")):
                bad_imports.append(f"{pyfile}: {imp}")
    assert bad_imports == [], (
        f"Django/nd_api/qc_package imports found in QC package:\n" + "\n".join(bad_imports)
    )


def test_no_old_core_imports_in_qc():
    qc_dir = pathlib.Path("deid/qc")
    bad_imports = []
    for pyfile in qc_dir.rglob("*.py"):
        for imp in _collect_imports(pyfile):
            if imp.startswith("core.") or imp.startswith("deIdentification."):
                bad_imports.append(f"{pyfile}: {imp}")
    assert bad_imports == [], (
        f"Old-path imports found in QC package:\n" + "\n".join(bad_imports)
    )
