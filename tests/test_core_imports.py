"""Verify core engine has no Django imports after refactoring."""
import ast
import pathlib


def _collect_imports(filepath: pathlib.Path) -> list[str]:
    """Parse a Python file and return all imported module names."""
    source = filepath.read_text()
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return []  # Skip unparseable files
    imports = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                imports.append(alias.name)
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                imports.append(node.module)
    return imports


def test_no_django_imports_in_core():
    core_dir = pathlib.Path("deid/core")
    django_imports = []
    for pyfile in core_dir.rglob("*.py"):
        for imp in _collect_imports(pyfile):
            if "django" in imp or imp.startswith("nd_api"):
                django_imports.append(f"{pyfile}: {imp}")
    assert django_imports == [], f"Django/nd_api imports found in core:\n" + "\n".join(django_imports)
