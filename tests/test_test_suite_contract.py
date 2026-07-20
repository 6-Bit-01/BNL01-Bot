import ast
from pathlib import Path
import unittest


class TestSuiteContractTests(unittest.TestCase):
    def test_no_module_level_tests_are_silently_skipped_by_unittest(self):
        offenders = []
        tests_dir = Path(__file__).resolve().parent
        for path in sorted(tests_dir.glob("test_*.py")):
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            cases = []
            installers = []
            for node in tree.body:
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name.startswith("test_"):
                    offenders.append(f"{path.name}:{node.lineno}:{node.name}")
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name.startswith("_case_"):
                    cases.append(node.name)
                if isinstance(node, (ast.Assign, ast.AnnAssign)):
                    targets = node.targets if isinstance(node, ast.Assign) else [node.target]
                    for target in targets:
                        if isinstance(target, ast.Name) and target.id.startswith("test_"):
                            offenders.append(f"{path.name}:{node.lineno}:{target.id}")
                if (
                    isinstance(node, ast.Expr)
                    and isinstance(node.value, ast.Call)
                    and isinstance(node.value.func, ast.Name)
                    and node.value.func.id == "install_function_cases"
                ):
                    installers.append(node.lineno)
            if cases and len(installers) != 1:
                offenders.append(
                    f"{path.name}:_case_ adapter count={len(installers)} for {len(cases)} cases"
                )
            if installers and not cases:
                offenders.append(f"{path.name}:installer_without_cases")
        self.assertEqual(
            offenders,
            [],
            "unittest discovery skips module-level test functions; convert these to TestCase methods: "
            + ", ".join(offenders),
        )


if __name__ == "__main__":
    unittest.main()
