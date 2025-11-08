import os
from textwrap import dedent

# Assuming taf_august already exists in PyCharm
BASE_DIR = os.getcwd()  # current project root, e.g., taf_august

# Define subfolder and file structure
structure = {
    "src": {
        "data_validations": [
            "count_check.py",
            "duplicate_check.py",
            "null_value_check.py",
            "uniqueness_check.py",
            "records_only_in_source.py",
            "records_only_in_target.py",
            "data_compare_check.py",
            "schema_check.py"
        ],
        "utility": [
            "report_lib.py"
        ]
    },
    "tests": {
        "table1": [
            "config.yml",
            "transformation.sql",
            "test_table1.py"
        ],
        "table2": [
            "config.yml",
            "transformation.sql",
            "test_table4.py"
        ],
        ".": [
            "conftest.py"
        ]
    },
    ".": [
        "pytest.ini",
        "requirements.txt",
        "setup.py",
        "README.md",
        ".gitignore"
    ]
}


def create_structure(base, struct):
    """Recursively create folders and files."""
    for folder, contents in struct.items():
        folder_path = os.path.join(base, folder) if folder != "." else base
        os.makedirs(folder_path, exist_ok=True)

        if isinstance(contents, dict):
            create_structure(folder_path, contents)
        elif isinstance(contents, list):
            for file in contents:
                file_path = os.path.join(folder_path, file)
                if not os.path.exists(file_path):  # don't overwrite existing files
                    with open(file_path, 'w', encoding='utf-8') as f:
                        f.write(get_template(file))
                    print(f"✅ Created: {file_path}")
                else:
                    print(f"⚙️  Skipped (exists): {file_path}")


def get_template(filename):
    """Return default content for new files."""
    if filename == "pytest.ini":
        return dedent("""
            [pytest]
            addopts = -v --tb=short --maxfail=3
            testpaths = tests
        """).strip()

    elif filename == "requirements.txt":
        return dedent("""
            pytest
            pyyaml
            pandas
            pyspark
            openpyxl
            snowflake-connector-python
        """).strip()

    elif filename == "setup.py":
        return dedent("""
            from setuptools import setup, find_packages

            setup(
                name='taf_august',
                version='1.0.0',
                author='Sreenivasulu Kattubadi',
                description='Test Automation Framework for Data Quality Validation',
                packages=find_packages(where='src'),
                package_dir={'': 'src'},
                install_requires=[
                    'pytest',
                    'pyyaml',
                    'pandas',
                    'pyspark',
                    'snowflake-connector-python'
                ],
            )
        """).strip()

    elif filename == ".gitignore":
        return dedent("""
            __pycache__/
            .pytest_cache/
            .idea/
            *.log
            *.pyc
            .DS_Store
        """).strip()

    elif filename == "README.md":
        return dedent("""
            # TAF August - Data Quality Automation Framework

            This framework automates data validation between source and target systems
            using **PySpark**, **Pytest**, and **YAML-based configuration**.

            ## 📂 Key Folders
            - `src/data_validations/` — Validation scripts
            - `src/utility/` — Common utility modules
            - `tests/` — Table-wise test folders

            ## 🚀 Run Tests
            ```bash
            pip install -r requirements.txt
            pytest
            ```
        """).strip()

    elif filename == "conftest.py":
        return dedent("""
            import pytest

            @pytest.fixture(scope="session")
            def setup_environment():
                print("\\n🔧 Setting up test environment...")
                yield
                print("\\n🧹 Cleaning up environment...")
        """).strip()

    elif filename.endswith(".yml"):
        return dedent("""
            source:
              type: csv
              path: "path/to/source.csv"

            target:
              type: snowflake
              table: "TARGET_TABLE_NAME"
        """).strip()

    elif filename.endswith(".sql"):
        return "-- Write SQL queries for validation here\n"

    elif filename.startswith("test_"):
        return dedent("""
            def test_sample():
                assert True  # Replace with actual test logic
        """).strip()

    else:
        return ""


if __name__ == "__main__":
    print(f"\n📁 Creating folder structure under: {BASE_DIR}\n")
    create_structure(BASE_DIR, structure)
    print("\n🎉 All required folders and files are now set up!\n")