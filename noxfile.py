import os
import shutil

import nox
from nox.sessions import Session


@nox.session()
def setup(session: Session):
    try:
        root_path: str = os.getcwd()
        # session.run("poetry", "install", "--with", "dev", external=True)
        nox_path = os.path.join(root_path, ".nox")
        reports_path = os.path.join(root_path, "reports")
        coverage_path = os.path.join(reports_path, "coverage")
        junit_path = os.path.join(reports_path, "junit")
        ruff_path = os.path.join(reports_path, "ruff")
        badges_path = os.path.join(reports_path, "badges")

        if os.path.exists(nox_path):
            shutil.rmtree(nox_path)
        if os.path.exists(reports_path):
            shutil.rmtree(reports_path)

        os.makedirs(reports_path)
        os.makedirs(junit_path)
        os.makedirs(coverage_path)
        os.makedirs(ruff_path)
        os.makedirs(badges_path)
    except Exception as e:
        # Handle other exceptions
        print(f"Error: {e}")


@nox.session()
def tests(session: Session):
    session.run("poetry", "install", "--with", "dev", external=True)
    # coverage
    session.run(
        "poetry",
        "run",
        "coverage",
        "run",
        "--source=.",
        "--data-file",
        "./.coverage",
        "-m",
        "pytest",
        "./tests",
        "--junitxml=./reports/junit/junit.xml",
        "--ignore=./tests/test_ssl",
        external=True,
    )
    session.run("poetry", "run", "coverage", "report", external=True)
    session.run("poetry", "run", "coverage", "xml", external=True)
    session.run("poetry", "run", "coverage", "html", external=True)
    session.run("mv", ".coverage", "./reports/coverage", external=True)
    session.run("mv", "coverage.xml", "./reports/coverage", external=True)
    session.run("cp", "-R", "htmlcov/", "./reports/coverage", external=True)
    session.run("rm", "-R", "htmlcov/", external=True)


@nox.session()
def lint(session: Session):
    session.install("ruff")
    # Tell Nox to treat non-zero exit codes from Ruff as success using success_codes.
    # We do that because Ruff returns `1` when errors found in the code syntax, e.g(missing whitespace, ..)
    session.run("ruff", "check", ".", "--config=./ruff.toml", "--preview", "--statistics", "--output-file=./reports/ruff/ruff.txt", success_codes=[0, 1])


@nox.session()
def gen_badge(session: Session):
    session.install("genbadge[tests,coverage,flake8]")
    session.run("genbadge", "tests", "-i", "./reports/junit/junit.xml", "-o", "./reports/badges/tests-badge.svg")
    session.run("genbadge", "coverage", "-i", "./reports/coverage/coverage.xml", "-o", "./reports/badges/coverage-badge.svg")
    session.run("genbadge", "flake8", "-i", "./reports/ruff/ruff.txt", "-o", "./reports/badges/ruff-badge.svg")
