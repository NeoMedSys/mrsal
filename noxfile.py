import nox
from nox.sessions import Session


@nox.session()
def tests(session: Session):
    session.run("poetry", "install", "--with", "dev", external=True)
    # session.run("poetry", "run", "pytest", "./tests", "--ignore", "./tests/test_ssl", "--junitxml=./reports/junit/junit.xml", external=True)
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
    session.run("genbadge", "tests", "-i", "./reports/junit/junit.xml", "-o", "./doc_images/tests-badge.svg")
    session.run("genbadge", "coverage", "-i", "./reports/coverage/coverage.xml", "-o", "./doc_images/coverage-badge.svg")
    session.run("genbadge", "flake8", "-i", "./reports/ruff/ruff.txt", "-o", "./doc_images/ruff-badge.svg")
