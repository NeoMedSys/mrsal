import os
import shutil
import nox
from nox.sessions import Session

# Define paths for easy management
ROOT_PATH = os.getcwd()
NOX_PATH = os.path.join(ROOT_PATH, ".nox")
REPORTS_PATH = os.path.join(ROOT_PATH, "reports")
COVERAGE_PATH = os.path.join(REPORTS_PATH, "coverage")
JUNIT_PATH = os.path.join(REPORTS_PATH, "junit")
RUFF_PATH = os.path.join(REPORTS_PATH, "ruff")
BADGES_PATH = os.path.join(REPORTS_PATH, "badges")


@nox.session(name="setup", reuse_venv=True)
def setup(session: Session):
    """
    Setup the environment by creating necessary directories and cleaning up old artifacts.
    """
    try:
        # Clean up existing directories if they exist
        for path in [NOX_PATH, REPORTS_PATH]:
            if os.path.exists(path):
                shutil.rmtree(path)

        # Create required directories
        os.makedirs(COVERAGE_PATH, exist_ok=True)
        os.makedirs(JUNIT_PATH, exist_ok=True)
        os.makedirs(RUFF_PATH, exist_ok=True)
        os.makedirs(BADGES_PATH, exist_ok=True)

        session.log("Setup complete: directories are clean and ready.")
    except Exception as e:
        session.error(f"Setup failed: {e}")


@nox.session(name="tests", reuse_venv=True)
def tests(session: Session):
    """
    Run tests using coverage and output results in multiple formats.
    """
    try:
        # Install dependencies only necessary for testing
        session.run("poetry", "install", "--with", "dev", external=True)

        # Run pytest with coverage
        session.run(
            "poetry", "run", "coverage", "run",
            "--source=.",
            "--data-file", "./.coverage",
            "-m", "pytest",
            "./tests",
            "--junitxml=./reports/junit/junit.xml",
            "--ignore=./tests/test_ssl",
            external=True,
        )

        # Generate and move coverage reports
        session.run("poetry", "run", "coverage", "report", external=True)
        session.run("poetry", "run", "coverage", "xml", external=True)
        session.run("poetry", "run", "coverage", "html", external=True)
        shutil.move(".coverage", COVERAGE_PATH)
        shutil.move("coverage.xml", COVERAGE_PATH)
        shutil.copytree("htmlcov", os.path.join(COVERAGE_PATH, "htmlcov"))
        shutil.rmtree("htmlcov")

        session.log("Tests and coverage reporting complete.")
    except Exception as e:
        session.error(f"Tests failed: {e}")


@nox.session(name="lint", reuse_venv=True)
def lint(session: Session):
    """
    Run code linting using Ruff.
    """
    try:
        session.install("ruff")
        session.run(
            "ruff", "check", ".",
            "--config=./ruff.toml",
            "--preview",
            "--statistics",
            "--output-file=./reports/ruff/ruff.txt",
            success_codes=[0, 1]
        )
        session.log("Linting complete: check reports for details.")
    except Exception as e:
        session.error(f"Linting failed: {e}")


@nox.session(name="generate_badges", reuse_venv=True)
def gen_badge(session: Session):
    """
    Generate coverage badge.
    """
    try:
        session.install("genbadge[coverage]")
        session.run("genbadge", "coverage", "-i", "./reports/coverage/coverage.xml", "-o", "./reports/badges/coverage-badge.svg")

        session.log("Coverage badge generated successfully.")
    except Exception as e:
        session.error(f"Badge generation failed: {e}")


@nox.session(name="clean", reuse_venv=True)
def clean(session: Session):
    """
    Clean all generated directories and files to reset the environment.
    """
    try:
        for path in [NOX_PATH, REPORTS_PATH]:
            if os.path.exists(path):
                shutil.rmtree(path)

        session.log("Clean complete: all temporary files removed.")
    except Exception as e:
        session.error(f"Clean failed: {e}")
