import nox


@nox.session()
def tests(session):
    session.run('poetry', 'install', '--with', 'dev')
    session.run('poetry', 'run', 'pytest', './tests', '--junitxml=./junit.xml')
    # coverage
    session.run('poetry', 'run', 'coverage', 'run', '--source=.', '--data-file', './.coverage',
                '-m', 'pytest', './tests')
    session.run('poetry', 'run', 'coverage', 'xml')


@nox.session()
def lint(session):
    session.install('flake8')
    session.run('flake8', '.', '--max-line-length=120', '--exit-zero',
                '--format=%(path)s::%(row)d,%(col)d::%(code)s::%(text)s', '--statistics', '--tee',
                '--output-file', 'flake8.txt')
