### How to run tests with nox
- Add environment variables to `.zshrc` or `.bashrc` file 
```vim
export RABBITMQ_DEFAULT_USER=<username>
export RABBITMQ_DEFAULT_PASS=<password>
export RABBITMQ_DOMAIN=<your_domain>
export RABBITMQ_DOMAIN_TLS=<your_domain>
export RABBITMQ_PORT=5672'
export RABBITMQ_PORT_TLS='5671'
export RABBITMQ_DEFAULT_VHOST=<your_vhost>

export RABBITMQ_CAFILE=</path/to/cafile>
export RABBITMQ_CERT=</path/to/client_crt>
export RABBITMQ_KEY=</path/to/client_key>
```

```bash
source  ~/.zshrc
```

```bash
rm -rf .venv poetry.lock .nox doc_images/*.svg reports/junit/*.xml reports/flake8/*.txt reports/coverage/.coverage reports/coverage/coverage.xml reports/coverage/htmlcov/

nox
```