### How to run tests with nox
- Add environment variables to `.zshrc` or `.bashrc` file 
```vim
export RABBITMQ_DEFAULT_USER=<username>
export RABBITMQ_DEFAULT_PASS=<password>
export RABBITMQ_DOMAIN='localhost'
export RABBITMQ_DOMAIN_TLS='rabbitmq.neomodels.app'
export RABBITMQ_PORT='5672'
export RABBITMQ_PORT_TLS='5671'
export RABBITMQ_DEFAULT_VHOST='myMrsalHost'

export RABBITMQ_CAFILE='/path/to/rabbitmq/rabbit-ca.crt'
export RABBITMQ_CERT='/path/to/rabbitmq/rabbit_mlapp_client.crt'
export RABBITMQ_KEY='/path/to/rabbitmq/rabbit_mlapp_client.key'
```

```bash
source  ~/.zshrc
```

```bash
rm -rf .venv .nox reports/coverage/* reports/flake8/* reports/junit/* doc_images/*.svg

nox
```