# py-amqp
PY-AMQP Message broker based on RabbitMQ 

## Package the code and add it to PyPI
```bash
poetry install
poetry run tox
poetry config pypi-token.pypi <pypi-token>
poetry publish --build
```
## Change the version if it already exists
- If after running `"poetry publish --build"` you get the following error:
```bash
[UploadError]
HTTP Error 400: File already exists. See https://pypi.org/help file-name-reuse for more information.
```
- Then we change `version` in `pyproject.toml` before rerun `"poetry publish --build"` again.

## Install the library with poetry
- To add the new library with poetry run : `"poetry add mrsal"` 
- If you don't get the latest version, then try to clear poetry cache with `"poetry cache clear pypi --all"` and then re-run `"poetry add mrsal"`

## Run tests locally
- Build image and start container
```bash
docker-compose -f docker-compose.yml up
```
- Run tests using tox
```bash
poetry run tox
```

## Run CI workflow in Github
```bash
Github --> Actions --> Choose workflow "mrsal" --> Run workflow --> Choose branch --> Run
```