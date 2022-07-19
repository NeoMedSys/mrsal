# py-amqp
PY-AMQP Message broker based on RabbitMQ 

## Package the code and add it to PyPI:
```bash
poetry install
poetry run tox
poetry config pypi-token.pypi <pypi-token>
poetry publish --build
```
### NB!
- If after running `poetry publish --build` we get the following error:
```bash
[UploadError]
HTTP Error 400: File already exists. See https://pypi.org/help file-name-reuse for more information.
```
Then we change `version` in `pyproject.toml` before rerun `poetry publish --build` again.
- To add the new library with poetry run : `poetry add rabbitamqp` 
- If you don't get the version you want then try clear poetry cache with `poetry cache clear pypi --all` and then re-run `poetry add rabbitamqp`