FROM nexus-docker.ops.gitential.com/python-poetry:v1-py3.8.6-build as build

COPY pyproject.toml poetry.lock ./
RUN poetry install --no-root

FROM nexus-docker.ops.gitential.com/python-poetry:v1-py3.8.6 as prod

COPY --from=build --chown=app:app /project/.cache /project/.cache
COPY --from=build --chown=app:app /project/.local /project/.local
COPY --chown=app:app . /project/app

ENTRYPOINT ["poetry", "run", "python", "-m", "gitential2"]
