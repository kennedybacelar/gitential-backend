FROM docker-internal.gitential.io/python-poetry:v6-py3.10.5-build as build

COPY pyproject.toml poetry.lock ./
RUN poetry install --no-root

FROM docker-internal.gitential.io/python-poetry:v6-py3.10.5 as prod

COPY --from=build --chown=app:app /project/.cache /project/.cache
COPY --from=build --chown=app:app /project/.local /project/.local
COPY --chown=app:app . /project/app
ARG APP_VERSION
RUN echo $APP_VERSION > /project/app/VERSION
RUN poetry install
ENTRYPOINT ["poetry", "run", "python", "-m", "gitential2"]
