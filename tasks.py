import os
from invoke import task

PACKAGE_DIR = "gitential2"
TESTS_DIR = "tests"


@task
def lint(ctx):
    ctx.run(f"black --check {PACKAGE_DIR} {TESTS_DIR}")
    ctx.run(f"pylint {PACKAGE_DIR}")
    ctx.run(f"mypy {PACKAGE_DIR}")


@task
def unit_test(ctx):
    ctx.run(f"pytest -v {TESTS_DIR}/unit")


@task
def integration_test(ctx, cleanup_after=False):
    cmd = f"pytest -v -x {TESTS_DIR}/integration"
    if _am_i_running_in_ci():
        # postgres and redis already linked to the container
        ctx.run(cmd)
    else:
        _run_with_docker_compose_services(ctx, cmd, cleanup_after)


def _am_i_running_in_ci():
    return os.getenv("CI_JOB_ID") is not None


def _run_with_docker_compose_services(ctx, cmd, cleanup_after):
    ctx.run("find . -name '*.pyc' -delete")
    ctx.run("docker-compose up -d")
    ctx.run(cmd)
    if cleanup_after:
        ctx.run("docker-compose down")
