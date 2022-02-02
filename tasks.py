import os
from invoke import task

PACKAGE_DIR = "gitential2"
TESTS_DIR = "tests"
DOCKER_REGISTRY = "docker-internal.gitential.io"
TESTING_IMAGE_TAG = "v4-20220131"


@task
def build_testing_image(ctx):
    ctx.run(f"docker build -f Dockerfile.testing -t {DOCKER_REGISTRY}/gitential2-testing:{TESTING_IMAGE_TAG} .")
    ctx.run(f"docker push {DOCKER_REGISTRY}/gitential2-testing:{TESTING_IMAGE_TAG}")


@task
def dependency_check(ctx):
    ignores = [
        42194,  # sqlalchemy-utils           | 0.37.9    | >=0.27.0
        44715,  # numpy                      | 1.22.1    | >0
        # | All versions of Numpy are affected by CVE-2021-41495: A null Pointer         |
        # | Dereference vulnerability exists in numpy.sort, in the PyArray_DescrNew      |
        # | function due to missing return-value validation, which allows attackers to   |
        # | conduct DoS attacks by repetitively creating sort arrays.                    |
        # | https://github.com/numpy/numpy/issues/19038                                  |
    ]
    ignores_str = " ".join([f"-i {i}" for i in ignores])
    ctx.run(f"safety check --full-report {ignores_str}")


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
