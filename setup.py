#!/usr/bin/env python3
import os, setuptools

setup_path = os.path.dirname(__file__)

with open(os.path.join(setup_path, "README.md")) as readme:
    long_description = readme.read()

setuptools.setup(
    name="scielo_core",
    version="0.1",
    author="SciELO Dev Team",
    author_email="scielo-dev@googlegroups.com",
    description="SciELO Core é o componente central da arquitetura de sistemas "
    "de informação da Metodologia SciELO a partir de 2022. "
    "É responsável pela gestão de ID.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    license="BSD 3-Clause License",
    packages=setuptools.find_packages(
        exclude=["*.tests", "*.tests.*", "tests.*", "tests", "docs"]
    ),
    include_package_data=False,
    python_requires=">=3.7",
    install_requires=[
        "packtools",
        "mongoengine",
        "celery",
    ],
    test_suite="tests",
    classifiers=[
        "Environment :: Other Environment",
        "License :: OSI Approved :: BSD License",
        "Programming Language :: Python :: 3 :: Only",
        "Operating System :: OS Independent",
    ],
    entry_points={
        "console_scripts": ["scielo_core_loader = scielo_core.id_provider.cli:main"],
    },
)

