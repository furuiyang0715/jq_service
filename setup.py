# coding=utf8

from setuptools import (
    find_packages,
    setup,
)

version = '1.0.0'


def parse_requirements(filename):
    require = []
    with open(filename) as f:
        for line in f:
            require.append(line.split()[0])
    return require


requirements = parse_requirements("requirements.txt")


setup(
    name='update_jqdata',
    version=version,
    description='jingzhuan JQdata update program',
    packages=find_packages(exclude=[]),
    author='jingzhuan',
    package_data={'': ['*.*']},
    install_requires=requirements,
    zip_safe=False,
    # ext_modules=cythonize(ext_modules),
    ext_modules=[],
    entry_points={
        "console_scripts": [
            "update_jqdata = update.__main__:run"
        ]
    },
    classifiers=[
        'Programming Language :: Python',
        'Operating System :: Microsoft :: Windows',
        'Operating System :: Unix',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
)
