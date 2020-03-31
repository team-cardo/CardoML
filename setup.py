from setuptools import find_packages, setup

setup(
    name='CardoML',
    version='1.0.3',
    packages=find_packages(),
    url='',
    license='',
    author='CardoTeam',
    author_email='',
    description='',
    python_requires='>=3.7',
    install_requires=[
        'CardoExecutor',
        'python_rapidjson',
        'scipy',
        'matplotlib',
        'seaborn',
        'pandas',
        'numpy'
    ],
)
