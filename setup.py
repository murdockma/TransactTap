from setuptools import setup, find_packages

setup(
    name="finance-pipeline",
    version="0.1.0",
    packages=find_packages(),
    package_data={
        "": ["*.yaml", "*.json"],
    },
    install_requires=[
        "selenium>=4.9.0",
        "pandas>=2.0.0",
        "google-cloud-bigquery>=3.11.0",
        "pandas-gbq>=0.19.0",
        "python-dotenv>=1.0.0",
        "webdriver-manager>=4.0.0",
        "pyyaml>=6.0",
        "requests>=2.31.0",
    ],
    python_requires=">=3.9",
)
