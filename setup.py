from setuptools import setup, find_packages
from pathlib import Path

long_description = (Path(__file__).parent / "README.md").read_text(encoding="utf-8")

setup(
    name="world-of-taxanomy",
    version="0.2.0",
    packages=find_packages(exclude=["tests*", "frontend*", "data*"]),
    python_requires=">=3.9",
    install_requires=[
        "asyncpg>=0.29.0",
        "fastapi>=0.110.0",
        "uvicorn[standard]>=0.29.0",
        "python-dotenv>=1.0.0",
        "bcrypt>=4.0.0",
        "PyJWT>=2.8.0",
        "slowapi>=0.1.9",
        "openpyxl>=3.1.0",
        "xlrd>=2.0.0",
        "pycountry>=22.3.5",
        "requests>=2.31.0",
    ],
    extras_require={
        "dev": [
            "pytest>=8.0.0",
            "pytest-asyncio>=0.23.0",
            "twine>=5.0.0",
            "build>=1.0.0",
        ]
    },
    description="36-system open taxonomy knowledge graph: industry, geography, product, occupation, education, health, regulation - all connected by crosswalk edges.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="ramdhanyk",
    author_email="",
    url="https://github.com/colaberry/WorldOfTaxanomy",
    project_urls={
        "Documentation": "https://worldoftaxanomy.com",
        "Source": "https://github.com/colaberry/WorldOfTaxanomy",
        "Tracker": "https://github.com/colaberry/WorldOfTaxanomy/issues",
    },
    license="MIT",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "Intended Audience :: Financial and Insurance Industry",
        "Intended Audience :: Healthcare Industry",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Topic :: Scientific/Engineering :: Information Analysis",
        "Topic :: Database",
        "Topic :: Internet :: WWW/HTTP :: HTTP Servers",
        "Topic :: Office/Business",
        "Operating System :: OS Independent",
        "Natural Language :: English",
    ],
    keywords=[
        "taxonomy", "classification", "industry", "NAICS", "ISIC", "NACE", "SIC",
        "knowledge-graph", "crosswalk", "MCP", "fastapi", "open-data",
        "HS", "CPC", "UNSPSC", "SOC", "ISCO", "ESCO", "CIP", "ATC", "ICD",
        "patent-cpc", "FMCSA", "GDPR", "ISO-31000", "occupational-classification",
        "product-classification", "trade-classification", "open-taxonomy",
    ],
    entry_points={
        "console_scripts": [
            "world-of-taxanomy=world_of_taxanomy.__main__:main",
        ],
    },
    include_package_data=True,
    package_data={
        "world_of_taxanomy": ["schema.sql", "schema_auth.sql"],
    },
)
