# SparkDB Open-Source and PyPI Release Guide

This guide provides the complete, step-by-step instructions to push **SparkDB** to GitHub as an open-source repository and publish it to **PyPI (Python Package Index)** so that anyone worldwide can install it via:

```bash
pip install sparkdb
```

---

## 1. Project & Contributor Metadata

* **Package Name**: `sparkdb`
* **Version**: `1.1.0`
* **License**: `BSD-3-Clause` (Permissive Open-Source)
* **Lead Author & Major Contributor**: Bommireddy Venkata Dheeraj Reddy ([bommireddyvenkatadheerajreddy@gmail.com](mailto:bommireddyvenkatadheerajreddy@gmail.com))
* **Maintainer & Contributor**: Ruthwik ([ruthwik4566@gmail.com](mailto:ruthwik4566@gmail.com))

---

## 2. Directory Structure of the Release Package

Before proceeding, ensure your working directory is `sparkdb_dev/`. All files are self-contained:

```
sparkdb_dev/
├── .gitignore                   <- Prevents temp files and build caches from being committed
├── LICENSE                      <- Official BSD-3-Clause License
├── pyproject.toml               <- Standard PEP 517/621 package metadata with contributors
├── README.md                    <- Public GitHub & PyPI landing documentation
├── OPEN_SOURCE_AND_PYPI_RELEASE_GUIDE.md  <- This guide
├── docs/                        <- Technical architecture, internals, and user guides
├── scripts/                     <- High-scale benchmarks and stress test scripts
├── tests/                       <- 39 unit and integration tests (100% passing)
├── docker/                      <- Dockerfile and docker-compose definitions
└── sparkdb/                     <- Core Python package source code
    ├── __init__.py
    ├── cli.py
    ├── algorithms/              <- Centrality, community, pathfinding, provenance
    ├── client/                  <- Remote and in-process SDK clients
    ├── core/                    <- MatrixStore, PropertyStore, VectorStore, Persistence
    ├── cypher/                  <- AST parser, two-tier LRU cache, CBO executor
    └── server/                  <- Multi-threaded HTTP daemon & MessagePack server
```

---

## 3. Phase 1: Push Code to GitHub

### Step 1.1: Initialize Git in `sparkdb_dev/`
Open your terminal and navigate to `sparkdb_dev`:

```bash
cd /home/ebomven/poc/sparkdb_dev

# Initialize a new Git repository
git init

# Configure your Git author info (if not already set)
git config user.name "Bommireddy Venkata Dheeraj Reddy"
git config user.email "bommireddyvenkatadheerajreddy@gmail.com"
```

### Step 1.2: Stage and Commit the Files
```bash
# Stage all sanitized files (ignoring temporary files via .gitignore)
git add .

# Verify status
git status

# Create the initial release commit
git commit -m "feat: Initial open-source release of SparkDB v1.1.0 (BSD-3-Clause)"
```

### Step 1.3: Create a Public Repository on GitHub
1. Log in to [GitHub](https://github.com).
2. Click the **+** icon in the top right and select **New repository**.
3. Repository Settings:
   - **Repository name**: `sparkdb` (or `sparkdb-graph`)
   - **Description**: `Fast GraphBLAS Sparse Linear Algebra Graph Database with Hybrid Storage, Cypher, and Vector Search`
   - **Visibility**: **Public**
   - **Initialize with**: Leave all unchecked (do NOT add README, .gitignore, or license since we already created them).
4. Click **Create repository**.

### Step 1.4: Push Local Repository to GitHub
Copy the commands shown on GitHub:

```bash
# Rename branch to main
git branch -M main

# Add your GitHub remote (replace <YOUR_GITHUB_USERNAME> with your actual username)
git remote add origin https://github.com/<YOUR_GITHUB_USERNAME>/sparkdb.git

# Push to GitHub
git push -u origin main
```

---

## 4. Phase 2: PyPI Account Setup & API Token

### Step 2.1: Register on PyPI
1. Go to [https://pypi.org/account/register/](https://pypi.org/account/register/).
2. Verify your email address.

### Step 2.2: Enable Two-Factor Authentication (2FA)
PyPI **requires** 2FA for all package uploads:
1. Navigate to **Account Settings** → **Two-factor authentication (2FA)**.
2. Setup an authenticator app (Google Authenticator, Microsoft Authenticator, 1Password, or Authy).
3. Save your recovery codes in a safe place.

### Step 2.3: Generate an API Token
1. In PyPI, go to **Account Settings** → scroll to **API tokens**.
2. Click **Add API token**.
3. Settings:
   - **Token name**: `sparkdb-release-token`
   - **Scope**: Select **Entire account (all projects)** *(for the first upload, since the `sparkdb` project does not exist yet on PyPI)*.
4. Click **Create token**.
5. **Copy the token immediately!** It starts with:
   ```
   pypi-AgEIcHlwaS5vcmc...
   ```
   *(You will not be able to view it again after closing the page).*

---

## 5. Phase 3: Building the Release Package

### Step 3.1: Install Standard Packaging Tools
Ensure `build` and `twine` are installed in your Python environment:

```bash
pip install --upgrade build twine
```

### Step 3.2: Clean Previous Artifacts and Build
Run the clean build command from `sparkdb_dev/`:

```bash
cd /home/ebomven/poc/sparkdb_dev

# Clean any existing build artifacts
rm -rf dist/ build/ *.egg-info/

# Build both Source Distribution (.tar.gz) and Binary Wheel (.whl)
python3 -m build
```

This will produce two files inside `dist/`:
```
dist/
├── sparkdb-1.1.0-py3-none-any.whl    <- Pre-compiled wheel for instant pip install
└── sparkdb-1.1.0.tar.gz              <- Full source distribution archive
```

### Step 3.3: Validate the Distribution with `twine check`
Verify that package metadata and Markdown descriptions render cleanly without syntax errors:

```bash
twine check dist/*
```

Expected output:
```
Checking dist/sparkdb-1.1.0-py3-none-any.whl: PASSED
Checking dist/sparkdb-1.1.0.tar.gz: PASSED
```

---

## 6. Phase 4: (Optional Recommended) Test on TestPyPI

Before pushing to the global PyPI, you can test your upload on the sandbox TestPyPI registry.

1. Create an account on [https://test.pypi.org/](https://test.pypi.org/) and generate a TestPyPI token.
2. Upload to TestPyPI:
   ```bash
   twine upload --repository testpypi dist/*
   ```
   - Username: `__token__`
   - Password: `<your-testpypi-token>`
3. Test installation in a clean environment:
   ```bash
   pip install --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple/ sparkdb
   ```

---

## 7. Phase 5: Publishing to Production PyPI

### Step 7.1: Upload with Twine
Publish directly to the official PyPI registry:

```bash
twine upload dist/*
```

When prompted:
* **Enter your username**: Type `__token__`
* **Enter your password**: Paste your PyPI API token (`pypi-AgEIcHlwaS5vcmc...`)

*(Tip: You won't see characters appear while pasting the token in the terminal — just paste and press Enter).*

### Step 7.2: Automated `.pypirc` Configuration (Alternative)
To avoid manually pasting the token every time, create `~/.pypirc` on your machine:

```ini
[distutils]
index-servers =
    pypi

[pypi]
username = __token__
password = pypi-AgEIcHlwaS5vcmcYOUR_ACTUAL_TOKEN_HERE
```
Secure the file permissions:
```bash
chmod 600 ~/.pypirc
```
Now you can simply run `twine upload dist/*` without being prompted.

---

## 8. Phase 6: Verifying Global Installation

Within 1–2 minutes of uploading:

1. Visit your public PyPI project page:
   [https://pypi.org/project/sparkdb/](https://pypi.org/project/sparkdb/)
2. Verify that **Bommireddy Venkata Dheeraj Reddy** and **Ruthwik** are listed under **Author** and **Maintainer**.
3. In any terminal or clean Python environment:
   ```bash
   pip install sparkdb
   ```
4. Verify import:
   ```python
   import sparkdb
   print(sparkdb.__version__)  # Output: 1.1.0

   from sparkdb import SparkDB
   db = SparkDB()
   g = db.select_project("test_graph")
   res = g.query("CREATE (n:Entity {name: 'Test'}) RETURN n.name")
   print("Query Result:", res.result_set)
   ```

---

## 9. Phase 7: Automate Future Releases with GitHub Actions

You can configure GitHub to automatically build and publish to PyPI every time you create a release tag (`v1.1.1`, `v1.2.0`, etc.).

Create the file `.github/workflows/publish.yml` in your repository:

```yaml
name: Publish SparkDB to PyPI

on:
  release:
    types: [published]

jobs:
  pypi-publish:
    name: Build and upload to PyPI
    runs-on: ubuntu-latest
    permissions:
      id-token: write  # Mandatory for PyPI Trusted Publishing

    steps:
      - name: Checkout Source Code
        uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install Build Tools
        run: pip install --upgrade build twine

      - name: Build Wheel and Source Tarball
        run: python -m build

      - name: Publish Package to PyPI
        uses: pypa/gh-action-pypi-publish@release/v1
        with:
          password: ${{ secrets.PYPI_API_TOKEN }}
```

In your GitHub repository:
1. Go to **Settings** → **Secrets and variables** → **Actions**.
2. Click **New repository secret**.
3. Name: `PYPI_API_TOKEN`
4. Value: Paste your `pypi-AgEIcHlwaS5vcmc...` token.

---

## 10. Managing Future Version Bumps

When you make new features or optimizations:
1. Update `version = "1.1.1"` in `sparkdb_dev/pyproject.toml`.
2. Update `__version__ = "1.1.1"` in `sparkdb/__init__.py`.
3. Rebuild and upload:
   ```bash
   rm -rf dist/ build/
   python -m build
   twine upload dist/*
   ```
4. Commit and push the tag to GitHub:
   ```bash
   git add pyproject.toml sparkdb/__init__.py
   git commit -m "chore: release version 1.1.1"
   git tag v1.1.1
   git push origin main --tags
   ```

---

## 11. Support and Contact

For questions regarding releases or security advisories:
* **Lead Author**: Bommireddy Venkata Dheeraj Reddy — `bommireddyvenkatadheerajreddy@gmail.com`
* **Maintainer**: Ruthwik — `ruthwik4566@gmail.com`
