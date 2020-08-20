Before test:

1. build package and install it locally

# Steps to create a virtual environment for testing
```
cd test
mkdir .venv
python3 -m venv .venv
source .venv/bin/activate
pip install pip setuptools --upgrade
pip install wheel
```

Then install the package locally
```bash
# assuming your pyschema project is at ~/projects/pyschema
pip install -e ~/projects/pyschema
```
