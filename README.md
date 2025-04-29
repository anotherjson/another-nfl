# Scripts to take data from nfl_data_py and load it into a db

# Setup

Will need:

- [poetry](https://python-poetry.org/docs/#installation)
- pyenv

After confirm the above is installed, run the following:

```zsh
git clone git@github.com:anotherjson/another-nfl.git
cd another-nfl
pyenv local 3.9
poetry install
eval $(poetry env activate)
python src/main.py
```
