# Scripts to take data from nfl_data_py and load it into a db

# Setup

Will need:

- [uv](https://docs.astral.sh/uv/getting-started/installation)
- An install of postgresql

After confirm the above is installed, run the following:

```zsh
git clone git@github.com:anotherjson/another-nfl.git
cd another-nfl
uv sync
uv run src/main.py
```
