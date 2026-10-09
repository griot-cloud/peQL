"""Sphinx configuration for the peQL documentation site."""

from datetime import date
from pathlib import Path
import tomllib

project = "peQL"
copyright = f"{date.today().year}, Griot Data Technologies"
author = "Griot Data Technologies"
with (Path(__file__).resolve().parent.parent / "Cargo.toml").open("rb") as manifest:
    release = tomllib.load(manifest)["package"]["version"]

extensions = ["myst_parser", "sphinx_design"]
source_suffix = {".md": "markdown"}
exclude_patterns = ["_build", "parcel-README.md", "peql-README.md"]

myst_enable_extensions = ["colon_fence", "deflist", "fieldlist"]
myst_heading_anchors = 3

html_theme = "shibuya"
html_title = "peQL: policy enforcing query engine"
html_static_path = ["_static"]
html_css_files = ["griot.css"]
html_theme_options = {
    "accent_color": "blue",
    "github_url": "https://github.com/griot-cloud/peql",
    "nav_links": [
        {"title": "parcel", "url": "https://griot-cloud.github.io/parcel/", "external": True},
        {"title": "peQL", "url": "https://griot-cloud.github.io/peQL/", "external": True},
        {"title": "Moruna", "url": "https://griot-cloud.github.io/moruna/", "external": True},
        {"title": "GitHub", "url": "https://github.com/griot-cloud/peQL", "external": True},
    ],
}
