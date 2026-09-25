"""Sphinx configuration for the peQL documentation site."""

from datetime import date

project = "peQL"
copyright = f"{date.today().year}, Griot Data Technologies"
author = "Griot Data Technologies"
release = "0.4.0"

extensions = ["myst_parser", "sphinx_design"]
source_suffix = {".md": "markdown"}
exclude_patterns = ["_build"]

myst_enable_extensions = ["colon_fence", "deflist", "fieldlist"]
myst_heading_anchors = 3

html_theme = "shibuya"
html_title = "peQL: query contracts, not tables"
html_static_path = ["_static"]
html_css_files = ["peql.css"]
html_theme_options = {
    "github_url": "https://github.com/griot-cloud/peql",
    "nav_links": [
        {"title": "GitHub", "url": "https://github.com/griot-cloud/peql", "external": True},
    ],
}
