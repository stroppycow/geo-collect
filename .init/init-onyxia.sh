#!/bin/bash


python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
poetry install


# Path to the VSCode settings.json file
SETTINGS_FILE="${HOME}/.local/share/code-server/User/settings.json"

# Check if the settings.json file exists, otherwise create a new one
if [ ! -f "$SETTINGS_FILE" ]; then
    echo "No existing settings.json found. Creating a new one."
    mkdir -p "$(dirname "$SETTINGS_FILE")"
    echo "{}" > "$SETTINGS_FILE"  # Initialize with an empty JSON object
fi

# Add or modify Python-related settings using jq
# We will keep the comments outside the jq block, as jq doesn't support comments inside JSON.
jq '. + {
    "workbench.colorTheme": "Default Dark Modern",  # Set the theme
    "python.defaultInterpreterPath": "${workspaceFolder}/.venv/bin/python",
    "python.terminal.activateEnvironment": true,
    "editor.rulers": [80, 100, 120],  # Add specific vertical rulers
    "files.trimTrailingWhitespace": true, # Automatically trim trailing whitespace
    "files.insertFinalNewline": true, # Ensure files end with a newline
    "flake8.args": [
        "--max-line-length=100" # Max line length for Python linting
    ],
    "[r]": {
        "editor.formatOnSave": true
    },
    "[python]": {
        "editor.defaultFormatter": "charliermarsh.ruff",
        "editor.formatOnSave": true,
        "editor.codeActionsOnSave": {
            "source.fixAll.ruff": "always",
            "source.organizeImports.ruff": "always"
        }
    }
}' "$SETTINGS_FILE" > "$SETTINGS_FILE.tmp" && mv "$SETTINGS_FILE.tmp" "$SETTINGS_FILE"

curl -LsSf https://mistral.ai/vibe/install.sh | bash

echo 'export PATH="/home/onyxia/.local/bin:$PATH"' >> ~/.bashrc



