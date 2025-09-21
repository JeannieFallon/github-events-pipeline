if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    echo "Script must be sourced to persist venv!"
    echo "Usage: . setup.sh"
    exit 1
fi

IS_NEW=false

# Create .venv if not present
if [[ ! -d ".venv" ]]; then
    echo "Creating .venv . . ."
    python3 -m venv .venv
    IS_NEW=true
fi

# Activate venv
source .venv/bin/activate

# Install dependencies
if ${IS_NEW}; then
    echo "Installing Python pacakges . . ."
    python -m pip install -U pip
    pip install -r requirements.txt
fi

echo "Set-up complete!"
