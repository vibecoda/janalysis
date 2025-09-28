#!/bin/bash

# Git Hooks Setup Script
# Run this script after cloning the repository to install git hooks

set -e  # Exit on any error

echo "🔧 Setting up git hooks..."

# Get the repository root directory
REPO_ROOT=$(git rev-parse --show-toplevel)
HOOKS_DIR="$REPO_ROOT/hooks"
GIT_HOOKS_DIR="$REPO_ROOT/.git/hooks"

# Check if hooks directory exists
if [ ! -d "$HOOKS_DIR" ]; then
    echo "❌ Error: hooks directory not found at $HOOKS_DIR"
    exit 1
fi

# Create .git/hooks directory if it doesn't exist
mkdir -p "$GIT_HOOKS_DIR"

# Install each hook from the hooks directory
echo "📁 Installing hooks from $HOOKS_DIR..."

hook_count=0
for hook_file in "$HOOKS_DIR"/*; do
    if [ -f "$hook_file" ]; then
        hook_name=$(basename "$hook_file")
        target_file="$GIT_HOOKS_DIR/$hook_name"
        
        echo "  ✓ Installing $hook_name"
        cp "$hook_file" "$target_file"
        chmod +x "$target_file"
        
        ((hook_count++))
    fi
done

if [ $hook_count -eq 0 ]; then
    echo "⚠️  No hooks found in $HOOKS_DIR"
else
    echo ""
    echo "✅ Successfully installed $hook_count git hook(s)!"
    echo ""
    echo "Git hooks are now active. Every commit will:"
    echo "  • Run all tests automatically"
    echo "  • Block commits if tests fail"
    echo "  • Ensure code quality is maintained"
fi

echo ""
echo "🚀 Repository setup complete!"