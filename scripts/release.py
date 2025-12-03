#!/usr/bin/env python3
"""Release management script for SLURM Emulator.

This script helps manage releases by:
- Updating version number in pyproject.toml (single source of truth)
- Creating git tags that trigger CI/CD
- Running local pre-release checks
- Building test packages locally

All code references to version are automatically updated since they import
from the central version source in emulator/__init__.py.
"""

import re
import subprocess
import sys
from pathlib import Path

# Script metadata for inline dependencies
# /// script
# dependencies = ["click>=8.0.0"]
# ///
import click


def run_command(cmd: list[str], check: bool = True) -> subprocess.CompletedProcess:
    """Run a shell command and return the result."""
    print(f"Running: {' '.join(cmd)}")
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=check)
        if result.stdout:
            print(result.stdout)
        return result
    except subprocess.CalledProcessError as e:
        print(f"Error running command: {e}")
        if e.stderr:
            print(f"Error output: {e.stderr}")
        if check:
            sys.exit(1)
        return e


def get_current_version() -> str:
    """Get current version from pyproject.toml."""
    pyproject_path = Path("pyproject.toml")
    if not pyproject_path.exists():
        print("Error: pyproject.toml not found")
        sys.exit(1)

    content = pyproject_path.read_text()
    match = re.search(r'version\s*=\s*"([^"]+)"', content)
    if not match:
        print("Error: Could not find version in pyproject.toml")
        sys.exit(1)

    return match.group(1)


def update_version(new_version: str) -> None:
    """Update version in pyproject.toml."""
    pyproject_path = Path("pyproject.toml")
    content = pyproject_path.read_text()

    # Replace version line in [project] section only
    updated_content = re.sub(
        r'(\[project\].*?\nversion\s*=\s*)"[^"]+"', f'\\1"{new_version}"', content, flags=re.DOTALL
    )

    if updated_content == content:
        print("Error: Could not update version in pyproject.toml")
        sys.exit(1)

    pyproject_path.write_text(updated_content)
    print(f"Updated version to {new_version} in pyproject.toml")


def validate_version(version: str) -> bool:
    """Validate version format (semantic versioning)."""
    pattern = r"^\d+\.\d+\.\d+(?:-[a-zA-Z0-9-]+)?(?:\+[a-zA-Z0-9-]+)?$"
    return bool(re.match(pattern, version))


def check_git_status() -> None:
    """Check if git working directory is clean."""
    result = run_command(["git", "status", "--porcelain"], check=False)
    if result.returncode != 0:
        print("Error: Not in a git repository")
        sys.exit(1)

    if result.stdout.strip():
        print("Warning: Git working directory is not clean:")
        print(result.stdout)
        if not click.confirm("Continue with uncommitted changes?"):
            sys.exit(1)


def run_pre_release_checks() -> None:
    """Run basic local checks before release (full testing is done in CI)."""
    print("Running local pre-release checks...")

    # Run basic linting
    print("Running linter...")
    run_command(["uv", "run", "ruff", "check", "emulator/"])

    # Run type checking
    print("Running type check...")
    run_command(["uv", "run", "mypy", "emulator/"])

    print("Local pre-release checks passed!")
    print("Note: Full testing is done automatically in GitHub Actions")


def build_package() -> None:
    """Build distribution packages locally (for testing - CI handles actual releases)."""
    print("Building distribution packages locally...")
    run_command(["uv", "build"])
    print("Local build completed successfully!")
    print("Note: Production builds and PyPI publishing are handled by GitHub Actions")


def create_git_tag(version: str) -> None:
    """Create and push git tag (triggers GitHub Actions for PyPI publishing)."""
    tag_name = f"{version}"  # GitHub Actions expects tags like "0.1.1" not "v0.1.1"

    # Create tag
    run_command(["git", "add", "pyproject.toml"])
    run_command(["git", "commit", "-m", f"Release version {version}"])
    run_command(["git", "tag", "-a", tag_name, "-m", f"Release {version}"])

    print(f"Created git tag: {tag_name}")
    print("This tag will trigger GitHub Actions to:")
    print("  - Run full test suite on multiple Python versions")
    print("  - Build and publish to PyPI automatically")

    if click.confirm("Push tag to remote (this will trigger PyPI release)?"):
        run_command(["git", "push"])
        run_command(["git", "push", "--tags"])
        print("Pushed tag to remote - check GitHub Actions for release progress")


@click.group()
def cli():
    """SLURM Emulator Release Management."""


@cli.command()
def status():
    """Show current release status."""
    current_version = get_current_version()
    print(f"Current version: {current_version}")

    # Check for existing tags
    result = run_command(["git", "tag", "--list"], check=False)
    if result.returncode == 0 and result.stdout:
        print("Existing tags:")
        for tag in result.stdout.strip().split("\n"):
            print(f"  {tag}")
    else:
        print("No tags found")


@cli.command()
@click.argument("version")
@click.option("--skip-checks", is_flag=True, help="Skip local pre-release checks")
@click.option("--skip-build", is_flag=True, help="Skip local build test")
@click.option("--skip-tag", is_flag=True, help="Skip creating git tag (no PyPI release)")
def release(version: str, skip_checks: bool, skip_build: bool, skip_tag: bool):
    """Create a new release - updates version and optionally creates git tag for CI/CD."""
    current_version = get_current_version()

    if not validate_version(version):
        print(f"Error: Invalid version format: {version}")
        print("Version should follow semantic versioning (e.g., 1.0.0)")
        sys.exit(1)

    print(f"Creating release {version} (current: {current_version})")

    if version == current_version:
        print("Error: New version is same as current version")
        sys.exit(1)

    # Check git status
    check_git_status()

    # Run pre-release checks
    if not skip_checks:
        run_pre_release_checks()

    # Update version
    update_version(version)

    # Build package
    if not skip_build:
        build_package()

    # Create git tag
    if not skip_tag:
        create_git_tag(version)

    print(f"✅ Successfully created release {version}")


@cli.command()
@click.argument("version")
def version_update(version: str):
    """Update version in pyproject.toml without creating a release."""
    if not validate_version(version):
        print(f"Error: Invalid version format: {version}")
        sys.exit(1)

    current_version = get_current_version()
    print(f"Updating version from {current_version} to {version}")

    update_version(version)
    print("Version updated successfully")


@cli.command()
def build():
    """Build distribution packages locally (for testing only)."""
    build_package()


@cli.command()
def check():
    """Run local pre-release checks (linting, type checking)."""
    run_pre_release_checks()


if __name__ == "__main__":
    cli()
