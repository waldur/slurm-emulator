#!/usr/bin/env python3
"""Release management script for SLURM Emulator.

This script helps manage releases by:
- Updating version number in pyproject.toml (single source of truth) and mirroring
  it into charts/slurm-emulator/Chart.yaml
- Creating git tags that trigger CI/CD
- Running local pre-release checks
- Building test packages locally

All code references to version are automatically updated since they import
from the central version source in emulator/__init__.py.
"""

import re
import shutil
import subprocess
import sys
from pathlib import Path

# Script metadata for inline dependencies
# /// script
# dependencies = ["click>=8.0.0"]
# ///
import click

CHART_YAML = Path(__file__).resolve().parent.parent / "charts" / "slurm-emulator" / "Chart.yaml"


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


def update_chart_version(new_version: str) -> None:
    """Mirror the release version into Chart.yaml's version AND appVersion.

    ``version`` is the chart version; ``appVersion`` is the image tag the chart
    deploys (templates/_helpers.tpl defaults image.tag to .Chart.AppVersion),
    and CI pushes opennode/slurm-emulator:<tag> from the same pipeline. CI
    rewrites both at publish time anyway — doing it here keeps the committed
    chart honest about which image it installs.
    """
    if not CHART_YAML.exists():
        print(f"Warning: {CHART_YAML} not found, skipping chart version update")
        return

    lines = CHART_YAML.read_text().splitlines(keepends=True)
    updated_lines = []
    found_version = found_app = False
    for line in lines:
        if line.startswith("version:"):
            updated_lines.append(f"version: {new_version}\n")
            found_version = True
        elif line.startswith("appVersion:"):
            updated_lines.append(f'appVersion: "{new_version}"\n')
            found_app = True
        else:
            updated_lines.append(line)

    if not found_version or not found_app:
        missing = "version:" if not found_version else "appVersion:"
        print(f"Error: Could not find {missing} line in Chart.yaml")
        sys.exit(1)

    CHART_YAML.write_text("".join(updated_lines))
    print(f"Updated Chart.yaml -> version: {new_version}, appVersion: {new_version}")


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

    # Chart lint/unittest mirror the "Lint Helm chart" CI job. run_command only
    # handles a non-zero exit; a missing binary raises FileNotFoundError before
    # there is an exit code at all, so check for helm up front.
    if shutil.which("helm") is None:
        print("Note: helm not found on PATH, skipping chart lint and unit tests.")
        print("      CI still gates them in the 'Lint Helm chart' job.")
        if not click.confirm("Continue without the chart checks?"):
            sys.exit(1)
    else:
        print("Linting Helm chart...")
        run_command(["helm", "lint", str(CHART_YAML.parent)])

        print("Running Helm chart unit tests...")
        result = run_command(["helm", "unittest", str(CHART_YAML.parent)], check=False)
        if result.returncode != 0:
            print(
                "Note: `helm unittest` requires the helm-unittest plugin. Install with:\n"
                "  helm plugin install https://github.com/helm-unittest/helm-unittest.git --version v0.8.2"
            )
            if not click.confirm("Skip helm unittest and continue?"):
                sys.exit(1)

    print("Local pre-release checks passed!")
    print("Note: Full testing is done automatically in GitLab CI/CD")


def build_package() -> None:
    """Build distribution packages locally (for testing - CI handles actual releases)."""
    print("Building distribution packages locally...")
    run_command(["uv", "build"])
    print("Local build completed successfully!")
    print("Note: Production builds and PyPI publishing are handled by GitLab CI/CD")


def generate_changelog(version: str) -> None:
    """Generate changelog entry for the release."""
    script = Path(__file__).parent / "changelog.sh"
    if not script.exists():
        print(f"Warning: {script} not found, skipping changelog generation")
        return

    print(f"Generating changelog entry for {version}...")
    result = subprocess.run(
        ["bash", str(script), version],
        check=False,
    )
    if result.returncode != 0:
        print("Warning: Changelog generation failed or was aborted")
        if not click.confirm("Continue release without changelog update?"):
            sys.exit(1)


def create_git_tag(version: str) -> None:
    """Create and push git tag (triggers GitLab CI/CD for PyPI publishing)."""
    tag_name = f"{version}"  # GitLab CI/CD expects tags like "0.1.1" not "v0.1.1"

    # Create tag
    run_command(["git", "add", "pyproject.toml", "CHANGELOG.md", str(CHART_YAML)])
    run_command(["git", "commit", "-m", f"Release version {version}"])
    run_command(["git", "tag", "-a", tag_name, "-m", f"Release {version}"])

    print(f"Created git tag: {tag_name}")
    print("This tag will trigger GitLab CI/CD to:")
    print("  - Run full test suite on multiple Python versions")
    print("  - Build and publish to PyPI automatically")

    if click.confirm("Push tag to remote (this will trigger PyPI release)?"):
        run_command(["git", "push"])
        run_command(["git", "push", "--tags"])
        print("Pushed tag to remote - check GitLab CI/CD for release progress")


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
@click.option("--skip-changelog", is_flag=True, help="Skip changelog generation")
@click.option("--skip-tag", is_flag=True, help="Skip creating git tag (no PyPI release)")
def release(version: str, skip_changelog: bool, skip_tag: bool):
    """Create a new release - updates version, generates changelog, and creates git tag.

    Building, testing, and publishing are handled by GitLab CI/CD.
    """
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

    # Update version
    update_version(version)
    update_chart_version(version)

    # Generate changelog
    if not skip_changelog:
        generate_changelog(version)

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
    update_chart_version(version)
    print("Version updated successfully")


@cli.command()
def build():
    """Build distribution packages locally (for testing only)."""
    build_package()


@cli.command()
def check():
    """Run local pre-release checks (linting, type checking, Helm chart)."""
    run_pre_release_checks()


if __name__ == "__main__":
    cli()
