# ruff: noqa: INP001 -- a script for `waldur shell`, not a package module
"""Seed a Waldur dev stack for the HPC workspace demo (run via `waldur shell`).

  KC_PORT=8080 FIRECREST_PORT=8000 [HOMEPORT_URL=http://localhost:8001] \
  [AI_ASSISTANT_CREDS=path] \
    uv run waldur shell < seed_waldur.py

Idempotent: re-running updates what exists. Creates
  - Keycloak (realm kcrealm, client "waldur") as the "keycloak" identity
    provider, matching users by preferred_username -> username;
  - users alice and bob (they sign in through Keycloak);
  - an HPC centre offering (Marketplace.Slurm) connected to FireCREST,
    with partitions, QoS and a small software catalog;
  - two allocations whose backend_id is the Slurm account up.sh seeded:
    Climate (alice, bob) -> alloc_climate, Genomics (alice) -> alloc_genomics;
  - offering users so the cluster username is the Keycloak login name.
With AI_ASSISTANT_CREDS (lines "url: …", "token: …", "model: …") the AI
assistant is enabled against that OpenAI-compatible endpoint.
"""

import os
from pathlib import Path

import requests
from constance import config
from waldur_auth_social.models import IdentityProvider
from waldur_core.core.models import User
from waldur_core.permissions.enums import RoleEnum
from waldur_core.structure.models import Customer, Project
from waldur_mastermind.marketplace import models as m
from waldur_mastermind.marketplace.enums import SITE_AGENT_OFFERING, ResourceStates

KC_PORT = os.environ.get("KC_PORT", "8080")
FIRECREST_PORT = os.environ.get("FIRECREST_PORT", "8000")
FIRECREST_URL = f"http://localhost:{FIRECREST_PORT}"
REALM = f"http://localhost:{KC_PORT}/auth/realms/kcrealm"
CLIENT_SECRET = os.environ.get("WALDUR_CLIENT_SECRET", "waldur-poc-secret")

# --- Identity provider ------------------------------------------------------
discovery_url = f"{REALM}/.well-known/openid-configuration"
discovery = requests.get(discovery_url, timeout=10).json()
IdentityProvider.objects.update_or_create(
    provider="keycloak",
    defaults={
        "is_active": True,
        "client_id": "waldur",
        "client_secret": CLIENT_SECRET,
        "verify_ssl": False,
        "discovery_url": discovery_url,
        "userinfo_url": discovery["userinfo_endpoint"],
        "token_url": discovery["token_endpoint"],
        "auth_url": discovery["authorization_endpoint"],
        "logout_url": discovery.get("end_session_endpoint", ""),
        "issuer": discovery["issuer"],
        "jwks_url": discovery["jwks_uri"],
        "label": "HPC centre (Keycloak)",
        "user_field": "username",
        "user_claim": "preferred_username",
        "extra_scope": "",
    },
)
config.FIRECREST_ALLOWED_API_URLS = FIRECREST_URL
if os.environ.get("HOMEPORT_URL"):
    # Where the OIDC login returns to (one-time code exchanged by homeport).
    config.HOMEPORT_URL = os.environ["HOMEPORT_URL"]
print(f"identity provider keycloak -> {REALM}; FireCREST allowlist {FIRECREST_URL}")

# --- People and projects ------------------------------------------------------
users = {}
for name in ("alice", "bob"):
    user, _ = User.objects.update_or_create(
        username=name,
        defaults={
            "email": f"{name}@example.org",
            "first_name": name.capitalize(),
            "last_name": "Researcher",
            "is_active": True,
            "registration_method": "keycloak",
        },
    )
    user.set_unusable_password()
    user.save()
    users[name] = user

org, _ = Customer.objects.get_or_create(name="Research Institute")
centre, _ = Customer.objects.get_or_create(name="HPC Centre")
provider, _ = m.ServiceProvider.objects.get_or_create(customer=centre)
climate, _ = Project.objects.get_or_create(customer=org, name="Climate")
genomics, _ = Project.objects.get_or_create(customer=org, name="Genomics")
for project, members in ((climate, ("alice", "bob")), (genomics, ("alice",))):
    for name in members:
        project.add_user_or_skip(users[name], RoleEnum.PROJECT_MEMBER)

# --- Offering ----------------------------------------------------------------
category, _ = m.Category.objects.get_or_create(title="HPC")
offering, _ = m.Offering.objects.update_or_create(
    customer=centre,
    name="Emulator cluster",
    defaults={
        "type": SITE_AGENT_OFFERING,
        "category": category,
        "state": m.Offering.States.ACTIVE,
        "shared": True,
        "description": "SLURM cluster (slurm-emulator) reachable through FireCREST v2.",
    },
)
offering.plugin_options = {
    **(offering.plugin_options or {}),
    "firecrest_api_url": FIRECREST_URL,
    "firecrest_system_name": "emulator",
}
offering.save(update_fields=["plugin_options"])
plan, _ = m.Plan.objects.get_or_create(offering=offering, name="Default")

partitions = {
    "debug": {
        "max_time": 30,
        "default_time": 10,
        "max_nodes": 2,
        "max_cpus_per_node": 64,
        "max_mem_per_node": 256000,
        "cpu_arch": "x86_64/amd/zen3",
    },
    "batch": {
        "max_time": 2880,
        "default_time": 60,
        "max_nodes": 16,
        "max_cpus_per_node": 128,
        "max_mem_per_node": 512000,
        "cpu_arch": "x86_64/amd/zen3",
    },
    "gpu": {
        "max_time": 1440,
        "default_time": 60,
        "max_nodes": 4,
        "max_cpus_per_node": 72,
        "max_mem_per_node": 480000,
        "cpu_arch": "aarch64/neoverse_v2",
        "gpu_arch": "nvidia/cc90",
    },
}
qos_normal, _ = m.SlurmOfferingQoS.objects.get_or_create(
    offering=offering, name="normal", defaults={"description": "Default"}
)
qos_long, _ = m.SlurmOfferingQoS.objects.update_or_create(
    offering=offering,
    name="long",
    defaults={"description": "Long jobs", "max_time": 2880, "max_nodes": 4},
)
for name, fields in partitions.items():
    partition, _ = m.OfferingPartition.objects.update_or_create(
        offering=offering, partition_name=name, defaults=fields
    )
    m.SlurmPartitionQoS.objects.update_or_create(
        partition=partition, qos=qos_normal, defaults={"is_default": True}
    )
    if name == "batch":
        m.SlurmPartitionQoS.objects.get_or_create(partition=partition, qos=qos_long)

# --- Software catalog --------------------------------------------------------
catalog, _ = m.SoftwareCatalog.objects.get_or_create(
    name="EESSI",
    version="2023.06",
    defaults={"catalog_type": "binary_runtime", "source_url": "https://eessi.io"},
)
m.OfferingSoftwareCatalog.objects.get_or_create(offering=offering, catalog=catalog)
software = [
    (
        "GROMACS",
        "Molecular dynamics",
        [("2024.1", "foss-2023b", ["x86_64", "aarch64"]), ("2023.3", "foss-2023a", ["x86_64"])],
    ),
    ("OpenFOAM", "CFD toolbox", [("11", "foss-2023a", ["x86_64"])]),
    ("Python", "Python interpreter", [("3.11.5", "GCCcore-13.2.0", ["x86_64", "aarch64"])]),
    ("PyTorch", "Deep learning framework", [("2.1.2", "foss-2023a-CUDA-12.1.1", ["aarch64"])]),
    ("WRF", "Weather Research and Forecasting model", [("4.4.1", "foss-2022b-dmpar", ["x86_64"])]),
]
for name, description, versions in software:
    package, _ = m.SoftwarePackage.objects.get_or_create(
        catalog=catalog, name=name, defaults={"description": description}
    )
    for version, toolchain, archs in versions:
        sv, _ = m.SoftwareVersion.objects.get_or_create(
            package=package, version=version, module_version=f"{version}-{toolchain}"
        )
        for arch in archs:
            m.SoftwareTarget.objects.get_or_create(
                version=sv,
                target_type="cpu_architecture",
                target_name=arch,
                target_subtype="generic",
                defaults={"gpu_architectures": ["nvidia/cc90"] if "CUDA" in toolchain else []},
            )

# --- Allocations and offering users ----------------------------------------------
for project, account in ((climate, "alloc_climate"), (genomics, "alloc_genomics")):
    resource, _ = m.Resource.objects.update_or_create(
        offering=offering,
        backend_id=account,
        defaults={
            "project": project,
            "plan": plan,
            "name": f"{project.name} allocation",
            "state": ResourceStates.OK,
        },
    )
for name, user in users.items():
    m.OfferingUser.objects.update_or_create(
        offering=offering, user=user, defaults={"username": name}
    )
print("offering 'Emulator cluster' with allocations alloc_climate, alloc_genomics")

# --- AI assistant -----------------------------------------------------------
creds_path = os.environ.get("AI_ASSISTANT_CREDS")
if creds_path:
    creds = {}
    with Path(creds_path).open() as f:
        for line in f:
            key, _, value = line.partition(":")
            creds[key.strip()] = value.strip()
    config.AI_ASSISTANT_ENABLED = True
    config.AI_ASSISTANT_ENABLED_ROLES = "all"
    config.AI_ASSISTANT_BACKEND_TYPE = "openai"
    config.AI_ASSISTANT_API_URL = creds["url"]
    config.AI_ASSISTANT_API_TOKEN = creds["token"]
    config.AI_ASSISTANT_MODEL = creds["model"]
    print(f"AI assistant enabled: {creds['url']} model {creds['model']}")
