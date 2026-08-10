"""KMTech factory desktop/web compatibility contracts.

The package deliberately contains contract and verification code only.  It is
safe to vendor into a frozen desktop application because it does not open a
database, contact a server, or mutate installer state during import.
"""

from .bundle import (
    CONTRACT_BUNDLE_VERSION,
    CONTRACT_BUNDLE_SHA256,
    bundle_root,
    load_contract_document,
    verify_bundled_contracts,
)
from .errors import FactoryContractError
from .lock import load_and_verify_contract_lock

__all__ = [
    "CONTRACT_BUNDLE_SHA256",
    "CONTRACT_BUNDLE_VERSION",
    "FactoryContractError",
    "bundle_root",
    "load_contract_document",
    "load_and_verify_contract_lock",
    "verify_bundled_contracts",
]
