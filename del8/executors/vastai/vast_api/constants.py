"""Constants for the vast API.

Putting them in separate file makes stuff cleaner and more readable.
"""

SERVER_URL_DEFAULT = "https://vast.ai/api/v0"

API_KEY_FILE_BASE = "~/.vast_api_key"

FIELD_ALIASES = {
    "cuda_vers": "cuda_max_good",
    "reliability": "reliability2",
    "dlperf_usd": "dlperf_per_dphtotal",
    "dph": "dph_total",
    "flops_usd": "flops_per_dphtotal",
    "display_active": "gpu_display_active",
}

OP_NAMES = {
    ">=": "gte",
    ">": "gt",
    "gt": "gt",
    "gte": "gte",
    "<=": "lte",
    "<": "lt",
    "lt": "lt",
    "lte": "lte",
    "!=": "neq",
    "==": "eq",
    "=": "eq",
    "eq": "eq",
    "neq": "neq",
    "noteq": "neq",
    "not eq": "neq",
    "notin": "notin",
    "not in": "notin",
    "nin": "notin",
    "in": "in",
}

FIELD_MULTIPLIERS = {
    "cpu_ram": 1000,
    "gpu_ram": 1000,
    "duration": 1.0 / (24.0 * 60.0 * 60.0),
}

FIELDS = {
    "compute_cap",
    "cpu_cores",
    "cpu_cores_effective",
    "cpu_ram",
    "cuda_max_good",
    "disk_bw",
    "disk_space",
    "dlperf",
    "dlperf_per_dphtotal",
    "dph_total",
    "duration",
    "external",
    "flops_per_dphtotal",
    "gpu_display_active",
    # "gpu_ram_free_min",
    "gpu_mem_bw",
    "gpu_name",
    "gpu_ram",
    "has_avx",
    "host_id",
    "id",
    "inet_down",
    "inet_down_cost",
    "inet_up",
    "inet_up_cost",
    "min_bid",
    "mobo_name",
    "num_gpus",
    "pci_gen",
    "pcie_bw",
    "reliability2",
    "rentable",
    "rented",
    "storage_cost",
    "total_flops",
    "verified",
}
