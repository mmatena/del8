"""TODO: Add title."""
import os

# Use a UUID to lower chance of accidently having the delimiters in
# other parts of stdout.
START_LOGS = "[START LOGS] ec1f15469a7848a589ef25fbcfdb000f"
END_LOGS = "[END LOGS] 49cc9ad1454748a3a95b6a82cecde886"


MAIN = os.path.join(os.path.dirname(__file__), "main.py")


def create_print_str(logs_str):
    return "\n".join(
        [
            START_LOGS,
            logs_str,
            END_LOGS,
        ]
    )


def extract_from_stdout(stdout):
    stdout = stdout.decode("utf-8")
    lines = stdout.split("\n")
    for i, line in enumerate(lines):
        if line == START_LOGS:
            assert lines[i + 2] == END_LOGS
            return lines[i + 1]
    raise ValueError(f"No logs found in {stdout}.")
