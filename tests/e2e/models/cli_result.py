import re
from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class ValidationMessage:
    """A single message from modelcat_validate output."""
    type: str  # "error", "warning", "note", "critical"
    message: str


@dataclass
class CLIResult:
    """
    Parsed result of a modelcat_validate CLI invocation.

    Provides structured access to exit code, stdout/stderr,
    parsed messages, and the dataset signature if validation passed.
    """
    exit_code: int
    stdout: str
    stderr: str
    messages: List[ValidationMessage] = field(default_factory=list)
    signature: Optional[str] = None

    def __post_init__(self):
        self.messages = self._parse_messages()
        self.signature = self._parse_signature()

    def _parse_messages(self) -> List[ValidationMessage]:
        """Extract typed messages from the Messages section of stdout."""
        results = []
        pattern = re.compile(
            r"^(ERROR|WARNING|NOTE|CRITICAL):\s+(.+)$",
            re.MULTILINE,
        )
        for match in pattern.finditer(self.stdout):
            results.append(
                ValidationMessage(
                    type=match.group(1).lower(),
                    message=match.group(2).strip(),
                )
            )
        return results

    def _parse_signature(self) -> Optional[str]:
        """Extract the dataset signature hash if validation passed."""
        match = re.search(
            r"Validation passed and signed:\s+([a-f0-9]{64})",
            self.stdout,
        )
        return match.group(1) if match else None

    @property
    def errors(self) -> List[ValidationMessage]:
        return [m for m in self.messages if m.type == "error"]

    @property
    def warnings(self) -> List[ValidationMessage]:
        return [m for m in self.messages if m.type == "warning"]

    @property
    def notes(self) -> List[ValidationMessage]:
        return [m for m in self.messages if m.type == "note"]

    @property
    def error_count(self) -> int:
        return len(self.errors)

    @property
    def warning_count(self) -> int:
        return len(self.warnings)

    @property
    def passed(self) -> bool:
        return self.signature is not None

    def has_error(self, substring: str) -> bool:
        """Check if any error message contains the given substring."""
        return any(substring.lower() in e.message.lower() for e in self.errors)

    def has_warning(self, substring: str) -> bool:
        """Check if any warning message contains the given substring."""
        return any(substring.lower() in w.message.lower() for w in self.warnings)

    def has_message(self, substring: str) -> bool:
        """Check if any message (any type) contains the given substring."""
        return any(substring.lower() in m.message.lower() for m in self.messages)

    def stdout_contains(self, substring: str) -> bool:
        return substring.lower() in self.stdout.lower()

    def stderr_contains(self, substring: str) -> bool:
        return substring.lower() in self.stderr.lower()

    def output_contains(self, substring: str) -> bool:
        """Check if stdout OR stderr contains the substring."""
        return self.stdout_contains(substring) or self.stderr_contains(substring)

    @property
    def crashed(self) -> bool:
        """True if the CLI exited with a traceback (unhandled exception)."""
        return "traceback" in self.stderr.lower()
