"""JVM-side runtime dependencies: the Iceberg Spark runtime and the Windows file system shim.

Jars are placed on the driver classpath from a local directory instead of being
resolved through ``spark.jars.packages``. That keeps runs offline after the first
setup, pins exactly which binaries are used, and avoids Spark's dependency
fetcher, which calls ``chmod`` through winutils.exe on Windows.
"""

from __future__ import annotations

import hashlib
import logging
import os
import shutil
import subprocess
import tempfile
import urllib.request
from dataclasses import dataclass
from importlib import resources
from pathlib import Path

logger = logging.getLogger(__name__)

SPARK_BINARY_VERSION = "4.0"
SCALA_BINARY_VERSION = "2.13"
ICEBERG_VERSION = "1.10.1"
MAVEN_CENTRAL = "https://repo1.maven.org/maven2"
LOCALFS_JAR = "icebergflow-localfs.jar"
LOCALFS_CLASS = "icebergflow.hadoop.PermissionlessLocalFileSystem"


@dataclass(frozen=True)
class MavenArtifact:
    group: str
    artifact: str
    version: str

    @property
    def filename(self) -> str:
        return f"{self.artifact}-{self.version}.jar"

    @property
    def url(self) -> str:
        group_path = self.group.replace(".", "/")
        return f"{MAVEN_CENTRAL}/{group_path}/{self.artifact}/{self.version}/{self.filename}"


ICEBERG_RUNTIME = MavenArtifact(
    "org.apache.iceberg",
    f"iceberg-spark-runtime-{SPARK_BINARY_VERSION}_{SCALA_BINARY_VERSION}",
    ICEBERG_VERSION,
)


def is_windows() -> bool:
    return os.name == "nt"


def required_jars(jars_dir: Path, windows: bool | None = None) -> list[Path]:
    windows = is_windows() if windows is None else windows
    jars = [jars_dir / ICEBERG_RUNTIME.filename]
    if windows:
        jars.append(jars_dir / LOCALFS_JAR)
    return jars


def missing_jars(jars_dir: Path, windows: bool | None = None) -> list[Path]:
    return [jar for jar in required_jars(jars_dir, windows) if not jar.is_file()]


def _sha1(path: Path) -> str:
    digest = hashlib.sha1(usedforsecurity=False)
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def fetch_artifact(artifact: MavenArtifact, jars_dir: Path) -> Path:
    target = jars_dir / artifact.filename
    if target.is_file():
        return target
    jars_dir.mkdir(parents=True, exist_ok=True)
    with urllib.request.urlopen(f"{artifact.url}.sha1", timeout=60) as response:
        expected = response.read().decode("ascii").split()[0].strip().lower()

    partial = target.with_suffix(".part")
    logger.info("downloading artifact", extra={"url": artifact.url})
    with urllib.request.urlopen(artifact.url, timeout=300) as response, partial.open("wb") as out:
        shutil.copyfileobj(response, out)

    actual = _sha1(partial)
    if actual != expected:
        partial.unlink()
        raise RuntimeError(
            f"checksum mismatch for {artifact.filename}: expected {expected}, got {actual}"
        )
    partial.replace(target)
    return target


def _java_tool(name: str) -> str:
    java_home = os.environ.get("JAVA_HOME")
    if java_home:
        suffix = ".exe" if is_windows() else ""
        candidate = Path(java_home) / "bin" / f"{name}{suffix}"
        if candidate.is_file():
            return str(candidate)
    found = shutil.which(name)
    if not found:
        raise RuntimeError(f"'{name}' not found; install a JDK 17+ and set JAVA_HOME")
    return found


def build_localfs_shim(jars_dir: Path) -> Path:
    """Compile the Windows local file system shim against Spark's bundled Hadoop client."""
    import pyspark

    target = jars_dir / LOCALFS_JAR
    jars_dir.mkdir(parents=True, exist_ok=True)
    spark_jars = Path(pyspark.__file__).parent / "jars"
    source = resources.files("icebergflow.lakehouse") / "resources"
    with tempfile.TemporaryDirectory() as tmp, resources.as_file(source) as source_dir:
        classes = Path(tmp) / "classes"
        subprocess.run(
            [
                _java_tool("javac"),
                "--release",
                "17",
                "-nowarn",
                "-cp",
                str(spark_jars / "*"),
                "-d",
                str(classes),
                str(source_dir / "PermissionlessLocalFileSystem.java"),
            ],
            check=True,
        )
        subprocess.run([_java_tool("jar"), "cf", str(target), "-C", str(classes), "."], check=True)
    logger.info("built local file system shim", extra={"jar": str(target)})
    return target


def setup_runtime(jars_dir: Path, windows: bool | None = None) -> list[Path]:
    windows = is_windows() if windows is None else windows
    fetch_artifact(ICEBERG_RUNTIME, jars_dir)
    if windows and not (jars_dir / LOCALFS_JAR).is_file():
        build_localfs_shim(jars_dir)
    return required_jars(jars_dir, windows)
